"""A Raft node (Ongaro & Ousterhout). No membership change, no snapshots.

Persistent state lives in a Disk object that survives a simulated crash;
everything else on the Node object is volatile and is rebuilt on restart.

Messages are plain tuples so traces are deterministic and hashable:
  ("RV", term, cand, last_idx, last_term)        ("RVR", term, granted)
  ("AE", term, leader, prev_idx, prev_term, entries, leader_commit)
  ("AER", term, ok, prev_idx, n_entries, conflict_term, conflict_idx)
  ("CREQ", client, req, op)                      ("CRESP", req, status, payload)
Log entries are (term, (client, req, op)); log index 1 is list position 0.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .kv import KVStore

BUG_NAMES = (
    "commit_old_terms",       # Figure 8: commit previous-term entries by counting replicas
    "no_persist_vote",        # votedFor is lost on crash
    "vote_no_log_check",      # grant votes without the log up-to-date check
    "skip_prevlog_check",     # AppendEntries accepted without the prevLog consistency check
    "truncate_on_stale_ae",   # follower truncates its log after every AE, even stale/duplicate ones
    "local_reads",            # leader answers get from local state without going through the log
)


@dataclass
class RaftConfig:
    election_lo: int = 150
    election_hi: int = 300
    heartbeat: int = 50
    max_batch: int = 8
    leader_noop: bool = True
    bugs: frozenset = field(default_factory=frozenset)

    def __post_init__(self):
        self.bugs = frozenset(self.bugs)
        unknown = self.bugs - set(BUG_NAMES)
        if unknown:
            raise ValueError("unknown bug(s): %s" % sorted(unknown))


class Disk:
    """Stable storage. Writes are synchronous (durable as soon as made)."""

    def __init__(self):
        self.term = 0
        self.voted_for = None
        self.log = []
        self.version = 0  # bumped on every log mutation (used by invariants)

    def append(self, entry):
        self.log.append(entry)
        self.version += 1

    def truncate(self, n):
        del self.log[n:]
        self.version += 1

    def set_log(self, entries):
        self.log[:] = entries
        self.version += 1


class Node:
    def __init__(self, nid, ids, disk, env, cfg, rng):
        self.id = nid
        self.ids = list(ids)
        self.peers = [i for i in ids if i != nid]
        self.majority = len(ids) // 2 + 1
        self.disk = disk
        self.env = env
        self.cfg = cfg
        self.rng = rng
        self.dead = False
        # volatile state (lost on crash)
        self.role = "follower"
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self.kv = KVStore()
        self.votes = set()
        self.next = {}
        self.match = {}
        self.pending = {}
        self.el_token = 0
        self.hb_token = 0
        self.rv_token = 0

    # ---- helpers ----
    @property
    def term(self):
        return self.disk.term

    def last_term(self):
        return self.disk.log[-1][0] if self.disk.log else 0

    def term_at(self, idx):
        return self.disk.log[idx - 1][0] if idx > 0 else 0

    def _send(self, dst, msg):
        self.env.send(self.id, dst, msg)

    def start(self):
        self.reset_election_timer()

    # ---- timers ----
    def reset_election_timer(self):
        self.el_token += 1
        d = self.rng.randint(self.cfg.election_lo, self.cfg.election_hi)
        self.env.sim.at(d, "timer", self.on_timer, "election", self.el_token)

    def _schedule_heartbeat(self):
        self.hb_token += 1
        self.env.sim.at(self.cfg.heartbeat, "timer", self.on_timer, "heartbeat", self.hb_token)

    def on_timer(self, kind, token):
        if self.dead:
            return
        if kind == "election":
            if token != self.el_token or self.role == "leader":
                return
            self.start_election()
        elif kind == "heartbeat":
            if token != self.hb_token or self.role != "leader":
                return
            self.broadcast_append()
            self._schedule_heartbeat()
        elif kind == "rvretry":
            if token != self.rv_token or self.role != "candidate":
                return
            self._request_votes(only_missing=True)
            self.env.sim.at(self.cfg.heartbeat, "timer", self.on_timer, "rvretry", token)

    def _request_votes(self, only_missing=False):
        d = self.disk
        for p in self.peers:
            if only_missing and p in self.votes:
                continue
            self._send(p, ("RV", d.term, self.id, len(d.log), self.last_term()))

    # ---- role changes ----
    def become_follower(self, term):
        d = self.disk
        was_leader = self.role == "leader"
        if term > d.term:
            d.term = term
            d.voted_for = None
        self.role = "follower"
        if was_leader:
            self.hb_token += 1
            self.pending = {}
            self.reset_election_timer()

    def start_election(self):
        d = self.disk
        d.term += 1
        d.voted_for = self.id
        self.role = "candidate"
        self.leader_id = None
        self.votes = {self.id}
        self.reset_election_timer()
        self.env.on_candidate(self.id, d.term)
        self._request_votes()
        self.rv_token += 1
        self.env.sim.at(self.cfg.heartbeat, "timer", self.on_timer, "rvretry", self.rv_token)
        if len(self.votes) >= self.majority:
            self.become_leader()

    def become_leader(self):
        d = self.disk
        self.role = "leader"
        self.leader_id = self.id
        self.next = {p: len(d.log) + 1 for p in self.peers}
        self.match = {p: 0 for p in self.peers}
        self.pending = {}
        if self.cfg.leader_noop:
            # Raft thesis 6.4: a no-op in the new term lets the leader commit
            # (and learn the commit status of) entries from earlier terms.
            d.append((d.term, (None, 0, ("noop",))))
        self.env.on_leader(self.id, d.term, d.log)
        self.broadcast_append()
        self._schedule_heartbeat()

    # ---- message dispatch ----
    def on_message(self, src, msg):
        if self.dead:
            return
        kind = msg[0]
        if kind == "RV":
            self._on_rv(src, *msg[1:])
        elif kind == "RVR":
            self._on_rvr(src, *msg[1:])
        elif kind == "AE":
            self._on_ae(src, *msg[1:])
        elif kind == "AER":
            self._on_aer(src, *msg[1:])
        elif kind == "CREQ":
            self._on_client(src, *msg[1:])

    # ---- elections ----
    def _on_rv(self, src, term, cand, last_idx, last_term):
        d = self.disk
        if term > d.term:
            self.become_follower(term)
        granted = False
        if term == d.term:
            if "vote_no_log_check" in self.cfg.bugs:
                up_to_date = True
            else:
                mine_t = self.last_term()
                up_to_date = last_term > mine_t or (last_term == mine_t and last_idx >= len(d.log))
            if up_to_date and d.voted_for in (None, cand):
                d.voted_for = cand
                granted = True
                self.reset_election_timer()
        self._send(src, ("RVR", d.term, granted))

    def _on_rvr(self, src, term, granted):
        if term > self.disk.term:
            self.become_follower(term)
            return
        if self.role != "candidate" or term != self.disk.term:
            return
        if granted:
            self.votes.add(src)
            if len(self.votes) >= self.majority:
                self.become_leader()

    # ---- replication: follower side ----
    def _on_ae(self, src, term, leader, prev_idx, prev_term, entries, leader_commit):
        d = self.disk
        n = len(entries)
        if term < d.term:
            self._send(src, ("AER", d.term, False, prev_idx, n, 0, 0))
            return
        self.become_follower(term)
        self.leader_id = leader
        self.reset_election_timer()
        if "skip_prevlog_check" not in self.cfg.bugs:
            if prev_idx > len(d.log):
                self._send(src, ("AER", d.term, False, prev_idx, n, 0, len(d.log) + 1))
                return
            if prev_idx > 0 and d.log[prev_idx - 1][0] != prev_term:
                ct = d.log[prev_idx - 1][0]
                ci = prev_idx
                while ci > 1 and d.log[ci - 2][0] == ct:
                    ci -= 1
                self._send(src, ("AER", d.term, False, prev_idx, n, ct, ci))
                return
        if "truncate_on_stale_ae" in self.cfg.bugs:
            d.set_log(d.log[:prev_idx] + list(entries))
        else:
            for k, e in enumerate(entries):
                idx = prev_idx + 1 + k
                if idx <= len(d.log):
                    if d.log[idx - 1][0] != e[0]:
                        d.truncate(idx - 1)
                        d.append(e)
                else:
                    d.append(e)
        if leader_commit > self.commit_index:
            new = min(leader_commit, prev_idx + n, len(d.log))
            if new > self.commit_index:
                self._set_commit(new)
        self._send(src, ("AER", d.term, True, prev_idx, n, 0, 0))

    # ---- replication: leader side ----
    def broadcast_append(self):
        for p in self.peers:
            self.send_append(p)

    def send_append(self, p):
        d = self.disk
        nxt = self.next[p]
        prev = nxt - 1
        entries = tuple(d.log[nxt - 1: nxt - 1 + self.cfg.max_batch])
        self._send(p, ("AE", d.term, self.id, prev, self.term_at(prev), entries, self.commit_index))

    def _on_aer(self, src, term, ok, prev_idx, n, ct, ci):
        d = self.disk
        if term > d.term:
            self.become_follower(term)
            return
        if self.role != "leader" or term < d.term:
            return
        if ok:
            m = prev_idx + n
            if m > self.match[src]:
                self.match[src] = m
            if m + 1 > self.next[src]:
                self.next[src] = m + 1
            self._advance_commit()
            if self.next[src] <= len(d.log):
                self.send_append(src)
        else:
            nxt = ci
            if ct:
                for i in range(len(d.log), 0, -1):
                    if d.log[i - 1][0] == ct:
                        nxt = i + 1
                        break
            nxt = max(nxt, self.match[src] + 1, 1)
            self.next[src] = nxt
            self.send_append(src)

    def _advance_commit(self):
        d = self.disk
        old_ok = "commit_old_terms" in self.cfg.bugs
        for N in range(len(d.log), self.commit_index, -1):
            if not old_ok and d.log[N - 1][0] != d.term:
                continue
            count = 1 + sum(1 for p in self.peers if self.match[p] >= N)
            if count >= self.majority:
                self._set_commit(N)
                break

    def _set_commit(self, new):
        old = self.commit_index
        self.commit_index = new
        self.env.on_commit(self.id, old, new, self.disk.log)
        self._apply()

    def _apply(self):
        d = self.disk
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            e = d.log[self.last_applied - 1]
            self.env.on_apply(self.id, self.last_applied, e)
            reply, result = self.kv.apply(e[1])
            if reply and self.role == "leader":
                self._send(e[1][0], ("CRESP", e[1][1], "ok", result))

    # ---- clients ----
    def _on_client(self, src, client, req, op):
        if self.role != "leader":
            self._send(client, ("CRESP", req, "notleader", self.leader_id))
            return
        sess = self.kv.sessions.get(client)
        if sess is not None:
            if sess[0] == req:
                self._send(client, ("CRESP", req, "ok", sess[1]))
                return
            if sess[0] > req:
                return
        if op[0] == "get" and "local_reads" in self.cfg.bugs:
            self._send(client, ("CRESP", req, "ok", self.kv.data.get(op[1])))
            return
        key = (client, req)
        idx = self.pending.get(key)
        d = self.disk
        if idx is not None and idx <= len(d.log) and d.log[idx - 1][1][:2] == key:
            return
        d.append((d.term, (client, req, op)))
        self.pending[key] = len(d.log)
        self.broadcast_append()
