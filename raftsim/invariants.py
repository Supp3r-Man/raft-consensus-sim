"""Safety invariants, checked after every simulator event.

Hooks (on_leader / on_commit / on_apply) are called inline by the nodes at the
moment something happens; after_event() runs the structural checks over all
persistent logs (including crashed nodes' disks) whenever any log changed.

"Committed" means: some node advanced its commitIndex past that entry. The
checker keeps one global record of committed entries and compares everything
against it.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Violation:
    kind: str
    time: int
    detail: str

    def __str__(self):
        return "%s at t=%d: %s" % (self.kind, self.time, self.detail)


KINDS = (
    "election_safety",
    "log_matching",
    "leader_completeness",
    "state_machine_safety",
    "committed_entry_lost",
)


class Invariants:
    def __init__(self, cluster):
        self.c = cluster
        self.leaders = {}      # term -> node id that won it
        self.committed = []    # committed[i] is the entry at log index i+1
        self.applied = {}      # log index -> entry first applied there
        self._vers = None
        self._clen = -1

    def _v(self, kind, detail):
        self.c.violations.append(Violation(kind, self.c.sim.now, detail))

    # ---- inline hooks ----
    def on_leader(self, nid, term, log):
        prev = self.leaders.get(term)
        if prev is not None and prev != nid:
            self._v("election_safety", "term %d has leaders %d and %d" % (term, prev, nid))
        self.leaders.setdefault(term, nid)
        for i, e in enumerate(self.committed):
            if e[0] >= term:
                break
            if i >= len(log) or log[i] != e:
                self._v("leader_completeness",
                        "leader %d of term %d lacks committed entry at index %d (term %d)"
                        % (nid, term, i + 1, e[0]))
                return

    def on_commit(self, nid, old, new, log):
        for idx in range(old + 1, new + 1):
            e = log[idx - 1]
            if idx <= len(self.committed):
                if self.committed[idx - 1] != e:
                    self._v("committed_entry_lost",
                            "node %d committed a different entry at index %d" % (nid, idx))
            elif idx == len(self.committed) + 1:
                self.committed.append(e)
            else:
                self._v("committed_entry_lost", "commit gap at index %d on node %d" % (idx, nid))

    def on_apply(self, nid, idx, entry):
        prev = self.applied.get(idx)
        if prev is None:
            self.applied[idx] = entry
        elif prev != entry:
            self._v("state_machine_safety",
                    "index %d applied as %r and %r" % (idx, prev, entry))

    # ---- structural checks after each event ----
    def after_event(self):
        c = self.c
        ids = c.ids
        disks = c.disks
        vers = tuple(disks[i].version for i in ids)
        clen = len(self.committed)
        if vers == self._vers and clen == self._clen:
            return
        if self._vers is None:
            changed = set(range(len(ids)))
        else:
            changed = {k for k in range(len(ids)) if vers[k] != self._vers[k]}
        self._vers, self._clen = vers, clen
        logs = [disks[i].log for i in ids]
        # log matching: same index+term => identical prefix. It is enough to
        # find the highest index with equal terms and compare that prefix.
        for a in range(len(ids)):
            for b in range(a + 1, len(ids)):
                if a not in changed and b not in changed:
                    continue
                la, lb = logs[a], logs[b]
                i = min(len(la), len(lb))
                while i > 0 and la[i - 1][0] != lb[i - 1][0]:
                    i -= 1
                if i > 0 and la[:i] != lb[:i]:
                    self._v("log_matching",
                            "nodes %d and %d agree on term at index %d but differ before it" % (ids[a], ids[b], i))
                    return
        # committed entries are never lost: a majority of persisted logs must
        # still hold the whole committed prefix.
        com = self.committed
        if com:
            ms = []
            for lg in logs:
                k = min(len(lg), len(com))
                if lg[:k] == com[:k]:
                    ms.append(k)
                else:
                    j = 0
                    while lg[j] == com[j]:
                        j += 1
                    ms.append(j)
            ms.sort(reverse=True)
            if ms[len(ids) // 2] < len(com):
                self._v("committed_entry_lost",
                        "fewer than a majority of logs still hold all %d committed entries" % len(com))
