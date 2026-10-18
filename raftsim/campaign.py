"""Seeded randomized fault campaign: run a workload under random faults, check
every invariant after every event, then check the client history for
linearizability."""
from __future__ import annotations

import random
from dataclasses import dataclass

from .client import Client
from .cluster import Cluster
from .core import NetConfig
from .faults import PROFILES, Nemesis
from .lincheck import check_history
from .node import BUG_NAMES, RaftConfig

PROFILE_NAMES = sorted(PROFILES)


@dataclass
class RunResult:
    seed: int
    ok: bool
    kind: str = ""
    detail: str = ""
    events: int = 0
    ops_done: int = 0
    ops_total: int = 0
    lin_unknown: bool = False
    profile: str = ""


def net_for(seed):
    """Per-seed network chaos level (all with drop, dup and reorder on)."""
    level = seed % 3
    if level == 0:
        return NetConfig(2, 20, drop=0.03, dup=0.03, reorder=0.05, reorder_extra=80)
    if level == 1:
        return NetConfig(2, 40, drop=0.08, dup=0.08, reorder=0.15, reorder_extra=120)
    return NetConfig(1, 12, drop=0.02, dup=0.10, reorder=0.10, reorder_extra=60)


def run_seed(seed, bugs=(), duration=10_000, quiesce=4_500, n=5, n_clients=3, trace=False):
    profile = PROFILE_NAMES[(seed // 3) % len(PROFILE_NAMES)]
    narrow = (seed // (3 * len(PROFILE_NAMES))) % 2 == 1  # narrow timeout range => more split votes
    # small AppendEntries batches slow replication down, which widens the windows in which a leader
    # holds entries on only some followers (needed to reach the Figure 8 scenario with a leader no-op)
    batch = (8, 1, 2)[(seed // (6 * len(PROFILE_NAMES))) % 3]
    # half of the seeds run without the leader no-op (paper-faithful Raft): the Figure 8 scenario is much
    # easier to reach when a new leader can commit old entries before it has written anything itself
    cfg = RaftConfig(election_hi=190 if narrow else 300, max_batch=batch, leader_noop=seed % 2 == 0,
                     bugs=frozenset(bugs))
    cl = Cluster(seed, n=n, cfg=cfg, net=net_for(seed), trace=trace)
    clients = [Client(cl, i, random.Random("%s:client:%d" % (seed, i))) for i in range(n_clients)]
    for i, c in enumerate(clients):
        c.start(100 + 17 * i)
    nem = Nemesis(cl, random.Random("%s:nemesis" % seed), profile)

    def result(kind="", detail="", lin_unknown=False):
        return RunResult(seed, kind == "", kind, detail, cl.sim.events_run,
                         sum(1 for h in cl.history if h.ret is not None), len(cl.history),
                         lin_unknown, profile)

    cl.run(duration)
    if cl.violations:
        v = cl.violations[0]
        return result(v.kind, str(v))
    # quiesce: stop faults and new work, heal, let everything converge
    nem.stop()
    cl.heal_all()
    cl.net.cfg = NetConfig(2, 15)  # calm network: the liveness check asks "does it converge once faults stop"
    cl.run(duration + 2_000)       # clients keep writing (a leader without a no-op needs a current-term write)
    for c in clients:
        c.stop()
    cl.run(duration + quiesce)
    if cl.violations:
        v = cl.violations[0]
        return result(v.kind, str(v))
    lin = check_history(cl.history)
    if not lin.ok:
        return result("linearizability", lin.reason, lin.unknown)
    stuck = [c.name for c in clients if not c.idle]
    live = [cl.nodes[i] for i in cl.ids]
    states = {tuple(sorted(nd.kv.data.items())) for nd in live}
    if stuck or len(states) != 1 or len({nd.last_applied for nd in live}) != 1:
        return result("liveness", "after healing: pending clients %s, distinct kv states %d" % (stuck, len(states)),
                      lin.unknown)
    return result("", "", lin.unknown)


def figure8_scenario(bugs=(), seed=1):
    """Scripted Raft paper Figure 8 (c)->(d): server 4 ("S5") holds a term-3 entry at index 2 that was
    never replicated; servers 0 and 1 hold a term-2 entry at index 2. A leader elected in term 4 replicates
    the old term-2 entry to a majority. Correct Raft must not call it committed; with `commit_old_terms` it
    does, and when the leader dies server 4 can win and overwrite it. Returns a RunResult."""
    cfg = RaftConfig(leader_noop=False, bugs=frozenset(bugs))
    cl = Cluster(seed, cfg=cfg, net=NetConfig(5, 15))

    def ent(term, k):
        return (term, ("x", k, ("put", "k", k)))

    first, old, mine = ent(1, 1), ent(2, 2), ent(3, 3)
    for i, log in enumerate([[first, old], [first, old], [first], [first], [first, mine]]):
        cl.disks[i].term = 3
        cl.disks[i].set_log(log)
    cl.crash(4)
    cl.run(1500)
    ld = cl.leader()
    if ld is not None and not cl.violations:
        cl.crash(ld.id)
        cl.restart(4)
        cl.run(5000)
    if cl.violations:
        v = cl.violations[0]
        return RunResult(seed, False, v.kind, str(v), cl.sim.events_run, profile="figure8")
    return RunResult(seed, True, "", "", cl.sim.events_run, profile="figure8")


def find_first(bug, max_seeds, start=0):
    """Run seeds start.. until the first failure. Returns (seeds_run, RunResult|None)."""
    bugs = (bug,) if bug else ()
    for k in range(max_seeds):
        r = run_seed(start + k, bugs)
        if not r.ok:
            return k + 1, r
    return max_seeds, None


__all__ = ["run_seed", "find_first", "RunResult", "BUG_NAMES"]
