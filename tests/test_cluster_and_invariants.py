import random

import pytest

from raftsim.client import Client
from raftsim.cluster import Cluster
from raftsim.core import NetConfig
from raftsim.invariants import KINDS
from raftsim.lincheck import check_history
from raftsim.node import RaftConfig

from helpers import E


# ---------------- whole-cluster behaviour ----------------
def settled(seed=1, **kw):
    cl = Cluster(seed, **kw)
    cl.run(3000)
    return cl


def test_leader_elected_and_unique_per_term():
    cl = settled()
    assert cl.leader() is not None and not cl.violations
    terms = [t for _, _, t in cl.leader_events]
    assert len(terms) == len(set(terms))


def test_clients_ops_replicate_to_every_node_and_history_is_linearizable():
    cl = settled(2)
    cs = [Client(cl, i, random.Random(i)) for i in range(3)]
    for c in cs:
        c.start()
    cl.run(8000)
    for c in cs:
        c.stop()
    cl.run(10000)
    assert not cl.violations
    assert sum(c.completed for c in cs) > 50
    states = {tuple(sorted(cl.nodes[i].kv.data.items())) for i in cl.ids}
    assert len(states) == 1
    assert check_history(cl.history).ok


def test_leader_crash_triggers_new_election_with_higher_term_and_old_node_rejoins():
    cl = settled(3)
    ld = cl.leader()
    cl.crash(ld.id)
    cl.run(cl.sim.now + 2000)
    new = cl.leader()
    assert new is not None and new.id != ld.id and new.term > ld.term
    cl.restart(ld.id)
    cl.run(cl.sim.now + 1500)
    assert cl.nodes[ld.id].role == "follower" and cl.nodes[ld.id].term == cl.leader().term
    assert not cl.violations


def test_crash_keeps_persistent_state_and_loses_volatile_state():
    cl = settled(4)
    c = Client(cl, 0, random.Random(0))
    c.start()
    cl.run(5000)
    victim = next(i for i in cl.ids if cl.nodes[i].role == "follower")
    nd = cl.nodes[victim]
    term, voted, log = nd.term, nd.disk.voted_for, list(nd.disk.log)
    assert nd.commit_index > 0 and voted is not None
    cl.crash(victim)
    assert cl.nodes[victim] is None and nd.dead
    cl.restart(victim)
    new = cl.nodes[victim]
    assert new is not nd
    assert (new.disk.term, new.disk.voted_for, new.disk.log) == (term, voted, log)
    assert new.commit_index == 0 and new.last_applied == 0 and new.kv.data == {} and new.role == "follower"


def test_bug_no_persist_vote_wipes_vote_on_restart_but_correct_keeps_it():
    for bugs, expect_none in ((("no_persist_vote",), True), ((), False)):
        cl = Cluster(5, cfg=RaftConfig(bugs=frozenset(bugs)))
        cl.run(2000)
        victim = next(i for i in cl.ids if cl.nodes[i].disk.voted_for is not None)
        cl.crash(victim)
        cl.restart(victim)
        assert (cl.disks[victim].voted_for is None) == expect_none


def test_minority_partition_cannot_commit_and_heals():
    cl = settled(6)
    ld = cl.leader()
    other = next(i for i in cl.ids if i != ld.id)
    rest = [i for i in cl.ids if i not in (ld.id, other)]
    cl.net.partition([ld.id, other], rest)
    c = Client(cl, 0, random.Random(1), think=(5, 10))
    c.target = ld.id
    c.start()
    cl.run(cl.sim.now + 2000)
    assert not cl.violations
    stale_commits = cl.nodes[ld.id].commit_index
    cl.run(cl.sim.now + 500)
    assert cl.nodes[ld.id].commit_index == stale_commits  # frozen: no quorum on its side
    cl.net.heal()
    cl.run(cl.sim.now + 3000)
    assert not cl.violations
    assert len({cl.nodes[i].commit_index for i in cl.ids}) <= 2


def test_exactly_once_under_duplicates_drops_and_retries():
    cl = Cluster(7, net=NetConfig(2, 30, drop=0.2, dup=0.4, reorder=0.2))
    cl.run(3000)
    c = Client(cl, 0, random.Random(3), retry=120)
    c.start()
    cl.run(15000)
    assert c.completed > 20 and not cl.violations
    entries = [e[1] for e in cl.disks[cl.leader().id].log if e[1][0] == "c0"]
    # retries may put the same request in the log more than once; the state machine applies it once
    assert len(entries) >= c.completed
    applied = [h for h in cl.history if h.ret is not None]
    assert check_history(cl.history).ok and applied


# ---------------- invariant checker unit tests ----------------
def fresh():
    return Cluster(1)


def kinds(cl):
    return [v.kind for v in cl.violations]


def test_all_five_invariant_kinds_are_declared():
    assert set(KINDS) == {"election_safety", "log_matching", "leader_completeness",
                          "state_machine_safety", "committed_entry_lost"}


def test_election_safety_flags_two_leaders_in_one_term_but_not_re_report_of_same_leader():
    cl = fresh()
    cl.inv.on_leader(0, 3, [])
    cl.inv.on_leader(0, 3, [])
    cl.inv.on_leader(1, 4, [])
    assert not cl.violations
    cl.inv.on_leader(2, 3, [])
    assert kinds(cl) == ["election_safety"]


def test_state_machine_safety_flags_different_commands_at_same_index():
    cl = fresh()
    cl.inv.on_apply(0, 1, E(1, 1))
    cl.inv.on_apply(1, 1, E(1, 1))
    assert not cl.violations
    cl.inv.on_apply(2, 1, E(1, 2))
    assert kinds(cl) == ["state_machine_safety"]


def test_committed_entry_conflict_and_gap_detected():
    cl = fresh()
    cl.inv.on_commit(0, 0, 2, [E(1, 1), E(1, 2)])
    cl.inv.on_commit(1, 0, 2, [E(1, 1), E(1, 2)])
    assert not cl.violations
    cl.inv.on_commit(2, 1, 2, [E(1, 1), E(2, 9)])
    assert kinds(cl) == ["committed_entry_lost"]
    cl2 = fresh()
    cl2.inv.on_commit(0, 3, 4, [E(1)] * 4)
    assert kinds(cl2) == ["committed_entry_lost"]


def test_leader_completeness_flags_leader_missing_a_committed_entry():
    cl = fresh()
    cl.inv.on_commit(0, 0, 2, [E(1, 1), E(2, 2)])
    cl.inv.on_leader(1, 3, [E(1, 1), E(2, 2), E(2, 3)])
    assert not cl.violations
    cl.inv.on_leader(2, 4, [E(1, 1)])
    assert kinds(cl) == ["leader_completeness"]


def test_leader_completeness_ignores_committed_entries_of_same_or_later_term():
    cl = fresh()
    cl.inv.on_commit(0, 0, 1, [E(5)])
    cl.inv.on_leader(1, 5, [])
    assert not cl.violations


def test_log_matching_flags_divergent_prefix_behind_equal_term():
    cl = fresh()
    cl.disks[0].set_log([E(1, 1), E(2, 2)])
    cl.disks[1].set_log([E(1, 9), E(2, 2)])
    cl.inv.after_event()
    assert "log_matching" in kinds(cl)


def test_log_matching_accepts_logs_that_diverge_only_after_a_term_difference():
    cl = fresh()
    cl.disks[0].set_log([E(1, 1), E(2, 2), E(2, 3)])
    cl.disks[1].set_log([E(1, 1), E(3, 7)])
    cl.disks[2].set_log([E(1, 1)])
    cl.inv.after_event()
    assert not cl.violations


def test_committed_entry_lost_when_majority_of_logs_no_longer_hold_it():
    cl = fresh()
    cl.inv.on_commit(0, 0, 1, [E(1, 1)])
    for i in (0, 1):
        cl.disks[i].set_log([E(1, 1)])
    cl.inv.after_event()
    assert kinds(cl) == ["committed_entry_lost"]   # only 2 of 5 logs hold it, a majority is 3
    cl2 = fresh()
    cl2.inv.on_commit(0, 0, 1, [E(1, 1)])
    for i in (0, 1, 2):
        cl2.disks[i].set_log([E(1, 1)])
    cl2.inv.after_event()
    assert not cl2.violations


def test_committed_entry_lost_flags_overwritten_entry():
    cl = fresh()
    cl.inv.on_commit(0, 0, 1, [E(1, 1)])
    for i in (0, 1, 2):
        cl.disks[i].set_log([E(1, 1)])
    cl.inv.after_event()
    assert not cl.violations
    for i in (0, 1, 2):
        cl.disks[i].set_log([E(2, 5)])
    cl.inv.after_event()
    assert "committed_entry_lost" in kinds(cl)
