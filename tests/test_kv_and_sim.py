import pytest

from raftsim.cluster import Cluster
from raftsim.core import NetConfig, Network, Sim
from raftsim.kv import KVStore
from raftsim.campaign import run_seed
import random


# ---------------- KV state machine ----------------
def test_kv_put_get_cas():
    kv = KVStore()
    assert kv.apply(("c", 1, ("get", "k"))) == (True, None)
    assert kv.apply(("c", 2, ("put", "k", 5))) == (True, "ok")
    assert kv.apply(("c", 3, ("get", "k"))) == (True, 5)
    assert kv.apply(("c", 4, ("cas", "k", 5, 6))) == (True, True)
    assert kv.apply(("c", 5, ("cas", "k", 5, 7))) == (True, False)
    assert kv.data["k"] == 6


def test_kv_duplicate_request_returns_cached_result_without_reexecuting():
    kv = KVStore()
    kv.apply(("c", 1, ("put", "k", 1)))
    assert kv.apply(("c", 2, ("cas", "k", 1, 2))) == (True, True)
    # same request id again: must NOT run the cas a second time (it would return False)
    assert kv.apply(("c", 2, ("cas", "k", 1, 2))) == (True, True)
    assert kv.data["k"] == 2


def test_kv_stale_request_is_ignored_with_no_reply():
    kv = KVStore()
    kv.apply(("c", 1, ("put", "k", 1)))
    kv.apply(("c", 2, ("put", "k", 2)))
    assert kv.apply(("c", 1, ("put", "k", 99))) == (False, None)
    assert kv.data["k"] == 2


def test_kv_sessions_are_per_client_and_noop_does_nothing():
    kv = KVStore()
    kv.apply(("a", 1, ("put", "k", 1)))
    assert kv.apply(("b", 1, ("put", "k", 2))) == (True, "ok")
    assert kv.apply((None, 0, ("noop",))) == (False, None)
    assert kv.data["k"] == 2


def test_kv_unknown_op_raises():
    with pytest.raises(ValueError):
        KVStore().apply(("c", 1, ("delete", "k")))


# ---------------- simulator core ----------------
def test_events_run_in_time_order_with_insertion_order_ties():
    sim, seen = Sim(0), []
    sim.at(5, "x", seen.append, "b")
    sim.at(1, "x", seen.append, "a")
    sim.at(5, "x", seen.append, "c")
    sim.run_until(10)
    assert seen == ["a", "b", "c"] and sim.now == 10


def test_negative_delay_rejected_and_run_until_does_not_run_future_events():
    sim, seen = Sim(0), []
    with pytest.raises(ValueError):
        sim.at(-1, "x", seen.append, 1)
    sim.at(50, "x", seen.append, 1)
    sim.run_until(49)
    assert seen == [] and sim.now == 49
    sim.run_until(50)
    assert seen == [1]


def _net(cfg, seed=1):
    sim, got = Sim(seed), []
    net = Network(sim, random.Random(seed), cfg, lambda dst, src, msg: got.append((sim.now, dst, src, msg)))
    return sim, net, got


def test_network_drop_all_and_delay_bounds():
    sim, net, got = _net(NetConfig(drop=1.0))
    net.send(0, 1, "m")
    sim.run_until(1000)
    assert got == [] and net.stats["dropped"] == 1
    sim, net, got = _net(NetConfig(delay_lo=7, delay_hi=9))
    for _ in range(50):
        net.send(0, 1, "m")
    sim.run_until(1000)
    assert len(got) == 50 and all(7 <= t <= 9 for t, *_ in got)


def test_network_duplicates():
    sim, net, got = _net(NetConfig(dup=1.0))
    net.send(0, 1, "m")
    sim.run_until(1000)
    assert len(got) == 2


def test_network_reordering_happens_with_extra_delay():
    sim, net, got = _net(NetConfig(delay_lo=5, delay_hi=5, reorder=0.5, reorder_extra=100), seed=3)
    for i in range(40):
        net.send(0, 1, i)
    sim.run_until(1000)
    order = [m for *_, m in got]
    assert sorted(order) == list(range(40)) and order != list(range(40))


def test_partition_is_symmetric_directed_block_is_not_and_heal_restores():
    sim, net, got = _net(NetConfig())
    net.partition([0, 1], [2])
    net.send(0, 2, "x"); net.send(2, 0, "y"); net.send(0, 1, "ok")
    sim.run_until(100)
    assert [m for *_, m in got] == ["ok"]
    net.heal()
    net.block(0, 1)
    net.send(0, 1, "no"); net.send(1, 0, "yes")
    sim.run_until(200)
    assert [m for *_, m in got] == ["ok", "yes"]
    net.heal()
    net.send(0, 1, "back")
    sim.run_until(300)
    assert got[-1][3] == "back"


def test_isolate_cuts_one_node_both_ways_but_not_client_endpoints():
    sim, net, got = _net(NetConfig())
    net.isolate(0, [0, 1, 2])
    net.send(0, 1, "a"); net.send(1, 0, "b"); net.send("c0", 0, "client"); net.send(0, "c0", "reply")
    sim.run_until(100)
    assert sorted(m for *_, m in got) == ["client", "reply"]


# ---------------- determinism ----------------
def _trace_digest(seed):
    from raftsim.campaign import net_for
    from raftsim.client import Client
    from raftsim.faults import Nemesis
    cl = Cluster(seed, net=net_for(seed), trace=True)
    Client(cl, 0, random.Random(seed)).start()
    Nemesis(cl, random.Random("n%d" % seed), "mixed")
    cl.run(4000)
    return cl.trace_digest(), len(cl.sim.trace)


def test_same_seed_gives_identical_trace():
    a, b = _trace_digest(5), _trace_digest(5)
    assert a == b and a[1] > 500


def test_different_seeds_give_different_traces():
    assert _trace_digest(5)[0] != _trace_digest(6)[0]


def test_campaign_run_is_reproducible():
    r1, r2 = run_seed(11), run_seed(11)
    assert r1 == r2
    assert run_seed(11, ("local_reads",)) == run_seed(11, ("local_reads",))


def test_unknown_bug_name_is_rejected():
    from raftsim.node import RaftConfig
    with pytest.raises(ValueError):
        RaftConfig(bugs=["nope"])
