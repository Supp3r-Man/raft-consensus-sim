"""Measurements printed by `python -m raftsim demo` (all seeds are fixed)."""
from __future__ import annotations

import random
import time

from .campaign import figure8_scenario, find_first, run_seed
from .client import Client, Prober
from .cluster import Cluster
from .core import NetConfig
from .node import BUG_NAMES, RaftConfig


def percentile(xs, p):
    """Nearest-rank percentile (p in 0..100) of a non-empty list."""
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, -(-len(xs) * p // 100) - 1))
    return xs[int(k)]


def median(xs):
    return percentile(xs, 50)


# ---------------------------------------------------------------- elections
def election_after_leader_crash(lo, hi, trials, seed_base=10_000):
    """Crash the leader of a settled 5-node cluster; time until a new leader.
    Heartbeat is lo // 3 so that followers do not time out between beats."""
    times, splits, stuck = [], 0, 0
    for k in range(trials):
        cl = Cluster(seed_base + k, cfg=RaftConfig(lo, hi, max(5, lo // 3)), net=NetConfig(5, 15))
        cl.run(3000)
        ld = cl.leader()
        if ld is None:
            stuck += 1
            continue
        t0, old_term, n0 = cl.sim.now, ld.term, len(cl.leader_events)
        cl.crash(ld.id)

        def watch():
            cl._check()
            if len(cl.leader_events) > n0:
                cl.sim.stopped = True

        cl.sim.run_until(t0 + 5000, watch)
        if len(cl.leader_events) > n0:
            t, _, term = cl.leader_events[n0]
            times.append(t - t0)
            if term - old_term > 1:
                splits += 1
        else:
            stuck += 1
    return {"lo": lo, "hi": hi, "n": len(times), "median": median(times), "p95": percentile(times, 95),
            "max": max(times), "split_rate": splits / len(times), "no_leader": stuck}


# ------------------------------------------------------------ commit latency
def commit_latency(drop, seeds, seed_base=20_000):
    lat = []
    for s in range(seeds):
        cl = Cluster(seed_base + s, net=NetConfig(5, 15, drop=drop))
        cl.run(3000)
        c = Client(cl, 0, random.Random("lat:%d" % s), think=(0, 0), retry=300)
        c.start()
        cl.run(3000 + 8000)
        lat += [h.ret - h.invoke for h in cl.history if h.ret is not None]
    return {"drop": drop, "ops": len(lat), "median": median(lat), "p95": percentile(lat, 95), "max": max(lat)}


# -------------------------------------------------------------- availability
def _followers(cl, ld, k):
    return [i for i in cl.ids if i != ld.id][:k]


SCENARIOS = [
    ("no failures", lambda cl, ld: None),
    ("1 node down (the leader)", lambda cl, ld: cl.crash(ld.id)),
    ("2 nodes down (leader + follower)", lambda cl, ld: [cl.crash(i) for i in [ld.id] + _followers(cl, ld, 1)]),
    ("3 nodes down (majority)", lambda cl, ld: [cl.crash(i) for i in [ld.id] + _followers(cl, ld, 2)]),
    ("partition 2|3, leader in the majority side",
     lambda cl, ld: cl.net.partition(_followers(cl, ld, 2), [ld.id] + _followers(cl, ld, 4)[2:])),
    ("partition 2|3, leader in the minority side",
     lambda cl, ld: cl.net.partition([ld.id] + _followers(cl, ld, 1), _followers(cl, ld, 4)[1:])),
    ("partition 2|2|1 (no majority anywhere)",
     lambda cl, ld: cl.net.partition([ld.id] + _followers(cl, ld, 1), _followers(cl, ld, 3)[1:], _followers(cl, ld, 4)[3:])),
    ("asymmetric: leader receives nothing, still sends",
     lambda cl, ld: [cl.net.block(o, ld.id) for o in cl.ids if o != ld.id]),
    ("asymmetric: leader sends nothing, still receives",
     lambda cl, ld: [cl.net.block(ld.id, o) for o in cl.ids if o != ld.id]),
]


def availability(scenario, seeds=8, window=20_000, seed_base=30_000):
    """Fraction of probe writes (one every 50 ms, 500 ms deadline) that commit
    within `window` ms after the fault is applied to a settled 5-node cluster."""
    name, fn = SCENARIOS[scenario]
    ok = total = 0
    recov = []
    for s in range(seeds):
        cl = Cluster(seed_base + s, net=NetConfig(5, 15))
        cl.run(3000)
        ld = cl.leader()
        t0 = cl.sim.now
        fn(cl, ld)
        pr = Prober(cl, t0, t0 + window)
        cl.run(t0 + window + 700)
        ok += sum(1 for r in pr.results if r[1])
        total += len(pr.results)
        wins = [r[0] + r[2] - t0 for r in pr.results if r[1]]
        if wins:
            recov.append(min(wins))
    return {"name": name, "avail": ok / total, "probes": total,
            "recovered": len(recov), "seeds": seeds, "recovery_median": median(recov) if recov else None}


# ------------------------------------------------------------ bug campaign
def bug_table(max_seeds, correct_seeds):
    rows = []
    t = time.time()
    bad = [run_seed(s) for s in range(correct_seeds)]
    bad = [r for r in bad if not r.ok]
    rows.append(("(correct implementation)", correct_seeds, "%d violations" % len(bad), 0.0 + time.time() - t))
    for b in BUG_NAMES:
        t = time.time()
        k, r = find_first(b, max_seeds)
        rows.append((b, k, r.kind if r else "NOT FOUND in %d seeds" % max_seeds, time.time() - t))
    t = time.time()
    k, r = find_first("commit_old_terms", 100, start=3300)
    rows.append(("commit_old_terms, seeds 3300+", k, r.kind if r else "NOT FOUND", time.time() - t))
    ok = figure8_scenario()
    bad = figure8_scenario(("commit_old_terms",))
    rows.append(("scripted Figure 8, correct", 1, "passes" if ok.ok else "FAILS: " + ok.kind, 0.0))
    rows.append(("scripted Figure 8, commit_old_terms", 1, bad.kind if not bad.ok else "NOT FOUND", 0.0))
    return rows


def run_demo(quick=False):
    trials = 40 if quick else 200
    out = []
    p = out.append
    p("raftsim demo (5-node cluster, virtual ms, fixed seeds)%s" % (" [quick mode: fewer seeds]" if quick else ""))
    p("")
    p("1. Election time after a leader crash (%d trials per row; heartbeat = lo/3; net delay 5-15 ms)" % trials)
    p("   timeout range    median   p95    max   split-vote rate   (new leader's term > crashed term + 1)")
    for lo, hi in [(50, 100), (100, 200), (150, 300), (300, 600)]:
        r = election_after_leader_crash(lo, hi, trials)
        p("   %4d-%-4d ms    %5d  %5d  %5d   %5.1f%%   (no leader within 5 s: %d)"
          % (lo, hi, r["median"], r["p95"], r["max"], 100 * r["split_rate"], r["no_leader"]))
    p("")
    p("2. Client write latency, one closed-loop client, steady leader")
    p("   message drop    ops   median    p95    max")
    for drop in (0.0, 0.05):
        r = commit_latency(drop, 3 if quick else 8)
        p("   %5.0f%%      %6d  %6d  %6d  %6d" % (100 * drop, r["ops"], r["median"], r["p95"], r["max"]))
    p("")
    p("3. Availability: share of probe writes committed within 500 ms, 20 s window after the fault")
    p("   (one probe every 50 ms; faults applied to a settled cluster and held; %d seeds)" % (3 if quick else 8))
    p("   %-52s avail   first commit after fault (median ms)" % "scenario")
    for i in range(len(SCENARIOS)):
        r = availability(i, seeds=3 if quick else 8)
        rec = "never (%d/%d seeds)" % (r["recovered"], r["seeds"]) if r["recovered"] == 0 else \
            "%d  (%d/%d seeds recovered)" % (r["recovery_median"], r["recovered"], r["seeds"])
        p("   %-52s %5.1f%%   %s" % (r["name"], 100 * r["avail"], rec))
    p("")
    ms, cs = (60, 40) if quick else (400, 300)
    p("4. Bug campaign: seeds run until the first violation (campaign seeds 0..; budget %d per bug)" % ms)
    p("   %-38s %-10s %s" % ("variant", "seeds run", "caught by"))
    for name, k, how, secs in bug_table(ms, cs):
        p("   %-38s %-10d %s" % (name, k, how))
    return "\n".join(out)
