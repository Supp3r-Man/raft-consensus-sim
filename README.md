# raft-consensus-sim

Raft consensus (leader election, log replication, persistence, a replicated key-value store with client request dedup) running inside a **deterministic discrete-event simulator**, pure Python standard library. A seed fully determines a run, so every failure the fault campaign finds can be replayed exactly.

Not included, on purpose: **no membership changes, no log compaction / snapshots**. Persistent writes are synchronous and never fail; there are no disk faults, no Byzantine nodes, no real network, no wall clock.

CI is configured (`.github/workflows/ci.yml`: pytest on Python 3.11 and 3.13, plus `python -m raftsim demo --quick`).

## What is in it

| Piece | File |
|---|---|
| Virtual clock, event heap, seeded network (drop / duplicate / reorder / delay, symmetric, asymmetric and leader-isolating partitions) | `raftsim/core.py` |
| Raft node: randomized election timeouts from a seeded RNG, RequestVote, AppendEntries with consistency check and conflict backtracking, commit rule (only current-term entries are committed by counting replicas), leader no-op, persistent `Disk` (currentTerm, votedFor, log) that survives crashes while volatile state is rebuilt | `raftsim/node.py` |
| KV state machine with per-client request-id dedup | `raftsim/kv.py` |
| Invariants checked after every event: election safety, log matching, leader completeness, state-machine safety, committed entries never lost | `raftsim/invariants.py` |
| Wing-Gong style linearizability checker (get / put / cas, per key, memoized, pending operations handled) | `raftsim/lincheck.py` |
| Six switchable deliberate bugs (`raftsim.node.BUG_NAMES`) | `raftsim/node.py` |
| Seeded randomized fault campaign (crashes, quick reboots, leader kills, partitions, one-way blocks) | `raftsim/faults.py`, `raftsim/campaign.py` |
| Measurements | `raftsim/experiments.py` |

## Quick start

```
python -m pytest                       # 105 tests
python -m raftsim demo --quick         # about a minute: the numbers below with fewer seeds
python -m raftsim demo                 # full run, several minutes (6.7 min measured on a busy machine)
python -m raftsim campaign --seeds 200                          # correct implementation, exit 0 if clean
python -m raftsim campaign --bug skip_prevlog_check --seeds 50  # exit 1 and prints the violated invariant
```

Exit codes: 0 no violation, 1 violation found, 2 usage error or crash.

## Results (copied from one run of `python -m raftsim demo`; virtual milliseconds; synthetic, fixed seeds)

**Election time after the leader of a settled 5-node cluster is crashed** (200 trials per row, network delay 5-15 ms, heartbeat = low end of range / 3). "Split" means the new leader's term was more than crashed term + 1, i.e. at least one election round produced no leader.

| timeout range | median | p95 | max | split-vote rate |
|---|---|---|---|---|
| 50-100 ms | 84 | 221 | 308 | 24.0% |
| 100-200 ms | 135 | 287 | 450 | 14.0% |
| 150-300 ms | 185 | 385 | 647 | 8.5% |
| 300-600 ms | 325 | 613 | 1163 | 5.5% |

**Client write latency**, one closed-loop client, steady leader, 5-15 ms one-way delay:

| message drop | ops | median | p95 | max |
|---|---|---|---|---|
| 0% | 1662 | 38 | 47 | 70 |
| 5% | 903 | 40 | 358 | 960 |

**Availability**: share of probe writes (one every 50 ms, 500 ms deadline, retrying other nodes every 120 ms) that commit during the 20 s after a fault is applied to a settled 5-node cluster and held (8 seeds each).

| scenario | availability | first commit after fault (median ms) |
|---|---|---|
| no failures | 100.0% | 63 |
| 1 node down (the leader) | 100.0% | 264 |
| 2 nodes down (leader + follower) | 99.9% | 286 |
| 3 nodes down (majority) | 0.0% | never |
| partition 2/3, leader on the majority side | 100.0% | 68 |
| partition 2/3, leader on the minority side | 99.6% | 282 |
| partition 2/2/1 (no majority anywhere) | 0.0% | never |
| asymmetric: leader receives nothing but still sends | **0.0%** | never |
| asymmetric: leader sends nothing but still receives | 100.0% | 249 |

**Bug campaign.** Each variant runs the same seeded campaign (3 clients, 3 keys, 10 s of random faults, then heal, calm network, linearizability check and a convergence check). "Seeds run" counts from seed 0 up to and including the first failing seed.

| variant | seeds run | caught by |
|---|---|---|
| correct implementation | 300 in the demo, 2000 (seeds 0-1999) in a separate run: 0 violations | - |
| `skip_prevlog_check` (no prevLog consistency check) | 1 | committed entry lost |
| `truncate_on_stale_ae` (follower truncates after any AppendEntries, even stale ones) | 1 | committed entry lost |
| `vote_no_log_check` (votes granted without the up-to-date check) | 2 | leader completeness |
| `no_persist_vote` (votedFor lost on crash) | 4 | election safety (two leaders in one term) |
| `local_reads` (leader answers gets from local state) | 4 | linearizability checker |
| `commit_old_terms` (Figure 8: commit older-term entries by counting replicas) | **3354** | leader completeness |

The demo prints `commit_old_terms` as "NOT FOUND in 400 seeds" because its budget is 400 (60 with `--quick`); it then shows the pinned window starting at seed 3300 (found after 54 seeds, seed 3353). Searching other 1000-seed windows (each stopped at its first hit) found it at seeds 4049, 4407, 5907, 8320 and 10039 and not at all in 0-2399, 6400-7399 and 8400-9399. So the rate is roughly one failing seed per 1000-1500, and the correct implementation never fails. A scripted version of the paper's Figure 8 scenario (`campaign.figure8_scenario`) triggers it deterministically: correct Raft passes, the bug is caught by leader completeness.

## Findings that are not flattering

- **The correct implementation failed my own checks first, twice** (details in STEPS.md). Without a leader no-op, 2 of the first 400 seeds ended with nodes disagreeing on the applied prefix (a new leader cannot commit older-term entries until it writes something in its own term). I added the standard leader no-op. Then 10 of 2000 seeds failed the convergence check because the campaign's lossy network was still on at the end (spurious elections); the check now runs on a calm network. No safety invariant was ever violated by the correct implementation.
- **The campaign was tuned after it failed to find two bugs.** The first version found 4 of the 6 bugs within 150 seeds. `no_persist_vote` needed quick node reboots timed at voters, and `commit_old_terms` needed small AppendEntries batches and (on half the seeds) no leader no-op. Those knobs were added because the bugs were missed, so "seeds run" is for the final campaign, not a blind first attempt.
- **Without check-quorum or pre-vote, a leader that can send but not receive stalls the cluster forever** (0.0% availability above). Followers keep hearing heartbeats and never start an election. This is a real Raft limitation, not a bug in the simulator.
- **The 500 ms probe deadline hides short outages**: after a leader crash the median first commit is 264 ms, yet availability reads 100.0%. Availability numbers depend on that deadline.
- Linearizability is checked once at the end of a run, not continuously; a run that is cut short by an invariant violation never reaches it. `local_reads` is the only bug caught by the checker, and it needs a leader isolated from the majority while clients keep reading from it.
- Reads go through the log (no leases, no read-index), so every get costs a full replication round.
- Split-vote and latency numbers depend on my network model (uniform 5-15 ms delay). They are simulation numbers, not measurements of a real deployment.
- The mutation check kills most mutants through the slower pinned-seed campaign tests rather than a small focused unit test. That is fine for coverage but means the unit tests alone would let some mutants through.

## Tests and mutation check

105 tests: message-level unit tests of one node (voting rules, prevLog check, backtracking, commit rule, dedup), invariant checker tests on hand-made violations, linearizability checker tests on hand-made linearizable and non-linearizable histories plus seeded randomized ones, network and determinism tests (same seed, identical trace digest; different seed, different), pinned-seed tests that each bug is caught and the correct implementation passes the same seed, a 60-seed correct campaign, and CLI exit-code tests.

Mutation check: 16 behaviours broken one at a time in a scratch copy (commit-rule term check, votedFor write, up-to-date check, prevLog check, majority size, strict majority, conflict truncation, vote reset on higher term, follower commit bound, dedup, linearizability real-time order, cas model, partitions, and three invariants): **16/16 killed**. The script reports "PATTERN NOT FOUND" if a pattern is missing instead of testing an unchanged file.

## Layout

```
raftsim/         core, node, kv, invariants, lincheck, client, faults, cluster, campaign, experiments, __main__
tests/           pytest suite
STEPS.md         what was done, in order, and how to run it
```
