# STEPS

**Part A** is what I did, in order. **Part B** is how you run it.

---

## Part A: steps taken

1. **Decided the shape:** integer virtual milliseconds, one event heap, every random draw from a `random.Random` seeded by a string derived from the run seed (`"<seed>:net"`, `"<seed>:node:<i>"`, ...). Messages are plain tuples so traces are hashable and a test can compare digests. Node object per incarnation: a crash marks it dead and drops it, a restart builds a new `Node` around the same `Disk`, so volatile state is lost by construction.
2. **Wrote the simulator core and network** (`core.py`): drop, duplicate, reorder (extra delay), delay, symmetric partitions, directed blocks, leader isolation. Client endpoints are never partitioned so a client can still reach an isolated old leader.
3. **Wrote the Raft node** (`node.py`) from the paper: RequestVote with the up-to-date check, AppendEntries with prevLog check and conflict term/index backtracking, commit only for current-term entries, apply loop, replies to clients. Six bugs are `if "name" in cfg.bugs` branches next to the correct code.
4. **Wrote the KV state machine** with last-(request id, result) per client for dedup, and the clients (closed-loop with retries and redirects, plus open-loop probes for availability).
5. **Wrote the invariants** (election safety, log matching, leader completeness, state-machine safety, committed entries never lost). Inline hooks catch events the moment they happen; a structural pass over all persistent logs runs after any event that changed a log. The log-matching check only compares pairs involving a changed log and compares one prefix per pair, so it stays cheap enough to run after every event.
6. **Wrote the Wing-Gong linearizability checker** (per key, bitmask + value memo, pending operations optional) and tests on hand-made histories before using it on real ones.
7. **First campaign run:** the correct implementation passed 40 seeds; three bugs were found on the first seed, `local_reads` at seed 4, and `commit_old_terms` and `no_persist_vote` were not found in 150 seeds.
8. **Correct implementation violated a check (liveness, not safety).** In 400 seeds, seeds 167 and 356 ended with nodes holding different applied prefixes. Diagnosis (printing every node after the run): the new leader held older-term entries that others had committed, but a Raft leader cannot count replicas for older-term entries, and with no new client writes it never learned they were committed. Fix: the standard leader no-op (Raft thesis 6.4), `RaftConfig.leader_noop`.
9. **Second violation of the convergence check:** with the no-op in, 10 of 2000 correct seeds still failed it. Diagnosis: seeds with the heavy lossy network were still lossy at the end, so spurious elections were still happening when the check ran. Fix: heal, switch to a calm network, keep clients writing for 2 s, then stop them and wait 2.5 s before checking. After that: 2000 correct seeds (0-1999) with zero violations, in two separate 1000-seed runs.
10. **Made the campaign able to reach the two missed bugs** (honestly: this was tuning after a miss). Added candidate RequestVote retransmission (a bounced voter can be asked again in the same term), quick-reboot faults (`bounce`), `bounce_voter` / `election_chaos` faults that reboot a node that has just voted, a `leader_killer` profile, narrower election timeouts on some seeds (more split votes), AppendEntries batch sizes 1/2/8, and no leader no-op on half of the seeds. `no_persist_vote` then fell at seed 3. `commit_old_terms` still needed 3354 seeds; a hunt over 1000-seed windows found it at 3353, 4049, 4407, 5907, 8320, 10039 and not in three other windows.
11. **Added a scripted Figure 8 scenario** (`figure8_scenario`) so that bug has a deterministic reproduction as well: correct passes, buggy is caught by leader completeness.
12. **Wrote the experiments and the demo** (`experiments.py`): election-time sweep, write latency, nine availability scenarios, the bug table.
13. **Wrote 105 tests**, including pinned-seed tests (each bug caught at its first failing seed, correct implementation passes the same seed), same-seed identical-trace tests, hand-made linearizable / non-linearizable histories and CLI exit codes.
14. **Mutation-checked 16 behaviours** in a scratch copy each time (`16/16 killed`). Most were killed by the pinned-seed or invariant tests; a mutated invariant is killed by the unit test that feeds it a hand-made violation.
15. **Wrote README and this file.** The README numbers are from one full `python -m raftsim demo` run.

Problems and choices along the way:
- My first `new/old inversion` linearizability test was wrong (the two writes were concurrent, so the history was linearizable); fixed the history so the first write completes before the second starts.
- My first invariant test for "committed entry lost" asserted the wrong direction (an entry on only 2 of 5 logs *is* a violation); fixed.
- Running the demo, the test suite and several seed hunts in parallel on a shared machine made timings meaningless; timings in the README are labelled as such.
- A clean run of the demo is deterministic (fixed seeds); wall-clock time is the only thing that varies.

---

## Part B: steps for you

### Run it
1. Python 3.11 or newer, no packages (pytest only for the tests).
2. `python -m raftsim demo --quick` for a fast pass, `python -m raftsim demo` for the full tables (several minutes).
3. Replay a campaign: `python -m raftsim campaign --seeds 200` (correct implementation) or `python -m raftsim campaign --bug no_persist_vote --seeds 20`. Add `--start N` to begin at seed N, `--all` to keep going after the first violation. Exit code 0 clean, 1 violation, 2 usage error or crash.
4. See the very hard bug: `python -m raftsim campaign --bug commit_old_terms --start 3353 --seeds 1`.

### Run the tests
`python -m pytest` (105 tests; a minute or two, the pinned-seed campaign tests and the CLI demo test dominate).

### What to read first
1. `raftsim/node.py` (the whole protocol, about 300 lines, bugs are the `bugs` branches).
2. `raftsim/invariants.py` (what "checked after every event" means).
3. `raftsim/campaign.py` and `raftsim/faults.py` (what the campaign does and where it was tuned).
4. `tests/test_bugs_and_cli.py` for the pinned seeds, then the README "Findings that are not flattering".
