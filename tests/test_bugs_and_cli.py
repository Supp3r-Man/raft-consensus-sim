"""Bug injection: each deliberate bug is caught by a specific check at a pinned seed, while the correct
implementation passes the same seeds. Seeds were found by the campaign (see README)."""
import subprocess
import sys

import pytest

from raftsim.campaign import figure8_scenario, find_first, run_seed
from raftsim.node import BUG_NAMES

# bug -> (pinned seed, expected detector); commit_old_terms is covered by the scripted scenario below
PINNED = {
    "no_persist_vote": (3, "election_safety"),
    "vote_no_log_check": (1, "leader_completeness"),
    "skip_prevlog_check": (0, "committed_entry_lost"),
    "truncate_on_stale_ae": (0, "committed_entry_lost"),
    "local_reads": (3, "linearizability"),
    # needed 3354 seeds from seed 0 (see README); the pinned seed is the first one that fails
    "commit_old_terms": (3353, "leader_completeness"),
}


@pytest.mark.parametrize("bug", sorted(PINNED))
def test_bug_is_caught_at_pinned_seed_and_correct_impl_passes_that_seed(bug):
    seed, kind = PINNED[bug]
    bad = run_seed(seed, (bug,))
    assert not bad.ok and bad.kind == kind
    good = run_seed(seed)
    assert good.ok, good


def test_every_bug_name_has_a_pinned_seed():
    assert set(BUG_NAMES) == set(PINNED)


def test_figure8_scenario_correct_impl_does_not_commit_old_term_entry():
    r = figure8_scenario()
    assert r.ok, r


def test_figure8_scenario_buggy_commit_is_caught_by_invariants():
    r = figure8_scenario(("commit_old_terms",))
    assert not r.ok and r.kind in ("leader_completeness", "committed_entry_lost")


@pytest.mark.parametrize("seed", [1, 2, 5, 9])
def test_figure8_scenario_is_deterministic_per_seed(seed):
    assert figure8_scenario(("commit_old_terms",), seed) == figure8_scenario(("commit_old_terms",), seed)


def test_correct_implementation_passes_a_campaign_of_many_seeds():
    bad = [r for r in (run_seed(s) for s in range(60)) if not r.ok]
    assert bad == []


def test_campaign_exercises_ops_and_faults():
    r = run_seed(0)
    assert r.ok and r.ops_done > 20 and r.events > 1000
    # profiles and network levels vary with the seed
    assert len({run_seed(s).profile for s in range(0, 12, 3)}) == 4


def test_find_first_reports_seeds_run_and_none_for_correct_impl():
    k, r = find_first("local_reads", 10)
    assert k == 4 and r.seed == 3 and r.kind == "linearizability"
    k, r = find_first(None, 3)
    assert (k, r) == (3, None)


def _cli(*args):
    return subprocess.run([sys.executable, "-m", "raftsim", *args], capture_output=True, text=True, timeout=300)


def test_cli_campaign_clean_exit_0():
    p = _cli("campaign", "--seeds", "3")
    assert p.returncode == 0 and "0 with violations" in p.stdout


def test_cli_campaign_with_bug_exit_1_and_names_the_violation():
    p = _cli("campaign", "--bug", "skip_prevlog_check", "--seeds", "5")
    assert p.returncode == 1 and "committed_entry_lost" in p.stdout


def test_cli_usage_errors_exit_2():
    assert _cli("campaign", "--bug", "not_a_bug").returncode == 2
    assert _cli().returncode == 2
    assert _cli("frobnicate").returncode == 2


def test_cli_demo_quick_prints_all_sections():
    p = _cli("demo", "--quick")
    assert p.returncode == 0
    for needle in ("1. Election time", "2. Client write latency", "3. Availability", "4. Bug campaign",
                   "commit_old_terms", "scripted Figure 8"):
        assert needle in p.stdout
