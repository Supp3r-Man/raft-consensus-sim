"""Direct message-level tests of one Raft node (no simulator)."""
from helpers import E, make_leader, make_node


# ---------------- voting ----------------
def test_vote_granted_once_per_term_but_repeat_from_same_candidate_ok():
    n, env = make_node(term=1)
    n.on_message(1, ("RV", 2, 1, 0, 0))
    n.on_message(2, ("RV", 2, 2, 0, 0))
    n.on_message(1, ("RV", 2, 1, 0, 0))
    replies = [m for m in env.to(1, "RVR")] + env.to(2, "RVR")
    assert [r[2] for r in env.to(1, "RVR")] == [True, True]
    assert [r[2] for r in env.to(2, "RVR")] == [False]
    assert n.disk.voted_for == 1 and n.disk.term == 2 and replies


def test_vote_is_available_again_in_a_higher_term():
    n, env = make_node(term=1)
    n.on_message(1, ("RV", 2, 1, 0, 0))
    n.on_message(2, ("RV", 3, 2, 0, 0))
    assert env.to(2, "RVR")[0][1:] == (3, True)
    assert n.disk.voted_for == 2 and n.disk.term == 3


def test_stale_term_vote_request_refused_and_term_unchanged():
    n, env = make_node(term=5)
    n.on_message(1, ("RV", 3, 1, 10, 3))
    assert env.to(1, "RVR")[0] == ("RVR", 5, False)
    assert n.disk.term == 5 and n.disk.voted_for is None


def test_vote_refused_when_candidate_log_has_older_last_term():
    n, env = make_node(term=1, log=[E(1), E(2)])
    n.on_message(1, ("RV", 3, 1, 5, 1))  # longer log but older last term
    assert env.to(1, "RVR")[0][2] is False


def test_vote_refused_when_same_last_term_but_shorter_log():
    n, env = make_node(term=1, log=[E(1), E(1, 2)])
    n.on_message(1, ("RV", 3, 1, 1, 1))
    assert env.to(1, "RVR")[0][2] is False
    n.on_message(2, ("RV", 3, 2, 2, 1))  # equal log is fine
    assert env.to(2, "RVR")[0][2] is True


def test_vote_granted_when_candidate_has_newer_last_term_even_if_shorter():
    n, env = make_node(term=1, log=[E(1), E(1, 2), E(1, 3)])
    n.on_message(1, ("RV", 2, 1, 1, 2))
    assert env.to(1, "RVR")[0][2] is True


def test_bug_vote_no_log_check_grants_to_stale_candidate():
    n, env = make_node(term=1, log=[E(1), E(2)], bugs=["vote_no_log_check"])
    n.on_message(1, ("RV", 3, 1, 0, 0))
    assert env.to(1, "RVR")[0][2] is True


def test_higher_term_message_makes_leader_step_down():
    n, env = make_leader(term=4)
    assert n.role == "leader"
    n.on_message(1, ("RV", 9, 1, 0, 0))
    assert n.role == "follower" and n.disk.term == 9


def test_candidate_wins_with_majority_votes_only():
    n, env = make_node()
    n.start_election()
    assert n.role == "candidate" and n.disk.term == 1 and n.disk.voted_for == 0
    assert len(env.to(1, "RV")) == 1
    n.on_message(1, ("RVR", 1, True))
    assert n.role == "candidate"
    n.on_message(2, ("RVR", 1, False))
    n.on_message(3, ("RVR", 1, True))
    assert n.role == "leader" and env.leaders == [(0, 1)]


def test_vote_reply_from_old_term_does_not_count():
    n, env = make_node()
    n.start_election()
    n.start_election()  # term 2
    n.on_message(1, ("RVR", 1, True))
    n.on_message(2, ("RVR", 1, True))
    assert n.role == "candidate"


# ---------------- AppendEntries: follower ----------------
def test_ae_with_stale_term_rejected():
    n, env = make_node(term=5, log=[E(1)])
    n.on_message(1, ("AE", 3, 1, 0, 0, (E(3),), 0))
    assert env.to(1, "AER")[0][:3] == ("AER", 5, False)
    assert len(n.disk.log) == 1 and n.disk.term == 5


def test_ae_prev_beyond_log_fails_with_short_log_hint():
    n, env = make_node(term=1, log=[E(1)])
    n.on_message(1, ("AE", 1, 1, 4, 1, (E(1, 5),), 0))
    r = env.to(1, "AER")[0]
    assert r[2] is False and r[5:] == (0, 2)  # conflict_term 0, conflict_idx len+1


def test_ae_prev_term_mismatch_fails_with_first_index_of_conflicting_term():
    n, env = make_node(term=3, log=[E(1), E(2), E(2, 3), E(2, 4)])
    n.on_message(1, ("AE", 3, 1, 4, 3, (), 0))
    r = env.to(1, "AER")[0]
    assert r[2] is False and r[5:] == (2, 2)


def test_ae_appends_and_truncates_conflicting_suffix():
    n, env = make_node(term=2, log=[E(1), E(1, 2), E(1, 3)])
    n.on_message(1, ("AE", 2, 1, 1, 1, (E(2, 7), E(2, 8)), 0))
    assert n.disk.log == [E(1), E(2, 7), E(2, 8)]
    assert env.to(1, "AER")[0][2:5] == (True, 1, 2)


def test_duplicate_or_stale_ae_does_not_truncate_matching_suffix():
    log = [E(1), E(1, 2), E(1, 3), E(1, 4)]
    n, env = make_node(term=1, log=log)
    n.on_message(1, ("AE", 1, 1, 1, 1, (E(1, 2),), 0))  # old AE covering only index 2
    assert n.disk.log == log


def test_bug_truncate_on_stale_ae_loses_suffix():
    log = [E(1), E(1, 2), E(1, 3), E(1, 4)]
    n, env = make_node(term=1, log=log, bugs=["truncate_on_stale_ae"])
    n.on_message(1, ("AE", 1, 1, 1, 1, (E(1, 2),), 0))
    assert len(n.disk.log) == 2


def test_bug_skip_prevlog_accepts_inconsistent_append():
    n, env = make_node(term=1, log=[E(1)], bugs=["skip_prevlog_check"])
    n.on_message(1, ("AE", 1, 1, 5, 1, (E(1, 9),), 0))
    assert env.to(1, "AER")[0][2] is True and len(n.disk.log) == 2


def test_follower_commit_is_min_of_leader_commit_and_last_new_entry():
    n, env = make_node(term=1, log=[E(1), E(1, 2), E(1, 3)])
    n.on_message(1, ("AE", 1, 1, 1, 1, (E(1, 2),), 9))
    assert n.commit_index == 2  # last new entry, not leader_commit=9 and not log length 3
    n.on_message(1, ("AE", 1, 1, 3, 1, (), 3))
    assert n.commit_index == 3 and [a[0] for a in env.applied] == [1, 2, 3]


def test_ae_updates_leader_hint_and_resets_candidate_to_follower():
    n, env = make_node()
    n.start_election()
    n.on_message(2, ("AE", 1, 2, 0, 0, (), 0))
    assert n.role == "follower" and n.leader_id == 2


# ---------------- leader: commit rule and backtracking ----------------
def test_leader_does_not_commit_previous_term_entry_by_counting_replicas():
    n, env = make_leader(term=4, log=[E(2)])
    n.on_message(1, ("AER", 4, True, 0, 1, 0, 0))
    n.on_message(2, ("AER", 4, True, 0, 1, 0, 0))  # entry on 3 of 5 servers
    assert n.commit_index == 0


def test_leader_commits_old_entry_indirectly_once_a_current_term_entry_is_replicated():
    n, env = make_leader(term=4, log=[E(2)])
    n.on_message("c9", ("CREQ", "c9", 1, ("put", "k", 1)))
    assert len(n.disk.log) == 2
    n.on_message(1, ("AER", 4, True, 0, 2, 0, 0))
    assert n.commit_index == 0
    n.on_message(2, ("AER", 4, True, 0, 2, 0, 0))
    assert n.commit_index == 2


def test_bug_commit_old_terms_commits_figure8_entry():
    n, env = make_leader(term=4, log=[E(2)], bugs=["commit_old_terms"])
    n.on_message(1, ("AER", 4, True, 0, 1, 0, 0))
    n.on_message(2, ("AER", 4, True, 0, 1, 0, 0))
    assert n.commit_index == 1


def test_leader_needs_a_real_majority_in_a_five_node_cluster():
    n, env = make_leader(term=4, log=[E(4)])
    n.on_message(1, ("AER", 4, True, 0, 1, 0, 0))
    assert n.commit_index == 0  # leader + 1 = 2 of 5
    n.on_message(2, ("AER", 4, True, 0, 1, 0, 0))
    assert n.commit_index == 1


def test_leader_noop_is_appended_in_new_term():
    n, env = make_node(term=4, log=[E(2)], noop=True)
    n.role = "candidate"
    n.become_leader()
    assert n.disk.log[-1] == (4, (None, 0, ("noop",)))


def test_backtracking_uses_conflict_term_when_leader_has_it():
    n, env = make_leader(term=5, log=[E(1), E(2), E(2, 3), E(4, 4)])
    n.next[1] = 4
    n.on_message(1, ("AER", 5, False, 3, 0, 2, 2))
    assert n.next[1] == 4  # last index of term 2 is 3, so next = 4
    n.on_message(1, ("AER", 5, False, 3, 0, 3, 2))
    assert n.next[1] == 2  # leader has no term 3: jump to follower's conflict index


def test_backtracking_never_goes_below_match_index_and_resends():
    n, env = make_leader(term=5, log=[E(1), E(1, 2), E(1, 3)])
    n.match[1] = 2
    env.clear()
    n.on_message(1, ("AER", 5, False, 3, 0, 0, 1))  # stale failure
    assert n.next[1] == 3
    assert env.to(1, "AE")[0][3] == 2  # prev index 2


def test_ae_reply_with_higher_term_demotes_leader():
    n, env = make_leader(term=4, log=[E(4)])
    n.on_message(1, ("AER", 7, False, 0, 0, 0, 0))
    assert n.role == "follower" and n.disk.term == 7


def test_batch_size_limits_entries_per_append():
    n, env = make_node(term=4, log=[E(2, i) for i in range(1, 6)], batch=2)
    n.role = "candidate"
    n.become_leader()
    n.next[1] = 1
    env.clear()
    n.send_append(1)
    assert len(env.to(1, "AE")[0][5]) == 2


# ---------------- clients ----------------
def test_follower_redirects_client_with_leader_hint():
    n, env = make_node()
    n.leader_id = 3
    n.on_message("c1", ("CREQ", "c1", 1, ("get", "k")))
    assert env.to("c1")[0] == ("CRESP", 1, "notleader", 3)


def test_leader_appends_a_retried_request_only_once():
    n, env = make_leader(term=4)
    for _ in range(3):
        n.on_message("c1", ("CREQ", "c1", 1, ("put", "k", 1)))
    assert len(n.disk.log) == 1


def test_leader_answers_applied_duplicate_from_session_cache_and_drops_stale():
    n, env = make_leader(term=4, n=1)
    n.on_message("c1", ("CREQ", "c1", 1, ("put", "k", 1)))
    n._advance_commit()
    assert n.commit_index == 1
    assert env.to("c1", "CRESP")[-1] == ("CRESP", 1, "ok", "ok")
    env.clear()
    n.on_message("c1", ("CREQ", "c1", 1, ("put", "k", 1)))
    assert env.to("c1", "CRESP") == [("CRESP", 1, "ok", "ok")] and len(n.disk.log) == 1
    n.on_message("c1", ("CREQ", "c1", 2, ("get", "k")))
    n._advance_commit()
    env.clear()
    n.on_message("c1", ("CREQ", "c1", 1, ("put", "k", 1)))  # now stale
    assert env.to("c1") == []


def test_bug_local_reads_answers_get_without_logging():
    n, env = make_leader(term=4, bugs=["local_reads"])
    n.kv.data["k"] = 5
    n.on_message("c1", ("CREQ", "c1", 1, ("get", "k")))
    assert env.to("c1")[0] == ("CRESP", 1, "ok", 5) and n.disk.log == []
    n2, env2 = make_leader(term=4)
    n2.on_message("c1", ("CREQ", "c1", 1, ("get", "k")))
    assert env2.to("c1") == [] and len(n2.disk.log) == 1
