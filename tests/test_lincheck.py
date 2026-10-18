import random

from raftsim.lincheck import HistOp, check_history, check_key, model_step


def op(client, req, o, invoke, ret=None, result=None):
    return HistOp(client, req, o, invoke, ret, result)


def put(c, r, k, v, inv, ret=None):
    return op(c, r, ("put", k, v), inv, ret, "ok" if ret is not None else None)


def get(c, r, k, inv, ret=None, res=None):
    return op(c, r, ("get", k), inv, ret, res)


def cas(c, r, k, old, new, inv, ret=None, res=None):
    return op(c, r, ("cas", k, old, new), inv, ret, res)


def test_empty_history_is_linearizable():
    assert check_history([]).ok


def test_sequential_history_ok():
    h = [put("a", 1, "x", 1, 0, 1), get("a", 2, "x", 2, 3, 1)]
    assert check_history(h).ok


def test_stale_read_after_completed_put_is_rejected():
    h = [put("a", 1, "x", 1, 0, 1), put("a", 2, "x", 2, 2, 3), get("b", 1, "x", 4, 5, 1)]
    r = check_history(h)
    assert not r.ok and r.key == "x"


def test_read_of_never_written_value_is_rejected():
    h = [put("a", 1, "x", 1, 0, 1), get("b", 1, "x", 2, 3, 99)]
    assert not check_history(h).ok


def test_get_before_any_put_returns_none():
    assert check_history([get("a", 1, "x", 0, 1, None)]).ok
    assert not check_history([get("a", 1, "x", 0, 1, 5)]).ok


def test_concurrent_put_and_get_may_see_old_or_new():
    base = put("a", 1, "x", 1, 0, 1)
    p = put("a", 2, "x", 2, 10, 20)
    assert check_history([base, p, get("b", 1, "x", 12, 15, 1)]).ok
    assert check_history([base, put("a", 2, "x", 2, 10, 20), get("b", 1, "x", 12, 15, 2)]).ok


def test_new_old_inversion_between_readers_is_rejected():
    # one put overlaps both gets; r1 sees the new value and finishes before r2 starts and sees the old one
    h = [
        put("w", 1, "x", 1, 0, 5),
        put("w", 2, "x", 2, 10, 100),
        get("r", 1, "x", 20, 30, 2),
        get("r", 2, "x", 40, 50, 1),
    ]
    assert not check_history(h).ok


def test_real_time_order_of_writes_is_enforced():
    h = [put("a", 1, "x", 1, 0, 1), put("a", 2, "x", 2, 2, 3), get("b", 1, "x", 5, 6, 1)]
    assert not check_history(h).ok
    h[2] = get("b", 1, "x", 5, 6, 2)
    assert check_history(h).ok


def test_cas_semantics():
    h = [put("a", 1, "x", 1, 0, 1), cas("a", 2, "x", 1, 2, 2, 3, True), get("a", 3, "x", 4, 5, 2)]
    assert check_history(h).ok
    h = [put("a", 1, "x", 1, 0, 1), cas("a", 2, "x", 7, 2, 2, 3, False), get("a", 3, "x", 4, 5, 1)]
    assert check_history(h).ok


def test_cas_claiming_wrong_outcome_is_rejected():
    h = [put("a", 1, "x", 1, 0, 1), cas("a", 2, "x", 7, 2, 2, 3, True)]
    assert not check_history(h).ok
    h = [put("a", 1, "x", 1, 0, 1), cas("a", 2, "x", 1, 2, 2, 3, False)]
    assert not check_history(h).ok


def test_two_concurrent_cas_only_one_can_win():
    h = [put("a", 1, "x", 1, 0, 1),
         cas("a", 2, "x", 1, 2, 5, 9, True),
         cas("b", 1, "x", 1, 3, 6, 8, True)]
    assert not check_history(h).ok
    h[2] = cas("b", 1, "x", 1, 3, 6, 8, False)
    assert check_history(h).ok


def test_pending_write_may_have_taken_effect():
    h = [put("a", 1, "x", 1, 0, 1), put("b", 1, "x", 2, 5), get("c", 1, "x", 10, 11, 2)]
    assert check_history(h).ok


def test_pending_write_is_not_required_to_have_taken_effect():
    h = [put("a", 1, "x", 1, 0, 1), put("b", 1, "x", 2, 5), get("c", 1, "x", 10, 11, 1)]
    assert check_history(h).ok


def test_pending_write_cannot_explain_a_read_before_its_invocation():
    h = [put("b", 1, "x", 2, 50), get("c", 1, "x", 10, 11, 2)]
    assert not check_history(h).ok


def test_pending_get_is_ignored():
    h = [put("a", 1, "x", 1, 0, 1), get("b", 1, "x", 5)]
    assert check_history(h).ok


def test_keys_are_checked_independently_and_violating_key_is_reported():
    h = [put("a", 1, "x", 1, 0, 1), get("a", 2, "x", 2, 3, 1),
         put("a", 3, "y", 1, 4, 5), get("a", 4, "y", 6, 7, 2)]
    r = check_history(h)
    assert not r.ok and r.key == "y"


def test_search_budget_exhaustion_is_reported_not_silently_ok():
    # many concurrent writes + an unsatisfiable read forces a big search
    h = [put("w%d" % i, 1, "x", i, 0, 100) for i in range(12)] + [get("r", 1, "x", 200, 201, 999)]
    ok, explored, out_of_budget = check_key(h, budget=50)
    assert out_of_budget and not ok
    r = check_history(h, budget=50)
    assert r.unknown


def test_model_step():
    assert model_step(None, ("put", "k", 3)) == (3, "ok")
    assert model_step(3, ("get", "k")) == (3, 3)
    assert model_step(3, ("cas", "k", 3, 4)) == (4, True)
    assert model_step(3, ("cas", "k", 9, 4)) == (3, False)


def _sequential_history(rng, n_ops):
    """Strictly sequential history generated from the sequential model."""
    value, t, h = None, 0, []
    for i in range(n_ops):
        k = rng.choice(["put", "get", "cas"])
        if k == "put":
            o = ("put", "x", i)
        elif k == "get":
            o = ("get", "x")
        else:
            o = ("cas", "x", rng.choice([value, None, 7]), i)
        value, res = model_step(value, o)
        h.append(op("c", i, o, t, t + 1, res))
        t += 2
    return h


def test_random_sequential_histories_from_the_model_are_linearizable():
    rng = random.Random(7)
    for _ in range(40):
        assert check_history(_sequential_history(rng, 12)).ok


def test_corrupting_one_result_of_a_sequential_history_is_always_caught():
    rng = random.Random(8)
    for _ in range(40):
        h = _sequential_history(rng, 12)
        gets = [o for o in h if o.op[0] == "get"]
        if not gets:
            continue
        victim = rng.choice(gets)
        victim.result = "never-written"
        assert not check_history(h).ok
