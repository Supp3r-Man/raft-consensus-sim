"""Wing-Gong style linearizability checker for a get/put/cas key-value history.

Linearizability is compositional, so each key is checked on its own. For one
key the search repeatedly picks an operation that could be linearized next (no
unlinearized operation returned before it was invoked), applies it to the
sequential model, and backtracks on a result mismatch. Visited (set of
linearized ops, model value) states are memoized (Lowe's optimisation).

Pending operations (no response seen) may or may not have taken effect: they
may be linearized at any point after their invocation with any result, or left
out. Pending gets have no effect and are dropped.
"""
from __future__ import annotations

from dataclasses import dataclass

INF = float("inf")


@dataclass
class HistOp:
    client: str
    req: int
    op: tuple              # ("put",k,v) | ("get",k) | ("cas",k,old,new)
    invoke: int
    ret: object = None     # None while pending
    result: object = None


@dataclass
class LinResult:
    ok: bool
    key: object = None
    reason: str = ""
    explored: int = 0
    unknown: bool = False  # search budget exhausted (treated as "no violation found")


def model_step(value, op):
    kind = op[0]
    if kind == "put":
        return op[2], "ok"
    if kind == "get":
        return value, value
    if kind == "cas":
        if value == op[2]:
            return op[3], True
        return value, False
    raise ValueError("unknown op %r" % (op,))


def check_key(ops, budget=500_000):
    ops = [o for o in ops if o.ret is not None or o.op[0] != "get"]
    ops.sort(key=lambda o: (o.invoke, o.req))
    n = len(ops)
    inv = [o.invoke for o in ops]
    ret = [o.ret if o.ret is not None else INF for o in ops]
    required = 0
    for i, o in enumerate(ops):
        if o.ret is not None:
            required |= 1 << i
    seen = set()
    explored = [0]
    out_of_budget = [False]

    def rec(mask, value):
        if mask & required == required:
            return True
        key = (mask, value)
        if key in seen:
            return False
        explored[0] += 1
        if explored[0] > budget:
            out_of_budget[0] = True
            return False
        seen.add(key)
        min_ret = INF
        for j in range(n):
            if not mask >> j & 1 and ret[j] < min_ret:
                min_ret = ret[j]
        for i in range(n):
            if mask >> i & 1 or inv[i] > min_ret:
                continue
            new_value, result = model_step(value, ops[i].op)
            if ops[i].ret is not None and result != ops[i].result:
                continue
            if rec(mask | 1 << i, new_value):
                return True
            if out_of_budget[0]:
                return False
        return False

    ok = rec(0, None)
    return ok, explored[0], out_of_budget[0]


def check_history(history, budget=500_000):
    """history: list of HistOp. Returns LinResult."""
    by_key = {}
    for o in history:
        by_key.setdefault(o.op[1], []).append(o)
    total = 0
    unknown = False
    for key in sorted(by_key, key=repr):
        ok, explored, oob = check_key(by_key[key], budget)
        total += explored
        if oob:
            unknown = True
            continue
        if not ok:
            return LinResult(False, key, "no linearization exists for key %r" % (key,), total)
    return LinResult(True, None, "", total, unknown)
