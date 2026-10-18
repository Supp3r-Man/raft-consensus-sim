"""Replicated state machine: a key-value store with per-client request dedup."""
from __future__ import annotations


class KVStore:
    """Commands are (client, req, op) with op one of
    ("put", k, v) -> "ok", ("get", k) -> value or None,
    ("cas", k, old, new) -> True if applied else False.

    Dedup: each client has at most one outstanding request and request ids
    increase, so remembering the last (req, result) per client is enough.
    A repeated request returns the cached result without re-executing; an
    older request than the last seen is stale and produces no reply.
    """

    def __init__(self):
        self.data = {}
        self.sessions = {}

    def apply(self, cmd):
        """Return (should_reply, result)."""
        client, req, op = cmd
        if client is None:  # leader no-op entry
            return False, None
        sess = self.sessions.get(client)
        if sess is not None and req <= sess[0]:
            if req == sess[0]:
                return True, sess[1]
            return False, None
        result = self._exec(op)
        self.sessions[client] = (req, result)
        return True, result

    def _exec(self, op):
        kind = op[0]
        if kind == "put":
            self.data[op[1]] = op[2]
            return "ok"
        if kind == "get":
            return self.data.get(op[1])
        if kind == "cas":
            if self.data.get(op[1]) == op[2]:
                self.data[op[1]] = op[3]
                return True
            return False
        raise ValueError("unknown op %r" % (op,))
