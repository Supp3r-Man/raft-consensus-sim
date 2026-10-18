"""Simulated clients: closed-loop KV clients (history for the linearizability
check) and one-shot probes (for availability measurements)."""
from __future__ import annotations

from .lincheck import HistOp


class Client:
    """One outstanding request at a time; retries the same request id (so the
    state machine dedups) against a rotating / hinted target."""

    def __init__(self, cluster, cid, rng, keys=("a", "b", "c"), think=(5, 50), retry=300):
        self.c = cluster
        self.name = "c%d" % cid
        self.cid = cid
        self.rng = rng
        self.keys = list(keys)
        self.think = think
        self.retry = retry
        self.req = 0
        self.target = rng.randrange(cluster.n)
        self.cur = None
        self.attempt = 0
        self.redirecting = False
        self.known = {}
        self.stopped = False
        self.completed = 0
        cluster.endpoints[self.name] = self

    @property
    def idle(self):
        return self.cur is None

    def start(self, delay=0):
        self.c.sim.at(delay, "client", self.issue)

    def stop(self):
        self.stopped = True

    def _gen_op(self):
        r = self.rng
        key = r.choice(self.keys)
        x = r.random()
        uniq = self.cid * 1_000_000 + self.req
        if x < 0.40:
            return ("put", key, uniq)
        if x < 0.75:
            return ("get", key)
        old = self.known.get(key) if r.random() < 0.7 else r.choice([None, self.known.get(key), uniq - 1])
        return ("cas", key, old, uniq)

    def issue(self):
        if self.stopped or self.cur is not None:
            return
        self.req += 1
        op = self._gen_op()
        rec = HistOp(self.name, self.req, op, self.c.sim.now)
        self.cur = rec
        self.c.history.append(rec)
        self._send()

    def _send(self):
        self.attempt += 1
        self.redirecting = False
        rec = self.cur
        self.c.send(self.name, self.target, ("CREQ", self.name, rec.req, rec.op))
        self.c.sim.at(self.retry, "cretry", self._on_retry, rec.req, self.attempt)

    def _on_retry(self, req, attempt):
        if self.cur is None or self.cur.req != req or attempt != self.attempt:
            return
        self.target = (self.target + 1) % self.c.n
        self._send()

    def _redirect(self, req, attempt):
        if self.cur is None or self.cur.req != req or attempt != self.attempt:
            return
        self._send()

    def on_message(self, src, msg):
        if msg[0] != "CRESP":
            return
        _, req, status, payload = msg
        if self.cur is None or self.cur.req != req:
            return
        if status == "ok":
            rec = self.cur
            rec.ret = self.c.sim.now
            rec.result = payload
            self.cur = None
            self.completed += 1
            op = rec.op
            if op[0] == "put":
                self.known[op[1]] = op[2]
            elif op[0] == "get":
                self.known[op[1]] = payload
            elif op[0] == "cas" and payload is True:
                self.known[op[1]] = op[3]
            self.target = src if isinstance(src, int) else self.target
            self.c.sim.at(self.rng.randint(*self.think), "client", self.issue)
        else:
            if self.redirecting:
                return
            self.redirecting = True
            self.target = payload if payload is not None else (self.target + 1) % self.c.n
            self.c.sim.at(10, "cretry", self._redirect, self.cur.req, self.attempt)


class Prober:
    """Open-loop availability probe: every `period` ms start a put that must be
    answered within `deadline` ms (retrying other nodes every `retry` ms)."""

    def __init__(self, cluster, start, end, period=50, deadline=500, retry=120):
        self.c = cluster
        self.period, self.deadline, self.retry = period, deadline, retry
        self.hint = 0
        self.end = end
        self.results = []   # (start_time, success, latency)
        self.k = 0
        cluster.sim.at(max(0, start - cluster.sim.now), "probe", self._launch)

    def _launch(self):
        if self.c.sim.now >= self.end:
            return
        self.k += 1
        _Probe(self, self.k)
        self.c.sim.at(self.period, "probe", self._launch)


class _Probe:
    def __init__(self, parent, k):
        self.p = parent
        self.name = "p%d" % k
        self.k = k
        self.t0 = parent.c.sim.now
        self.target = parent.hint
        self.done = False
        self.redirecting = False
        parent.c.endpoints[self.name] = self
        self._send()
        parent.c.sim.at(parent.deadline, "probe", self._expire)

    def _send(self):
        self.redirecting = False
        c = self.p.c
        c.send(self.name, self.target, ("CREQ", self.name, 1, ("put", "probe", self.k)))
        c.sim.at(self.p.retry, "probe", self._retry)

    def _retry(self):
        if self.done:
            return
        self.target = (self.target + 1) % self.p.c.n
        self._send()

    def _expire(self):
        if not self.done:
            self._finish(False)

    def _finish(self, ok):
        self.done = True
        c = self.p.c
        self.p.results.append((self.t0, ok, c.sim.now - self.t0))
        c.endpoints.pop(self.name, None)

    def on_message(self, src, msg):
        if self.done or msg[0] != "CRESP":
            return
        _, req, status, payload = msg
        if status == "ok":
            self.p.hint = src
            self._finish(True)
        elif not self.redirecting:
            self.redirecting = True
            self.target = payload if payload is not None else (self.target + 1) % self.p.c.n
            self.p.c.sim.at(10, "probe", self._redirect)

    def _redirect(self):
        if not self.done:
            self._send()
