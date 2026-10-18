"""Deterministic discrete-event core: a virtual clock and a seeded network.

Time is integer virtual milliseconds. Nothing here reads the wall clock or the
global random module, so a seed fully determines a run.
"""
from __future__ import annotations

import heapq
from collections import Counter
from dataclasses import dataclass


class Sim:
    """Event heap with a virtual clock. Ties are broken by insertion order."""

    def __init__(self, seed, trace=False):
        self.seed = seed
        self.now = 0
        self._heap = []
        self._seq = 0
        self.trace_on = trace
        self.trace = []
        self.stopped = False

    def at(self, delay, kind, fn, *args):
        if delay < 0:
            raise ValueError("negative delay")
        self._seq += 1
        heapq.heappush(self._heap, (self.now + delay, self._seq, kind, fn, args))

    def log(self, text):
        if self.trace_on:
            self.trace.append((self.now, "log", text))

    @property
    def events_run(self):
        return self._seq - len(self._heap)

    def step(self):
        t, _, kind, fn, args = heapq.heappop(self._heap)
        self.now = t
        if self.trace_on:
            self.trace.append((t, kind, repr(args)))
        fn(*args)

    def run_until(self, t_end, after_event=None):
        while self._heap and self._heap[0][0] <= t_end and not self.stopped:
            self.step()
            if after_event is not None:
                after_event()
        if not self.stopped and self.now < t_end:
            self.now = t_end


@dataclass
class NetConfig:
    delay_lo: int = 5
    delay_hi: int = 15
    drop: float = 0.0
    dup: float = 0.0
    reorder: float = 0.0       # probability a copy gets extra delay
    reorder_extra: int = 100   # extra delay is uniform in [0, reorder_extra]


class Network:
    """Seeded lossy network. Partitions are directed (src, dst) blocks between
    node ids (ints). Client endpoints are strings and are never partitioned."""

    def __init__(self, sim, rng, cfg, deliver):
        self.sim = sim
        self.rng = rng
        self.cfg = cfg
        self.deliver = deliver
        self.blocked = set()
        self.stats = Counter()

    def send(self, src, dst, msg):
        self.stats["sent"] += 1
        if (src, dst) in self.blocked:
            self.stats["blocked"] += 1
            return
        if self.rng.random() < self.cfg.drop:
            self.stats["dropped"] += 1
            return
        copies = 2 if self.rng.random() < self.cfg.dup else 1
        if copies == 2:
            self.stats["duplicated"] += 1
        for _ in range(copies):
            d = self.rng.randint(self.cfg.delay_lo, self.cfg.delay_hi)
            if self.rng.random() < self.cfg.reorder:
                d += self.rng.randint(0, self.cfg.reorder_extra)
                self.stats["reordered"] += 1
            self.sim.at(d, "deliver", self.deliver, dst, src, msg)

    # --- fault controls ---
    def block(self, src, dst):
        """Directed block: src can no longer reach dst."""
        self.blocked.add((src, dst))

    def partition(self, *groups):
        """Symmetric partition: nodes in different groups cannot talk."""
        for a, ga in enumerate(groups):
            for b, gb in enumerate(groups):
                if a != b:
                    for x in ga:
                        for y in gb:
                            self.blocked.add((x, y))

    def isolate(self, node, ids):
        for o in ids:
            if o != node:
                self.blocked.add((node, o))
                self.blocked.add((o, node))

    def heal(self):
        self.blocked.clear()
