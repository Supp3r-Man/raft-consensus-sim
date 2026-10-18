"""Wires nodes, network, clients and the invariant checker into one simulation."""
from __future__ import annotations

import random

from .core import NetConfig, Network, Sim
from .invariants import Invariants
from .node import Disk, Node, RaftConfig


class Cluster:
    def __init__(self, seed, n=5, cfg=None, net=None, trace=False):
        self.seed = seed
        self.n = n
        self.ids = list(range(n))
        self.cfg = cfg or RaftConfig()
        self.sim = Sim(seed, trace)
        self.net = Network(self.sim, random.Random("%s:net" % seed), net or NetConfig(), self._deliver)
        self.disks = {i: Disk() for i in self.ids}
        self.rngs = {i: random.Random("%s:node:%d" % (seed, i)) for i in self.ids}
        self.nodes = {}
        self.endpoints = {}
        self.history = []
        self.violations = []
        self.inv = Invariants(self)
        self.leader_events = []     # (time, node id, term)
        self.candidate_events = []  # (time, node id, term)
        for i in self.ids:
            self._boot(i)

    # ---- node lifecycle ----
    def _boot(self, i):
        node = Node(i, self.ids, self.disks[i], self, self.cfg, self.rngs[i])
        self.nodes[i] = node
        node.start()

    def crash(self, i):
        node = self.nodes.get(i)
        if node is None:
            return
        node.dead = True
        self.nodes[i] = None
        self.sim.log("crash %d" % i)

    def restart(self, i):
        if self.nodes.get(i) is not None:
            return
        if "no_persist_vote" in self.cfg.bugs:
            self.disks[i].voted_for = None
        self.sim.log("restart %d" % i)
        self._boot(i)

    def up_ids(self):
        return [i for i in self.ids if self.nodes[i] is not None]

    def leader(self):
        """The up node that currently believes it is leader (highest term)."""
        best = None
        for i in self.ids:
            nd = self.nodes[i]
            if nd is not None and nd.role == "leader" and (best is None or nd.term > best.term):
                best = nd
        return best

    def heal_all(self):
        self.net.heal()
        for i in self.ids:
            self.restart(i)

    # ---- transport ----
    def send(self, src, dst, msg):
        self.net.send(src, dst, msg)

    def _deliver(self, dst, src, msg):
        if isinstance(dst, int):
            node = self.nodes.get(dst)
            if node is not None:
                node.on_message(src, msg)
        else:
            ep = self.endpoints.get(dst)
            if ep is not None:
                ep.on_message(src, msg)

    # ---- hooks called by nodes ----
    def on_leader(self, nid, term, log):
        self.leader_events.append((self.sim.now, nid, term))
        self.sim.log("leader %d term %d" % (nid, term))
        self.inv.on_leader(nid, term, log)

    def on_candidate(self, nid, term):
        self.candidate_events.append((self.sim.now, nid, term))

    def on_commit(self, nid, old, new, log):
        self.inv.on_commit(nid, old, new, log)

    def on_apply(self, nid, idx, entry):
        self.inv.on_apply(nid, idx, entry)

    # ---- running ----
    def _check(self):
        self.inv.after_event()
        if self.violations:
            self.sim.stopped = True

    def run(self, until, check=True):
        self.sim.run_until(until, self._check if check else None)

    def trace_digest(self):
        import hashlib
        h = hashlib.sha256()
        for t in self.sim.trace:
            h.update(repr(t).encode())
        return h.hexdigest()
