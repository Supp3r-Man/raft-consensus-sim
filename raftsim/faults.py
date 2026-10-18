"""Randomized fault injector ("nemesis") for the bug-finding campaign."""
from __future__ import annotations

PROFILES = {
    # name: (gap range ms between faults, action weights)
    "crashy": ((150, 600), {"crash_random": 3, "crash_leader": 4, "crash_two": 1, "isolate_leader": 1,
                            "bounce": 2, "election_chaos": 2, "heal": 1}),
    "partitiony": ((200, 800), {"isolate_leader": 4, "partition": 3, "asym": 2, "crash_leader": 1,
                                "bounce": 1, "heal": 2}),
    "mixed": ((150, 700), {"crash_random": 2, "crash_leader": 2, "isolate_leader": 2, "partition": 2,
                           "asym": 1, "crash_two": 1, "bounce": 2, "heal": 1}),
    "leader_killer": ((100, 400), {"crash_leader": 4, "election_chaos": 3, "bounce": 1, "isolate_leader": 1,
                                   "crash_random": 1}),
}


class Nemesis:
    def __init__(self, cluster, rng, profile="mixed"):
        self.c = cluster
        self.rng = rng
        self.gap, weights = PROFILES[profile]
        self.actions = [a for a in sorted(weights) for _ in range(weights[a])]
        self.stopped = False
        self.log = []
        cluster.sim.at(rng.randint(*self.gap), "fault", self._tick)

    def stop(self):
        self.stopped = True

    def _tick(self):
        if self.stopped:
            return
        action = self.rng.choice(self.actions)
        getattr(self, "_" + action)()
        self.c.sim.at(self.rng.randint(*self.gap), "fault", self._tick)

    def _record(self, text):
        self.log.append((self.c.sim.now, text))
        self.c.sim.log("nemesis " + text)

    def _down_for(self, i):
        self.c.crash(i)
        self.c.sim.at(self.rng.randint(100, 1200), "fault", self.c.restart, i)

    def _heal_later(self):
        self.c.sim.at(self.rng.randint(300, 1500), "fault", self.c.net.heal)

    def _crash_random(self):
        up = self.c.up_ids()
        if up:
            i = self.rng.choice(up)
            self._record("crash %d" % i)
            self._down_for(i)

    def _bounce(self):
        """Quick reboot: down for only a few ms, so volatile state is lost but
        the node is back inside the same election."""
        up = self.c.up_ids()
        if up:
            i = self.rng.choice(up)
            self._record("bounce %d" % i)
            self.c.crash(i)
            self.c.sim.at(self.rng.randint(5, 60), "fault", self.c.restart, i)

    def _bounce_voter(self):
        """Reboot a node that has just voted for someone else (volatile state lost)."""
        cands = [nd for nd in (self.c.nodes[i] for i in self.c.ids)
                 if nd is not None and nd.role == "follower" and nd.disk.voted_for not in (None, nd.id)]
        if cands:
            top = max(nd.term for nd in cands)
            nd = self.rng.choice([x for x in cands if x.term == top])
            self._record("bounce voter %d" % nd.id)
            self.c.crash(nd.id)
            self.c.sim.at(self.rng.randint(2, 40), "fault", self.c.restart, nd.id)

    def _election_chaos(self):
        """Kill the leader, then reboot voters while the election is under way."""
        self._crash_leader()
        for _ in range(4):
            self.c.sim.at(self.rng.randint(80, 350), "fault", self._bounce_voter_if_running)

    def _bounce_voter_if_running(self):
        if not self.stopped:
            self._bounce_voter()

    def _crash_leader(self):
        ld = self.c.leader()
        if ld is None:
            return self._crash_random()
        self._record("crash leader %d" % ld.id)
        self._down_for(ld.id)

    def _crash_two(self):
        up = self.c.up_ids()
        for i in self.rng.sample(up, min(2, len(up))):
            self._record("crash %d" % i)
            self._down_for(i)

    def _isolate_leader(self):
        ld = self.c.leader()
        if ld is None:
            return self._crash_random()
        self._record("isolate leader %d" % ld.id)
        self.c.net.isolate(ld.id, self.c.ids)
        self._heal_later()

    def _partition(self):
        ids = list(self.c.ids)
        self.rng.shuffle(ids)
        k = self.rng.randint(1, len(ids) // 2)
        self._record("partition %s | %s" % (sorted(ids[:k]), sorted(ids[k:])))
        self.c.net.partition(ids[:k], ids[k:])
        self._heal_later()

    def _asym(self):
        x = self.rng.choice(self.c.ids)
        outbound = self.rng.random() < 0.5
        self._record("asymmetric %s %d" % ("cannot send" if outbound else "cannot receive", x))
        for o in self.c.ids:
            if o != x:
                self.c.net.block(x, o) if outbound else self.c.net.block(o, x)
        self._heal_later()

    def _heal(self):
        self._record("heal")
        self.c.net.heal()
