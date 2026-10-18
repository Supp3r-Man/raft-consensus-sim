"""Test helpers: a fake environment for driving one Node directly."""
import random

from raftsim.node import Disk, Node, RaftConfig


class FakeSim:
    def __init__(self):
        self.now = 0
        self.timers = []

    def at(self, delay, kind, fn, *args):
        self.timers.append((delay, kind, fn.__name__, args))


class FakeEnv:
    def __init__(self):
        self.sim = FakeSim()
        self.sent = []
        self.leaders = []
        self.commits = []
        self.applied = []
        self.candidates = []

    def send(self, src, dst, msg):
        self.sent.append((src, dst, msg))

    def on_leader(self, nid, term, log):
        self.leaders.append((nid, term))

    def on_candidate(self, nid, term):
        self.candidates.append((nid, term))

    def on_commit(self, nid, old, new, log):
        self.commits.append((old, new))

    def on_apply(self, nid, idx, entry):
        self.applied.append((idx, entry))

    def to(self, dst, kind=None):
        return [m for (_, d, m) in self.sent if d == dst and (kind is None or m[0] == kind)]

    def clear(self):
        self.sent.clear()


def E(term, k=1, client="c0"):
    """A log entry of the given term carrying a put."""
    return (term, (client, k, ("put", "k", k)))


def make_node(nid=0, n=5, bugs=(), noop=False, log=(), term=0, voted=None, batch=8):
    env = FakeEnv()
    disk = Disk()
    disk.term = term
    disk.voted_for = voted
    disk.log.extend(log)
    cfg = RaftConfig(bugs=frozenset(bugs), leader_noop=noop, max_batch=batch)
    node = Node(nid, list(range(n)), disk, env, cfg, random.Random(1))
    return node, env


def make_leader(nid=0, n=5, bugs=(), log=(), term=4, noop=False, batch=8):
    node, env = make_node(nid, n, bugs, noop, log, term)
    node.role = "candidate"
    node.become_leader()
    env.clear()
    return node, env
