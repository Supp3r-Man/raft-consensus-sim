"""CLI: python -m raftsim demo | campaign | trace

Exit codes: 0 ok / no violation, 1 violation found, 2 usage error or crash.
"""
from __future__ import annotations

import argparse
import sys

from .node import BUG_NAMES


def _parser():
    p = argparse.ArgumentParser(prog="raftsim", description="Raft in a deterministic simulator")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("demo", help="print the measured numbers (fixed seeds)")
    d.add_argument("--quick", action="store_true", help="fewer seeds (used by CI)")
    c = sub.add_parser("campaign", help="run the randomized fault campaign")
    c.add_argument("--bug", choices=BUG_NAMES, default=None, help="inject one deliberate bug")
    c.add_argument("--seeds", type=int, default=100)
    c.add_argument("--start", type=int, default=0)
    c.add_argument("--all", action="store_true", help="do not stop at the first violation")
    t = sub.add_parser("trace", help="print the head of the trace for one seed")
    t.add_argument("--seed", type=int, default=0)
    t.add_argument("--lines", type=int, default=30)
    return p


def main(argv=None):
    try:
        args = _parser().parse_args(argv)
    except SystemExit as e:
        return 2 if e.code else 0
    try:
        if args.cmd == "demo":
            from .experiments import run_demo
            print(run_demo(quick=args.quick))
            return 0
        if args.cmd == "campaign":
            from .campaign import run_seed
            bugs = (args.bug,) if args.bug else ()
            found = 0
            for s in range(args.start, args.start + args.seeds):
                r = run_seed(s, bugs)
                if not r.ok:
                    found += 1
                    print("seed %d: %s (%s)" % (s, r.kind, r.detail))
                    if not args.all:
                        break
            print("%d seeds, %d with violations" % (s - args.start + 1 if not args.all else args.seeds, found))
            return 1 if found else 0
        if args.cmd == "trace":
            from .campaign import run_seed
            r = run_seed(args.seed, trace=True)
            print(r)
            return 0
    except Exception as e:  # fail closed: could not judge
        print("error: %r" % (e,), file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())
