"""Payment generator CLI.

Examples:
    python -m generator file --count 5000 --out data/payments.jsonl
    python -m generator stdout --count 5
"""

import argparse
import collections
import json
import sys
from pathlib import Path

from generator.simulator import PaymentSimulator


def _write_file(args: argparse.Namespace, sim: PaymentSimulator) -> None:
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    patterns: collections.Counter[str] = collections.Counter()
    with out.open("w") as f:
        for event in sim.generate(args.count, args.rate):
            patterns[event["label_pattern"]] += 1
            f.write(json.dumps(event) + "\n")
    fraud = sum(n for p, n in patterns.items() if p != "none")
    print(f"Wrote {args.count} events to {out} ({fraud} fraud, {fraud / args.count:.1%})")
    for pattern, n in sorted(patterns.items()):
        print(f"  {pattern:<24}{n}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m generator", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--users", type=int, default=500)
    parser.add_argument("--merchants", type=int, default=120)
    parser.add_argument("--fraud-rate", type=float, default=0.01, help="probability that a tick starts a fraud episode")
    parser.add_argument("--rate", type=float, default=5.0, help="events per second on the simulated clock")
    parser.add_argument("--seed", type=int, default=None)
    sub = parser.add_subparsers(dest="mode", required=True)

    p_file = sub.add_parser("file", help="write JSON Lines to a file")
    p_file.add_argument("--count", type=int, default=1000)
    p_file.add_argument("--out", default="data/payments.jsonl")

    p_out = sub.add_parser("stdout", help="print JSON Lines")
    p_out.add_argument("--count", type=int, default=10)

    args = parser.parse_args(argv)
    sim = PaymentSimulator(args.users, args.merchants, args.fraud_rate, args.seed)

    if args.mode == "file":
        _write_file(args, sim)
    elif args.mode == "stdout":
        for event in sim.generate(args.count, args.rate):
            sys.stdout.write(json.dumps(event) + "\n")


if __name__ == "__main__":
    main()
