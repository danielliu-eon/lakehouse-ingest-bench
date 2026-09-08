"""Command line for building a corpus and for merging the shards of one.

A corpus is built once and read by every run that is scored against it, so the
two commands here are the only way one is created: the preset and the seed name
the corpus, and `corpus.json` inside it is the record of what was built.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from ingest_bench import uri
from ingest_bench.corpus import merge as merge_corpora
from ingest_bench.corpus import values as v
from ingest_bench.corpus.generate import generate
from ingest_bench.corpus.preset import Preset, corpus_dir_name, corpus_dir_name_for, corpus_hash, load_preset

WORKLOADS_ENV = "INGEST_BENCH_WORKLOADS"

# A plan cannot know what the corpus will compress to, and the figure is worth
# printing anyway: it is what decides whether the corpus fits the bucket and the
# machine it is staged on. Zstd returns about this on a corpus whose payload is
# incompressible by construction; `corpus.json` publishes what it actually stored.
ASSUMED_COMPRESSION_RATIO = 4


def workloads_dir(explicit: str | None) -> Path:
    """Where presets and schemas are read from.

    The default is the directory shipped beside the package, which is right for
    a checkout and wrong for an installed wheel or a container that mounts its
    workloads elsewhere — hence the environment variable, so a runner sets it
    once instead of passing it to every command.
    """
    if explicit:
        return Path(explicit)
    from_env = os.environ.get(WORKLOADS_ENV)
    if from_env:
        return Path(from_env)
    return Path(__file__).resolve().parents[2] / "workloads"


def print_plan(preset: Preset, seed: int) -> None:
    """What generating this preset would cost, without writing a byte.

    The row figures come from the same calibration the generator runs, so the
    plan is the generator's own estimate rather than a second model of it. They
    are lower bounds: a batch is filled in whole row blocks and stops on the
    first block that crosses its byte budget, so it overshoots slightly.
    """
    payload_width = v.calibrate_payload_width(seed, preset.target_row_bytes, preset.columns)
    mean_row = v.realized_encoded_row_size(seed, payload_width, preset.columns)
    rows = max(1, int(preset.batch_bytes / mean_row)) * preset.batch_count
    encoded = preset.batch_bytes * preset.batch_count
    print(f"preset: {preset.name} (hash {corpus_hash(preset)})")
    print(f"batches: {preset.batch_count} x {preset.batch_interval_ms} ms, {preset.batch_bytes} encoded bytes each")
    print(f"estimated rows: {rows} ({rows // preset.duration_s}/s), mean row {mean_row:.1f} bytes")
    print(f"estimated encoded bytes: {encoded}")
    print(
        f"estimated stored bytes: ~{encoded // ASSUMED_COMPRESSION_RATIO} (assumes ~{ASSUMED_COMPRESSION_RATIO}:1 zstd)"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="corpus", description="Build a benchmark corpus, or merge the shards of one.")
    commands = parser.add_subparsers(dest="command", required=True)

    gen = commands.add_parser("gen", help="build a corpus, or one shard of one")
    gen.add_argument("--preset", required=True, help="a shipped preset name, or a path to a preset YAML file")
    gen.add_argument(
        "--set", dest="overrides", action="append", default=[], metavar="KEY=VALUE", help="override a preset key"
    )
    gen.add_argument("--out", required=True, metavar="URI", help="parent of the corpus directory to write")
    gen.add_argument("--seed", type=int, default=1)
    gen.add_argument("--shard-index", type=int, default=0)
    gen.add_argument("--shard-count", type=int, default=1)
    gen.add_argument("--zstd-level", type=int, default=3)
    gen.add_argument("--workloads", metavar="DIR", help=f"preset and schema directory (env {WORKLOADS_ENV})")
    gen.add_argument("--plan", action="store_true", help="print what would be generated and exit")

    merge = commands.add_parser("merge", help="merge shard corpora into one corpus")
    merge.add_argument("shard_uris", nargs="+", metavar="SHARD_URI")
    merge.add_argument("--out", required=True, metavar="URI", help="parent of the corpus directory to write")
    return parser


def run_gen(args: argparse.Namespace) -> int:
    preset = load_preset(
        str(args.preset),
        workloads_dir=workloads_dir(None if args.workloads is None else str(args.workloads)),
        overrides=[str(assignment) for assignment in args.overrides],
    )
    seed = int(args.seed)
    if args.plan:
        print_plan(preset, seed)
        return 0
    shard_index, shard_count = int(args.shard_index), int(args.shard_count)
    meta = generate(
        preset,
        str(args.out),
        seed=seed,
        shard_index=shard_index,
        shard_count=shard_count,
        zstd_level=int(args.zstd_level),
    )
    shard = "" if shard_count == 1 else f" shard {shard_index} of {shard_count}"
    print(
        f"wrote {uri.join(str(args.out), corpus_dir_name(preset))}{shard}: "
        f"{meta['row_count']} rows, {meta['encoded_bytes']} encoded bytes, {meta['stored_bytes']} stored bytes"
    )
    return 0


def run_merge(args: argparse.Namespace) -> int:
    shard_uris = [str(shard_uri) for shard_uri in args.shard_uris]
    meta = merge_corpora.merge(shard_uris, str(args.out))
    corpus_uri = uri.join(str(args.out), corpus_dir_name_for(str(meta["name"]), str(meta["corpus_hash"])))
    print(
        f"wrote {corpus_uri} from {len(shard_uris)} shards: "
        f"{meta['row_count']} rows, {meta['encoded_bytes']} encoded bytes"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "gen":
        return run_gen(args)
    return run_merge(args)


def gen_corpus() -> int:
    return main(["gen", *sys.argv[1:]])


def merge_corpus() -> int:
    return main(["merge", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
