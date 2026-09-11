# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
from typing import cast

import pytest

from ingest_bench.collect.table import _HEADER_ROW, _SEPARATOR_ROW, render_results_table
from tests.test_collect import EXTERNAL_SPEC, _build, _run_dir, _site, _summary


def _table_rows(text: str) -> list[str]:
    lines = text.splitlines()
    start = lines.index(_SEPARATOR_ROW) + 1
    end = lines.index("", start)
    return lines[start:end]


def _cell(row: str, index: int) -> str:
    return row.strip("| ").split(" | ")[index]


def _minimal_document(
    *,
    preset: str,
    engine: str,
    variant: str,
    date: str,
    harness_version: str = "1.2.3",
    run_valid: bool = True,
    state: str = "drained",
    site_pricing: tuple[float, float] = (0.04, 0.005),
    exact: bool = True,
    geometry: dict[str, object] | None = None,
    fleet: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """A minimal document satisfying the shared result-consumer schema."""
    if fleet is None:
        fleet = [{"role": "worker", "count": 1, "vcpu": 2.0, "gib": 4.0, "machine_type": "m6i.large"}]
    vcpu_price, gib_price = site_pricing
    usd_per_hour = sum(
        cast(float, role["count"]) * (cast(float, role["vcpu"]) * vcpu_price + cast(float, role["gib"]) * gib_price)
        for role in fleet
    )
    return {
        "schema_version": 2,
        "collected_at": f"{date}T00:00:00Z",
        "harness_version": harness_version,
        "run": {
            "engine": engine,
            "corpus_hash": None,
            "variant": variant,
            "spec": {"corpus": preset},
            "fleet": fleet,
            "site_pricing": {"vcpu_hour_usd": vcpu_price, "gib_hour_usd": gib_price},
        },
        "data": {"summary": {"run_valid": run_valid, "state": state}},
        "derived": {
            "freshness": {"window": {"p50_s": 7.71, "p95_s": 14.3, "max_s": 27.44}},
            "exactness": {"exact": exact, "loss_rows": 0 if exact else 3, "duplicate_rows": 0 if exact else 1},
            "keepup": {"absorbed_at_offer_end": 0.993, "drain_s": 5.87},
            "cost": {"usd_per_hour": usd_per_hour},
            "producer": {"producer_bound": False},
        },
        "geometry": geometry,
    }


# ---------------------------------------------------------------------------
# Columns, from real collected documents
# ---------------------------------------------------------------------------


def _producer_bound_run_dir(tmp_path: Path) -> Path:
    run_dir = _run_dir(tmp_path)
    summary = _summary()
    summary["run_valid"] = False
    summary["state"] = "producer_bound"
    summary["producer_bound"] = True
    (run_dir / "scores" / "summary.json").write_text(json.dumps(summary))
    return run_dir


def test_render_results_table_renders_every_column(tmp_path: Path) -> None:
    flink = _build(_run_dir(tmp_path / "flink"), _site(tmp_path / "flink"), variant="hash")
    external = _build(_run_dir(tmp_path / "external", spec=EXTERNAL_SPEC), _site(tmp_path / "external"), variant="hash")
    # Same spec as `flink`, so distinguished from it by variant alone — the
    # way two tunings of one engine are told apart in a real `results/`.
    bound = _build(_producer_bound_run_dir(tmp_path / "bound"), _site(tmp_path / "bound"), variant="bound")

    documents = [
        (Path("flink.json"), flink),
        (Path("external.json"), external),
        (Path("bound.json"), bound),
    ]
    text = render_results_table(documents)
    rows = _table_rows(text)
    assert len(rows) == 3
    by_key = {(_cell(row, 0), _cell(row, 1)): row for row in rows}

    flink_row = by_key[("flink", "hash")]
    assert _cell(flink_row, 2) == "smoke"
    assert _cell(flink_row, 3) == "2026-09-20"
    assert _cell(flink_row, 4) == "jobmanager×1 m6i.xlarge + taskmanager×2 m6i.xlarge"
    assert _cell(flink_row, 5) == "valid"
    assert _cell(flink_row, 6) == "7.7/14.3/27.4"
    assert _cell(flink_row, 7) == "exact"
    assert _cell(flink_row, 8) == "99.3% / 5.9s"
    assert _cell(flink_row, 9) == "0.25"
    # `_geometry()`'s `final` is null — no snapshot was ever measured as final.
    assert _cell(flink_row, 10) == "n/a"

    external_row = by_key[("external", "hash")]
    assert _cell(external_row, 4) == "worker×3 n2-standard-4"
    assert _cell(external_row, 9) == "0.60"

    bound_row = by_key[("flink", "bound")]
    assert _cell(bound_row, 5) == "producer_bound"


def test_render_results_table_is_deterministic(tmp_path: Path) -> None:
    site = _site(tmp_path)
    documents = [(Path("flink.json"), _build(_run_dir(tmp_path), site))]
    first = render_results_table(documents)
    second = render_results_table(documents)
    assert first == second


# ---------------------------------------------------------------------------
# Sorting, header and footer
# ---------------------------------------------------------------------------


def test_render_results_table_sorts_by_preset_then_engine_then_variant_then_date() -> None:
    documents = [
        (Path("z"), _minimal_document(preset="zeta", engine="flink", variant="hash", date="2026-01-01")),
        (Path("a-spark"), _minimal_document(preset="alpha", engine="spark", variant="hash", date="2026-01-01")),
        (Path("a-range"), _minimal_document(preset="alpha", engine="flink", variant="range", date="2026-01-01")),
        (Path("a-late"), _minimal_document(preset="alpha", engine="flink", variant="hash", date="2026-01-02")),
        (Path("a-early"), _minimal_document(preset="alpha", engine="flink", variant="hash", date="2026-01-01")),
    ]
    rows = _table_rows(render_results_table(documents))
    assert [(_cell(r, 2), _cell(r, 0), _cell(r, 1), _cell(r, 3)) for r in rows] == [
        ("alpha", "flink", "hash", "2026-01-01"),
        ("alpha", "flink", "hash", "2026-01-02"),
        ("alpha", "flink", "range", "2026-01-01"),
        ("alpha", "spark", "hash", "2026-01-01"),
        ("zeta", "flink", "hash", "2026-01-01"),
    ]


def test_render_results_table_names_the_harness_versions_present() -> None:
    def doc(engine: str, harness_version: str) -> dict[str, object]:
        return _minimal_document(
            preset="p", engine=engine, variant="hash", date="2026-01-01", harness_version=harness_version
        )

    documents = [(Path("a"), doc("flink", "1.0.0")), (Path("b"), doc("spark", "1.1.0"))]
    text = render_results_table(documents)
    header = text.splitlines()[2]
    assert header == "Harness version(s): 1.0.0, 1.1.0"


def test_render_results_table_of_an_empty_directory_has_no_rows() -> None:
    text = render_results_table([])
    assert _table_rows(text) == []
    assert "Harness version(s): (none published yet)" in text
    assert _HEADER_ROW in text


def test_render_results_table_footer_says_it_is_generated() -> None:
    text = render_results_table([])
    assert "results-table" in text.splitlines()[-1]


# ---------------------------------------------------------------------------
# `n/a` cells
# ---------------------------------------------------------------------------


def test_cost_is_n_a_when_the_site_disclosed_no_prices() -> None:
    document = _minimal_document(preset="p", engine="flink", variant="hash", date="2026-01-01", site_pricing=(0.0, 0.0))
    row = _table_rows(render_results_table([(Path("a"), document)]))[0]
    assert _cell(row, 9) == "n/a"


def test_cost_is_n_a_when_resource_cost_does_not_apply() -> None:
    document = _minimal_document(preset="p", engine="spark", variant="hash", date="2026-01-01")
    derived = document["derived"]
    assert isinstance(derived, dict)
    derived["cost"] = {"usd_per_hour": None, "run_hours": 0.5, "usd": None}
    row = _table_rows(render_results_table([(Path("a"), document)]))[0]
    assert _cell(row, 9) == "n/a"


def test_file_size_is_n_a_when_geometry_is_null() -> None:
    document = _minimal_document(preset="p", engine="flink", variant="hash", date="2026-01-01", geometry=None)
    row = _table_rows(render_results_table([(Path("a"), document)]))[0]
    assert _cell(row, 10) == "n/a"


def test_file_size_is_n_a_when_the_final_snapshot_was_never_measured() -> None:
    document = _minimal_document(
        preset="p", engine="flink", variant="hash", date="2026-01-01", geometry={"final": None}
    )
    row = _table_rows(render_results_table([(Path("a"), document)]))[0]
    assert _cell(row, 10) == "n/a"


def test_exactness_names_the_lost_and_duplicate_rows_when_not_exact() -> None:
    document = _minimal_document(preset="p", engine="flink", variant="hash", date="2026-01-01", exact=False)
    row = _table_rows(render_results_table([(Path("a"), document)]))[0]
    assert _cell(row, 7) == "lost 3 / dup 1"


# ---------------------------------------------------------------------------
# A document that cannot be rendered
# ---------------------------------------------------------------------------


def test_render_results_table_names_the_file_that_is_missing_a_field() -> None:
    broken = _minimal_document(preset="p", engine="flink", variant="hash", date="2026-01-01")
    del broken["derived"]
    with pytest.raises(ValueError, match="broken.json"):
        render_results_table([(Path("broken.json"), broken)])


@pytest.mark.parametrize("value", [None, [], {"schema_version": 2}])
def test_results_table_cli_reports_bad_shapes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], value: object
) -> None:
    from ingest_bench.collect.cli import results_table_main

    source = tmp_path / "broken.json"
    source.write_text(json.dumps(value))
    output = tmp_path / "RESULTS.md"
    assert results_table_main([str(tmp_path), "--out", str(output)]) == 1
    assert str(source) in capsys.readouterr().err
    assert not output.exists()


def test_result_parser_preserves_unconsumed_artifacts() -> None:
    from ingest_bench.collect.schema import parse_result

    document = _minimal_document(preset="p", engine="flink", variant="hash", date="2026-01-01")
    document["artifacts"] = {"future_measurement": [1, 2, 3]}
    assert parse_result(document) is document
