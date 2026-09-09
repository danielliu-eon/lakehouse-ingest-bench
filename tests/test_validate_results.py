import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

from ingest_bench.collect.table import render_results_table
from tests.test_collect import _build, _run_dir, _site

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "validate-results.py"


def _run(results_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(SCRIPT), str(results_dir)], capture_output=True, text=True)


def _rerender(results: Path) -> None:
    """Regenerate `RESULTS.md` from whatever `.json` files `results` holds now.

    Called after every mutation but the one testing staleness itself, so each
    other test fails on exactly the one rule it is checking and not also on a
    table that no longer matches the document it mutated.
    """
    documents = [(path, json.loads(path.read_text())) for path in sorted(results.glob("**/*.json"))]
    (results / "RESULTS.md").write_text(render_results_table(documents))


def _valid_results_dir(tmp_path: Path) -> tuple[Path, Path]:
    """One publishable flink result, with a `RESULTS.md` that matches it."""
    results = tmp_path / "results"
    flink_dir = results / "flink"
    flink_dir.mkdir(parents=True)
    document = _build(_run_dir(tmp_path / "run"), _site(tmp_path / "run"))
    doc_path = flink_dir / "2026-09-20-flink-smoke-hash.json"
    doc_path.write_text(json.dumps(document))
    _rerender(results)
    return results, doc_path


def _mutated(tmp_path: Path, mutate: Callable[[dict[str, object]], None], *, rerender: bool = True) -> Path:
    results, doc_path = _valid_results_dir(tmp_path)
    document = json.loads(doc_path.read_text())
    mutate(document)
    doc_path.write_text(json.dumps(document))
    if rerender:
        _rerender(results)
    return results


def test_validate_results_passes_on_a_valid_document_and_a_fresh_table(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_results_passes_on_an_empty_directory(tmp_path: Path) -> None:
    results = tmp_path / "results"
    results.mkdir()
    (results / "RESULTS.md").write_text(render_results_table([]))
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_results_fails_on_a_twelve_digit_number(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["table"] = f"{run['table']}-123456789012"

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "account_id" in result.stdout


def test_validate_results_fails_on_a_non_placeholder_uri(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["table"] = f"{run['table']} s3://leaked-bucket/x"

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert ": uri:" in result.stdout


def test_validate_results_fails_when_the_producer_was_shortened(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        spec = run["spec"]
        assert isinstance(spec, dict)
        producer = spec["producer"]
        assert isinstance(producer, dict)
        producer["seconds"] = 300

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "producer.seconds" in result.stdout


def test_validate_results_fails_when_the_corpus_hash_does_not_match_the_shipped_preset(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["corpus_hash"] = "deadbeef"

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "corpus_hash" in result.stdout


def test_validate_results_fails_on_a_stale_results_md(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["variant"] = "sorted"

    result = _run(_mutated(tmp_path, mutate, rerender=False))
    assert result.returncode == 1
    assert "results_md" in result.stdout


def test_validate_results_fails_on_a_null_derived_keepup(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        derived = document["derived"]
        assert isinstance(derived, dict)
        derived["keepup"] = None

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "derived.keepup" in result.stdout
