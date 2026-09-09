# Publishing a result

A result is one `run.json` (`schema_version: 2`) under `results/<engine>/`,
plus `RESULTS.md` regenerated from every file here. Both arrive together, from
`scripts/finish.sh <run_id> --publish results/` — never by hand-editing either.
`finish.sh` refuses to publish a run whose `run_valid` is false unless you pass
`--publish-invalid`, which keeps the run but never as a headline.

The document's own schema is in
[`../docs/results-format.md`](../docs/results-format.md).

## Checked by `scripts/validate-results.py`

CI runs this over the whole directory, so these fail a pull request:

- **`schema_version` is 2**, and the file parses.
- **No leaked site.** No unredacted `s3://` or `gs://` URI anywhere in the file,
  and no 12-digit number inside any string — the shape an account id takes in a
  bucket name, an ARN or a registry host.
- **The corpus is a shipped preset**, and `run.corpus_hash` matches that
  preset's own hash. So a corpus generated from an overridden preset, or from a
  shape that lives only on one machine, is refused.
- **`spec.producer.seconds` is unset.** A shortened offer is a probe, not a
  result.
- **The fleet is disclosed**: at least one role, each with a `machine_type` and a
  positive `vcpu` and `gib`. And `run.site_pricing` carries both rates, so the
  cost column can be re-derived.
- **The scorer's summary and the derived keep-up and `producer_bound` are
  present**, not null.
- **`RESULTS.md` is a fresh render** of the documents beside it. Re-render with
  `results-table results/ --out results/RESULTS.md` in the same change.

## A reviewer's responsibility

The checker does not see these. They are what a reader of `RESULTS.md` is
entitled to assume, and a result that breaks one is misleading rather than
invalid:

- Every run measured a **fresh table and topic**. Reusing either lets one run
  measure the one before it.
- **`run_valid: true`** — `finish.sh` gates this at publish time, but the checker
  does not, so a `--publish-invalid` result must show its validity state
  (`producer_bound`, `void`, `undersized`, `not drained`) and never appear as a
  headline number. A `producer_bound` run in particular says nothing about the
  engine: the offer, not the engine, set the rate.
- Any engine tuning beyond the run spec's own knobs is a **separately named
  variant** (`--variant <name>`), not a silent re-run of the same file.
- An external result carries `run.engine_versions` as `{name, version, notes}` —
  what its operator said, since the harness never ran it.
- No company, product, person, cluster, node-pool or taint name anywhere in a
  published file. The leak scans above catch account ids and buckets; a name is
  a judgement.
