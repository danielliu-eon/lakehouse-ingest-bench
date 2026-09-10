# Publishing a result

Run `scripts/finish.sh <run_id> --publish results/` to write a redacted
`run.json` (`schema_version: 2`) under `results/<engine>/` and regenerate
`RESULTS.md`. Commit both together; do not edit them by hand.

Publishing requires `run_valid: true` unless you pass `--publish-invalid`.
Invalid runs remain labelled by validity state and must not be presented as
headline results. See the [result schema](../docs/results-format.md).

No results have been published yet. The empty table in `RESULTS.md` is expected.

## Automated checks

CI runs `scripts/validate-results.py` over this directory. It checks that:

- Each result parses and uses `schema_version: 2`.
- No unredacted `s3://` or `gs://` URIs remain, and no string contains a
  12-digit account ID.
- The corpus uses a shipped preset and `run.corpus_hash` matches its hash.
  Locally overridden presets are not accepted.
- `spec.producer.seconds` is absent or null, so the offer is not shortened.
- The fleet includes at least one role. Each role has a nonempty
  `machine_type` other than `unspecified` or a `YOUR_` placeholder, plus
  positive `vcpu` and `gib` values.
- Both rates in `run.site_pricing` are positive, allowing costs to be derived.
- The scorer summary, derived keep-up measures and `producer_bound` are present.
- No two results reuse a table or topic name.
- `RESULTS.md` matches a fresh render of the result documents.

To regenerate the table separately, run:

```bash
results-table results/ --out results/RESULTS.md
```

## Reviewer checks

The validator cannot establish every publication requirement. Reviewers must
also check that:

- Only valid runs are used as headline comparisons. `finish.sh` checks
  validity at publication, but the directory validator allows explicitly
  published invalid runs. A `producer_bound` run cannot establish engine
  capacity because the producer limited the offered rate.
- Tuning beyond the run spec's knobs has a separate variant name
  (`--variant <name>`).
- External results include `run.engine_versions` with `name`, `version` and
  `notes` supplied by the operator.
- Published files contain no identifying company, product, person, cluster,
  node-pool or taint names. Automated scans do not recognize every private name.
