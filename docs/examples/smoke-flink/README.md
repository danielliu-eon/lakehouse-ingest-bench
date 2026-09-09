# A recorded smoke run

`scripts/smoke.sh` — the full 300 s, 5 MB/s corpus against stock Flink — on
2026-09-09, on an Apple M5 Pro under OrbStack with the engine image emulated
under amd64. Verdict `run_valid: true`: drained at batch 299, all 5,840,896
offered rows exactly once, freshness p95 17.6 s inside the spec's 60 s bound,
96.9% of the offer absorbed when the last batch was acked. `summary.json` and
`freshness.json` are the scorer's unedited artifacts. Nothing measured locally
is a result — the repository's `README.md` says why.
