# An engine config for the external walk-through

Three files an operator would hand their own Flink cluster to make it consume a
staged run: the SQL, the settings it is submitted with, and the cluster's shape.
`docs/adding-an-engine.md` walks through filling in the `@PLACEHOLDER@` tokens
and starting the job.

They are here so that the walk-through can be followed end to end without first
having to write a Flink job. They are not a template to build a managed engine
from — `engines/flink/` is that, and it renders these same three files per run
from a spec's knobs. This copy is that renderer's output for the local stack
with the run's names left as placeholders, which is also how a unit test keeps
it from drifting away from the corpus schema.

If your engine is not Flink, take this directory as the *shape* of what you
have to produce: a source declaring every corpus column, a catalog pointed at
the one the harness names, and an append-only insert. The columns and their
types come from `corpus.json`; everything else comes from `facts.json`.

| Token | Fill from |
|---|---|
| `@RUN_ID@` | `run_id` — also a fine consumer-group id, and the job's name |
| `@TOPIC@` | `topic` |
| `@NAMESPACE@` / `@TABLE@` | the two halves of `table`, split on the dot |

The catalog block carries the local stack's MinIO credentials verbatim, which
is why this is a config file and not a publishable record. `facts.json` is the
publishable record, and it redacts them.
