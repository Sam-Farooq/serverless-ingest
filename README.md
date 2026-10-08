# serverless-ingest

An object lands in S3, a message goes to SQS, a Lambda validates it and loads
it into RDS Postgres. Three handlers, no long-running process, and Terraform
for the things that trigger them.

```
  upload ──► s3://<bucket>/raw/ ──► ingest-objects ──► load ──► Postgres
                                         (sqs)                    readings
                                           │                      rejected_rows
                 after 5 failed deliveries                        ingest_objects
                                           ▼
                                  ingest-objects-dlq
                                           │
                                           └──► replay, invoked by a person

  reconcile, daily: list the bucket, read the ledger, report the difference
```

## The handlers

| File | Triggered by | Returns |
|---|---|---|
| `src/handlers/load.py` | SQS event source mapping, up to 10 objects | `batchItemFailures` |
| `src/handlers/replay.py` | A person, with `{"max_messages": 50, "dry_run": true}` | counts |
| `src/handlers/reconcile.py` | A CloudWatch rule at 02:15 UTC | the unreconciled keys |

No handler imports another. Everything shared is in `src/ingest/`, so a
handler file holds the shape of its event and the decision about failure, and
nothing else.

## Partial batch failure is most of the design

One invocation receives ten messages. The event source mapping declares
`ReportBatchItemFailures`, so the return value decides what happens to all
ten: the ids in the list are redelivered, everything else is deleted.

Getting that wrong has two shapes, and both are expensive.

**Raising instead of returning fails all ten.** Nine objects that loaded
correctly come back round, get loaded again, and share a receive count with
the one object that cannot be read. Five deliveries later all ten are in the
dead letter queue and only one of them belonged there.

**Reporting a permanent failure is a loop.** A message whose body is not JSON,
or whose object has been deleted, fails the same way on every attempt. It is
counted, logged and dropped instead. `src/ingest/errors.py` holds the one
distinction the handlers need, and `src/ingest/s3source.py` decides which S3
error codes fall on which side of it: `NoSuchKey`, `NoSuchBucket` and
`AccessDenied` are permanent, and everything else including `SlowDown` is
worth another delivery.

A third shape is worth knowing because it is silent: an `itemIdentifier` that
is empty, or that was not one of the ids SQS sent, makes AWS treat the whole
batch as failed. `tests/test_partial_batch_failure.py` asserts the returned
ids are a subset of the ids received.

## The ledger row is also the lock

`ingest_objects` is keyed on `(bucket, object_key, etag)`, and the insert into
it is the first statement of the transaction that loads the object.

That one statement is doing two jobs. It is the idempotency check, because a
redelivery of an object that already committed gets `rowcount` 0 and returns
without touching `readings`. It is also the mutex: two invocations racing on
the same object both attempt the same primary key, and Postgres makes the
second wait on the first rather than letting both load.

The etag is in the key on purpose. An object overwritten in place is a second
ingest with different bytes, and a ledger keyed on the path alone would skip
it. The etag is used as a version token and never as a content hash, because
a multipart upload produces an etag that is not the MD5 of anything.

Rows are inserted one `execute` at a time rather than with `executemany` or
`COPY`. **What that gives up is throughput.** The reason is the count: with
`ON CONFLICT (event_id) DO NOTHING`, a per-row execute reports whether that
row was new, and the ledger and the metrics both want the exact number
inserted rather than the number offered. `COPY` into a temporary table
followed by an upsert is the faster shape and needs a second copy of the
schema to stage into, which is a second place for it to drift. At a few
hundred rows per object the round trips are affordable. At a few thousand this
is the first thing to change.

## A bad row must not be able to stop a good one

A message is one object and an object is many rows, so a single malformed row
cannot be allowed to fail the message. It would retry five times, reach the
dead letter queue, and take every valid row in that object with it.

So `parse_ndjson` returns two lists instead of raising. Four classes of line
go to the second one: unparseable JSON, JSON that is not an object, a schema
violation, and an `event_id` repeated inside the same object. They are written
to `rejected_rows` in the same transaction as the good rows, with the line
number and the first 2000 characters of the text, so the question after an
alarm has an answer in SQL rather than in a log that aged out.

`Reading` sets `extra="forbid"`. **What that gives up is tolerance:** a
producer that adds a field has all of its rows rejected rather than loaded
without the new column. The trade is deliberate. The rejected count has an
alarm on it at a threshold of zero, so a producer change surfaces within one
15 minute period, and the alternative is a renamed field silently absent from
a table nobody checks.

Ranges are physical rather than business limits. A humidity of 140 is a broken
sensor whatever the dashboard thinks.

## The cold start, and the connection that outlives it

Lambda keeps the execution environment alive between invocations, so anything
in a module global is paid for once. `src/ingest/clients.py` keeps the boto3
clients and the Postgres connection there.

The connection needs more care than the clients, because reusing it blindly is
wrong rather than just slow. RDS closes idle connections and a failover moves
the endpoint, so a warm environment can wake up holding a socket to nothing.
The cached connection is checked for `closed` before it is handed out, and a
handler that saw a `psycopg.Error` drops it rather than passing the same dead
socket to the next invocation. `reset_connection` swallows a failing `close`,
because the caller is already reporting a batch failure and does not need a
second exception from the cleanup.

The connection count is managed with `reserved_concurrent_executions = 20`,
since one warm environment holds one connection. **What that gives up is
elasticity:** a burst past 20 concurrent invocations is throttled rather than
served. RDS Proxy would lift the cap, and it is another component with its own
failure modes and its own line on the bill. At this concurrency the cap is the
cheaper answer, and it is one number to change if that stops being true.

## Six things that bite

**The first message on the queue is not an object.** S3 posts an
`s3:TestEvent` the moment a notification configuration is saved. It has no
`Records` key, so a handler that assumes one fails on it, retries to the
maximum, and parks its first ever message in the dead letter queue.

**The object key in the event is URL-encoded.** A space arrives as `+`, other
reserved characters as percent escapes. The encoded and real key are identical
for anything made of letters, digits, dashes and slashes, so this works until
somebody uploads a file with a space in the name. Then `GetObject` answers
`NoSuchKey`, which is classified permanent, so the object is discarded without
a retry. Commit `1e1a333` is that bug and the four key shapes that now cover
it.

**Only some of the S3 APIs quote the etag.** An event notification carries
`d41d8c…`. `ListObjectsV2` and `HeadObject` carry `"d41d8c…"`, quotes
included. The ledger is written from the first and read against the second by
`reconcile`, so without `normalize_etag` every object in the bucket reports as
unreconciled, every run, forever.

**The visibility timeout is derived, not typed.** `infra/queues.tf` sets it to
`var.function_timeout * 6`, which is 360 seconds at the default. A queue whose
messages become visible while an invocation is still working hands the same
object to a second invocation. The ledger makes that harmless and it is still
twice the work, which is what the `objects_skipped` alarm watches for.

**Lambda creates its own log group if Terraform does not**, with retention set
to never expire. `aws_cloudwatch_log_group` is declared for all three
functions, and the functions `depends_on` it, because the other order creates
the function, lets it write first, and leaves the retention applying to
nothing.

**A metric that has never been published does not exist.** The three alarms on
the function's own metrics would sit in `INSUFFICIENT_DATA` on a fresh
install, so every alarm sets `treat_missing_data = "notBreaching"`.

## What it does not do

**It does not re-enqueue a gap it finds.** `reconcile` reports and stops. The
loads are idempotent so re-sending would be safe, and the cause is almost
always a configuration change rather than a lost message: a notification
edited by hand, a queue policy that stopped allowing `s3.amazonaws.com`, a
dead letter queue nobody drained. A handler that quietly repairs the symptom
removes the only evidence the cause exists. `replay` is the lever, and a
person pulls it.

**It does not preserve order.** The queue is standard, not FIFO, because the
upserts are idempotent and the rows carry their own timestamps. On a FIFO
queue the partial failure contract changes: a failed message means every later
message in its group must also be reported, and none of this code does that.

**It does not publish metrics through an API call.** They are embedded metric
format log lines, so the function makes no `PutMetricData` request at all.
**What that gives up is immediacy:** the metric exists once the log line is
parsed rather than at the moment it is counted. The role has no
`cloudwatch:PutMetricData` in it, by way of proof.

## Tests

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests
```

No AWS account, no network, no Docker. moto stubs S3 and SQS. The Postgres
statements are the real statements, run against sqlite through
`tests/pgshim.py`, which rewrites `%s` to `?` and translates three types:
`BIGSERIAL PRIMARY KEY`, `TIMESTAMPTZ` and `NUMERIC(p,s)`.

**What that gives up is dialect fidelity.** sqlite says nothing about NUMERIC
precision, timezone handling, or the speculative insertion lock that makes the
ledger claim a mutex under real concurrency. What it does check is that the
statements in `ingest/repository.py` are the ones the tests execute, so a
wrong column name fails a test rather than a deployment.

The translation is derived from `sql/001_schema.sql` rather than copied from
it, and `tests/test_schema_translation.py` compares every declared column
against `PRAGMA table_info`. A column added to Postgres in a type the
translation does not know fails there instead of silently leaving the test
schema behind.

Terraform is checked separately:

```bash
cd infra
terraform fmt -check -recursive
terraform init -backend=false
terraform validate
```

`-backend=false` means no state bucket and no lock, so it needs no
credentials.

## Deploying

```bash
./scripts/package.sh
cd infra
cp terraform.tfvars.example terraform.tfvars   # then fill in the three values
terraform plan
```

Three variables have no default, because none of them has a safe one:
`database_url`, `subnet_ids` and `security_group_ids`. The bucket name carries
the account id as well, and that is read from `aws_caller_identity` rather than
supplied by hand. `plan` needs AWS credentials; `validate` above does not.

`package.sh` fetches wheels for `manylinux2014_aarch64` rather than for the
machine running it. psycopg and pydantic-core are compiled, so a zip built
from a developer's own site-packages installs a macOS wheel into an arm64
Linux runtime and fails at import with an error that names neither.

The result is 23 MiB zipped and 42 MiB of unzipped content, against a 250 MB
unzipped limit. `package.sh` prints both figures at the end of a build, summing
file sizes rather than calling `du`. The difference matters here: the tree is
mostly thousands of small botocore JSON files, so a 4 KiB block count reads
48M under `du -sh` and 49 under `du -sm` for the same 42 MiB of content, and
the limit counts content.

botocore is 19 MiB of the 42, and the Python 3.11 runtime already provides it.
**What bundling it gives up is 19 MiB of package size,** in exchange for the
version being pinned by the zip rather than changed underneath the function by
a runtime update.

The runtime dependencies are floors rather than pins, so a build next month
resolves newer wheels and these numbers move. The script's own output is the
current answer; the ones above are what it printed when this was written.

The script deletes nothing except `__pycache__`. Stripping the `dist-info`
directories saves 0.7 MiB of the 42 and it breaks any dependency that reads its
own version through `importlib.metadata`, which fails nowhere except in the
runtime.

`database_url` is marked `sensitive` and has no default. It comes from Secrets
Manager; a `.tfvars` file holding it is gitignored and is not how it should be
supplied for anything that matters.
