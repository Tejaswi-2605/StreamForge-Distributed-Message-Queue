# StreamForge verified results

Completion work executed on **5â€“6 October 2026**, Asia/Calcutta. **The educational portfolio deliverable is complete and verified: native and Docker builds/services, tests/race checks, recovery, retention, CLI behavior, workload coverage, authentic replay and actual Prometheus scraping passed.** Historical initial measurements below remain explicitly separate.

## Final source verification

All seven Windows binaries rebuilt successfully. `go vet ./...`, `go mod verify`, and Linux/amd64 cross-compilation (` CGO_ENABLED=0 go build -buildvcs=false ./...`) passed. The cross-build verifies compilation, not Linux or container execution.

| Check | Passing leaf cases | Failures / skips | Integration cases | Integration seconds |
|---|---:|---:|---:|---:|
| Normal `go test -json -count=1 ./...` | 50 | 0 / 0 | 17 | 30.862 |
| `go test -race -json -count=1 ./...` | 50 | 0 / 0 | 17 | 93.017 |

There are 45 top-level names. Counting the six corruption children instead of their parent yields 50 unique leaf cases; normal and race executions are two runs of those cases, not 100 unique tests. Integration was enabled and used real PostgreSQL/Redis. Race verification completed without reported races using CGO/GCC in a temporary path without spaces, cleaned up afterward. Public scripts/verify.ps1 now reproduces this optional local compiler extraction and fails on any unavailable/failed race check.

Added completion coverage verifies automatic retention keeps incomplete retry sources, later removes inactive segments, reports expired offsets and preserves next offsets across restart; a real broker is forcibly killed after 30 acknowledged writes and restarted to verify all payloads, commits and offset continuity; separate producer/consumer executables exercise keyed/explicit routing, group resume, earliest/latest reads, failure scheduling and malformed/overflowing flags. A damaged retry job does not prevent another job in the same selected pass from dispatching; it remains incomplete and its error is reported. Unit checks cover retention configuration, pinned segments, replay digest sensitivity and Windows benchmark clock resolution. Existing four-bug, maximum 16 MiB, stress, lease and retry/DLQ regressions also pass.

Current evidence: `artifacts/tests.jsonl`, `artifacts/race.jsonl`, `artifacts/completion-verification.json`. The earlier 42-case correctness follow-up remains in `artifacts/fixes-tests.jsonl`, `artifacts/fixes-race.jsonl` and `artifacts/fixes-verification.json` (normal integration 23.478 s, race 31.005 s). No hosted CI execution is claimed.

## Completion workload matrix

Run ID: `20261005T183737-9542248f`. Eight actual local trials; one measured run per workload. This is coverage of the requested message/payload/partition/broker dimensions, not every Cartesian combination. Each trial publishes sequentially after 100 untimed warmups and then fetches/validates all measured plus warmup records. End-to-end rate divides measured messages by publish-plus-fetch seconds. Write mode acknowledges the OS cache; fsync synchronizes before acknowledgment, so the two modes have different durability contracts. API broker configuration was set by the workflow rather than inferred from the benchmark flag.

Windows uses [QueryPerformanceCounter](https://learn.microsoft.com/en-us/windows/win32/sysinfo/acquiring-high-resolution-time-stamps) for short-operation timing. An earlier completion attempt produced zero raw percentiles with the coarser clock; it remains diagnostic evidence in `artifacts/benchmarks/20261005T161840-f8c2e96f`, and the table below comes from the corrected rerun. Host/background/cache conditions were not isolated. Payload sizes and durability differ across trials, so these rows do not isolate broker-count scaling or API overhead.

| Trial | Mode | Messages | Payload B | Partitions / brokers | Durability | Publish msg/s | Fetch records/s | Combined msg/s | Mean ms | p50 / p95 / p99 ms | Log bytes |
|---|---|---:|---:|---:|---|---:|---:|---:|---:|---|---:|
| raw-small | raw | 10,000 | 100 | 1 / 0 | write | 246361.85 | 170479.10 | 100164.77 | 0.003962 | 0.0030 / 0.0065 / 0.0109 | 1,412,770 |
| raw-medium | raw | 100,000 | 1,024 | 3 / 0 | write | 196175.56 | 149335.07 | 84742.01 | 0.005011 | 0.0036 / 0.0077 / 0.0406 | 106,789,290 |
| raw-large | raw | 500,000 | 100 | 10 / 0 | write | 295267.28 | 176542.52 | 110469.74 | 0.003308 | 0.0030 / 0.0040 / 0.0074 | 71,247,860 |
| raw-fsync | raw | 10,000 | 10,240 | 1 / 0 | fsync | 1168.72 | 51947.81 | 1142.75 | 0.855302 | 0.8128 / 1.0828 / 1.5955 | 103,836,870 |
| api-single | api | 10,000 | 100 | 1 / 1 | fsync | 722.30 | 75261.81 | 715.36 | 1.384077 | 1.2890 / 1.8789 / 2.2726 | 2,019,870 |
| api-medium | api | 100,000 | 1,024 | 3 / 3 | write | 1939.31 | 41504.04 | 1852.66 | 0.515327 | 0.4810 / 0.7659 / 0.9527 | 112,796,522 |
| api-large | api | 500,000 | 100 | 10 / 3 | write | 1482.95 | 52583.09 | 1442.27 | 0.673807 | 0.6075 / 1.1174 / 1.5099 | 101,255,140 |
| api-fsync | api | 10,000 | 10,240 | 3 / 3 | fsync | 480.96 | 11328.08 | 461.18 | 2.078521 | 1.9983 / 2.7608 / 3.3201 | 104,457,174 |

Log bytes include warmups, framing and broker metadata. API sizes were rechecked per topic after broker shutdown because Windows directory listings initially gave stale lengths for open files. `log-size-verification.json` preserves the correction; the workflow now queries actual EOF through shared handles and rejects a count below published payload bytes. Timings were unaffected. Exact seconds, payload bytes, percentiles and other fields are in [benchmark-results.json](docs/benchmark-results.json). Reproduce with scripts/benchmark.ps1 as documented in README. Local raw logs used temporary storage; API logs used this Windows/OneDrive workspace.

## Completion authentic consumer-group replay

The entire authentic GH Archive hour was replayed again into an empty three-partition topic on three native brokers with fsync. Parsed/published/consumed: **11,351 / 11,351 / 11,351**, rejected **0**, reduced-envelope bytes **1,314,477**. Ingestion **28.6986988 s / 395.5232 events/s**; group readback/commit verification **0.8682191 s / 13073.8888 records/s**. SHA-256 offset/key/payload digests matched per partition; group membership/heartbeat, committed next offsets, empty committed resume and **committed lag 0** were verified. Verification state is O(partitions), replacing the original O(events) map.

The exact archive/source/hash/privacy notes and original replay are retained in [data/PROVENANCE.json](data/PROVENANCE.json), with the stronger completion replay recorded separately. Repository names can identify users; reduced fields are not anonymization. Evidence is replay.json under the run directory above. Broker processes started by the workflow were stopped afterward; its isolated SQL schemas were dropped and ignored log files retained.

## Verified Docker deployment

Executed **2026-10-06T00:52:37.1398984+05:30** using Docker Desktop **4.94.0.241994**, Docker client/engine **29.8.2**, Compose **5.5.1**, Linux/amd64 containers and WSL 2.7.13.0. No account login was needed for the public images.

- Compose configuration validation and the supplied multi-stage image build passed; all seven Linux executables were built inside Docker.
- PostgreSQL 17.6, Redis 8.2.1, three broker containers and Prometheus 3.5.0 started; `compose up --wait` passed. Broker health endpoints each returned HTTP 200.
- The tools-container demo published/fetched six records, checked stable keyed routing, consumer-group rebalance/committed resume, three retries and DLQ attempt 4 with original provenance.
- Prometheus `/api/v1/targets` showed all three broker targets `up`, with empty scrape errors. This is actual collector execution, not only endpoint text retrieval.
- The unique verification project `streamforge-check-0113dfde1944` and its temporary volumes were removed. A subsequent check found zero remaining test containers/volumes; the generated credential file was removed. Preexisting native services were left available. Start your own persistent stack using README when you want to run the project.

Evidence: `artifacts/docker/streamforge-check-0113dfde1944/verification.json`, `prometheus-targets.json`, `services.jsonl`, `services.log`, and `artifacts/docker/environment.json`. Compact portable verification facts are in [verification-results.json](docs/verification-results.json). Reproduce with scripts/verify-docker.ps1.

## Environment

| Component | Observed version/environment |
|---|---|
| OS | Microsoft Windows NT 10.0.26200.0, amd64 |
| CPU | 13th Gen Intel Core i5-1334U |
| Go | 1.27.1 |
| PostgreSQL | 17.6, real local Windows server |
| Redis | 8.10.2 community Windows build, real local server |
| Protobuf generation | protoc 29.3; Go plugin 1.36.8; gRPC plugin 1.5.1 |
| Race compiler | GCC 16.2.0 / MinGW-w64 UCRT 14.0.0 |
| Docker | Desktop 4.94.0.241994; client/engine 29.8.2; Compose 5.5.1; Linux containers verified |

The Redis version actually tested differs from the Compose image's configured 8.2.1. API broker logs were under this Windows/OneDrive workspace; the raw benchmark used an operating-system temporary directory. Hardware/cache/background-load effects were not isolated; no repeat-run distribution or production capacity claim is provided.

## Initial audit: build, tests and boundaries

| Check | Verified outcome |
|---|---|
| Seven CLI binaries and `go build -buildvcs=false ./...` | Passed |
| `go vet ./...` and `go mod verify` | Passed |
| Tests with `STREAMFORGE_INTEGRATION=1` | 34 leaf cases passed; 0 failed; 0 skipped |
| Real-database integration | All nine integration tests passed |
| `go test -race -json -count=1 ./...` | Passed all 34 leaf cases with integration enabled; no reported races |
| API regeneration | Both included generated Go files matched fresh generation byte for byte |
| Source scope | 21 project packages inside this root; no sibling/nested project source |
| Separate-process demo | Three broker processes healthy; publication/readback, group resume, retry and DLQ assertions passed |
| Metrics endpoints | Prometheus text retrieved from all three brokers |

There are 29 named top-level tests. The corruption test has six child cases; counting those children instead of their parent yields 34 leaf cases: 25 non-integration and nine integration. The latest file-review test run completed the integration package in 21.995 seconds. The successful race run's integration package took 46.087 seconds.

Integration coverage includes clean restart recovery of 30 messages across three partitions, fixed ownership/wrong-owner routing, group rebalance and committed resume, actual Redis lease expiry, simulated lease loss, retry/DLQ scheduling and restart, API validation/batches, and concurrent publication/readback. The lease-loss test deletes Redis keys; it does **not** restart the Redis server. The concurrency test publishes 900 messages and performs 75 fetches, checking unique offsets. Deterministic stress uses seed 73461 and 2,500 mixed operations, recovering 1,436 messages and checking ordering/payload/commit/membership invariants.

The separate-process demo published six source messages, demonstrated consistent keyed routing and group resume, and delivered three retries followed by DLQ attempt 4 with original provenance. The audit stopped its three broker processes afterward. Its metrics checks did not include an actual Prometheus scrape.

Race setup required `CGO_ENABLED=1` and GCC extracted to a temporary path without spaces. Earlier setup failures were not detected application races. That temporary extraction was removed afterward. At the initial audit the wrapper only warned on race failure; this has now been changed to fail. No GitHub Actions execution is claimed.

## Historical initial CLI benchmark observations

Each row is **one run**, with 10,000 measured messages and 100 untimed warmup messages. Publication is sequential; fetch follows publication and includes all 10,100 records, including warmup. Payload preparation is outside publication timing. Fetch validation is inside fetch timing. Combined throughput uses the 10,000 measured messages divided by publish-plus-fetch elapsed time; it is not concurrent streaming throughput.

| Measurement | Raw log | gRPC API |
|---|---:|---:|
| Payload bytes/message | 1,024 | 100 |
| Partitions / participating brokers | 1 / 0 | 3 / 3 |
| Durability | fsync | fsync |
| Publish seconds | 11.0366811 | 16.1559598 |
| Fetch seconds | 0.0950597 | 0.1321948 |
| Combined seconds | 11.1317408 | 16.2881546 |
| Publish messages/s | 906.07 | 618.97 |
| Fetch records/s, including warmup | 106,249.02 | 76,402.40 |
| Combined measured messages/s | 898.33 | 613.94 |
| Average publish latency, ms | 1.103376 | 1.615596 |
| p50 / p95 / p99, ms | 1.1505 / 1.7407 / 2.8910 | 1.6098 / 2.3101 / 2.7565 |
| Measured payload bytes | 10,240,000 | 1,000,000 |
| Observed raw log bytes | 10,755,270, including warmup/framing | Not separately measured |

The different payload sizes make these unsuitable for isolating API overhead. API brokers were explicitly configured for fsync; the benchmark flag alone does not verify server settings. Commands and prerequisites are in [README.md](README.md).

## Go append microbenchmarks

Command: `go test -run '^$' -bench BenchmarkAppend -benchtime=500x -benchmem ./internal/logstore`. One run per case, 500 timed operations. Payload MB/s is Go's reported rate; it is distinct from records/s.

| Payload | Mode | ns/op | MB/s | B/op | allocs/op |
|---|---|---:|---:|---:|---:|
| 100 B | write | 19,210 | 5.21 | 528 | 4 |
| 100 B | fsync | 1,161,069 | 0.09 | 528 | 4 |
| 1,024 B | write | 25,203 | 40.63 | 3,492 | 4 |
| 1,024 B | fsync | 1,154,049 | 0.89 | 3,493 | 4 |
| 10,240 B | write | 34,822 | 294.07 | 32,189 | 4 |
| 10,240 B | fsync | 971,984 | 10.54 | 32,183 | 4 |

## Historical initial event replay

Source: [GH Archive](https://www.gharchive.org/), exact [2015-01-01-15.json.gz archive](https://data.gharchive.org/2015-01-01-15.json.gz). Acquired on 2 October 2026 and revalidated/replayed on 5 October 2026. The entire hour contains 11,351 valid JSON events from `15:00:00Z` through `15:59:59Z`.

| Observation | Verified value |
|---|---:|
| Downloaded gzip bytes | 3,844,072 |
| Parsed / published / consumed | 11,351 / 11,351 / 11,351 |
| Rejected events | 0 |
| Published reduced-envelope bytes | 1,314,477 |
| Ingest seconds / events per second | 32.9850065 / 344.1260501 |
| Readback seconds / records per second | 0.2580662 / 43,984.8380 |

Archive SHA-256: `dc50a9e7cf6fd56cfbb7fa11381a6bcf3657a1e279131b66888ebbfb58652e76`.

Replay used an empty three-partition topic and reduced envelopes containing event ID/type/time and repository name. Readback verified offsets and counts, not full payload digest equality. It fetched partitions directly rather than using consumer groups. Removing actor/full-event fields minimizes data; repository names can still identify users. See [provenance](data/PROVENANCE.json).

## Deployment state and remaining limits

WSL **2.7.13.0**, kernel **6.18.33.2-2**, installed successfully from Microsoft's hash/signature-verified standalone MSI after the winget attempt timed out. WSL feature setup and Docker engine startup succeeded without an automatic restart. Initial Docker verification exposed credential-helper PATH discovery and an overstrict project-name cleanup check; both were corrected before the successful complete rerun. The setup and verification scripts now handle the per-user Docker path explicitly. There is no remaining required implementation/deployment gate for the specified educational scope.

Power-loss durability, replication, consensus/election, automatic failover, TLS/authentication/authorization, live ownership reassignment, continuous application consumer runtime and exactly-once effects are not implemented or established. They are documented production boundaries rather than claims. Retention is opt-in and does not preserve slow-group offsets. Thirty-two permanently broken oldest retry jobs can still fill a whole selection; no unbounded fairness guarantee is claimed. Topic deletion was conditional and remains absent.

Older evidence remains ignored under artifacts: audit-race-complete.jsonl, file-audit-tests.jsonl, audit-demo.txt, audit-benchmark-raw.json, audit-benchmark-api.json, audit-go-benchmarks.txt, audit-replay.json, audit-probes.json, package inventories, logs and metric snapshots. Windows PowerShell test logs are UTF-16; newer JSON observations can be UTF-8. The tracked benchmark summary and provenance provide compact observations, not a hosted immutable evidence bundle or independent reproduction.
