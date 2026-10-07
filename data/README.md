# Replay dataset

The verified demonstration used the complete public GH Archive hour [2015-01-01-15.json.gz](https://data.gharchive.org/2015-01-01-15.json.gz), obtained from [GH Archive](https://www.gharchive.org/). The source stores GitHub event records as gzip-compressed newline-delimited JSON. Source details, hash and observed counts are recorded in [PROVENANCE.json](PROVENANCE.json); execution measurements are in [RESULTS.md](../RESULTS.md).

Local file: `data/raw/2015-01-01-15.json.gz`, 3,844,072 bytes. Acquired on 2 October 2026, initially audited/replayed on 5 October and replayed with payload/group verification on 6 October 2026. SHA-256: `dc50a9e7cf6fd56cfbb7fa11381a6bcf3657a1e279131b66888ebbfb58652e76`. The archive contains 11,351 valid event lines for 15:00 through 15:59:59 UTC. No synthetic events were substituted.

To obtain your own copy from the project root:

```powershell
New-Item -ItemType Directory -Force data/raw | Out-Null
Invoke-WebRequest -Uri https://data.gharchive.org/2015-01-01-15.json.gz -OutFile data/raw/2015-01-01-15.json.gz
Get-FileHash data/raw/2015-01-01-15.json.gz -Algorithm SHA256
```

Compare the hash before replay. The raw archive is ignored by Git and Docker image builds; the optional Compose tools service mounts it read-only. Do not assume it is included in a fresh checkout.

Replay emits only event ID, type, creation time and repository name, routing by repository name. Actors and full event payloads are omitted. Repository names can contain usernames: this is minimization, not anonymization. Public accessibility is not a blanket license for all underlying event content; no uniform content license is asserted here. Avoid redistributing raw data unnecessarily.

Use an empty topic and follow [the replay commands](../README.md#tests-and-measurements). `--limit 0` processes the entire file. Parsing is bounded per line; verification retains one SHA-256 state and expected count per partition, so auxiliary memory is O(partitions). It hashes length-delimited offset, key and payload for both publication and readback, checks ordered contiguous offsets, joins an exclusive consumer group, heartbeats while fetching, commits each batch and confirms committed resume returns no records. Use a fresh group (the default is unique); this verifier intentionally rejects nonempty topics or shared group ownership.

The original audit replay in provenance remains historical count/offset evidence. The completion replay records the stronger payload and consumer-group verification separately.

`data/brokers/` holds local logs and `data/local/` local database state. Both are ignored runtime storage belonging to this project.
