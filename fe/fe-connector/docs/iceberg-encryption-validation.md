# Native Iceberg encrypted-read validation

This record covers the default-off HMS/v3/native-Parquet implementation and its
cache/lifetime and private-RPC prerequisites. It records bounded local evidence;
maintainer, QA, security and performance acceptance remain pending. Enabling the
deployment switch still requires the [read contract](iceberg-encryption-read.md)
and [private RPC configuration](../../../docs/private-rpc-tls.md).

## Review-fix verification

The review-fix candidate has a fresh FE package, 234 passing native ASAN tests
in 14 suites, 175 passing FE tests across 25 selected classes and 3,242
reactor tests with zero failures/errors (including 2,394 connector
cache/SPI/Iceberg tests, with 11 existing skips). FE Checkstyle, clang-format 16,
header hygiene and scoped clang-tidy pass.

The refreshed FE run also covers plugin API loading, upstream path-partition
keys, file formats and backend selection. It validates the combined connector
SPI 35.0 after upstream 34.0, preserving both interface surfaces. Native static
analysis checks every affected file against the PR's changed lines after the
build has completed; independent files run in four workers.

Real SDK tests reject all native write operations on encrypted targets, including
concurrent encryption changes at transaction refresh/final CAS, and preserve
plaintext writes and existing OCC checks. SQL-cache tests reject encrypted
eligibility before key unwrap and evict an already cached entry if the provider
later refuses it. A serialized heartbeat-package sequence proves immediate
capability revocation without shortening the configured leader/follower liveness
grace, followed by recovery.

The opt-in `test_iceberg_encryption_native_read_guards` suite passes output
generation and normal replay before the compatibility rebase, followed by a
normal replay on the rebased candidate against the same eight generated result
blocks. With both SQL cache settings enabled, a plaintext control reaches
`PhysicalSqlCache` while encrypted COUNT/SUM remain ineligible. Same-endpoint mock
KMS denial makes both
encrypted queries fail twice; plaintext cache hits remain valid. Permission
restoration recovers encrypted reads. Encrypted INSERT/OVERWRITE are rejected
without changing the four existing rows, and plaintext INSERT adds the fifth row.

The new SQL checks use the fresh FE package with the earlier D10 RELEASE BE
package. They validate the FE admission/cache fixes, not a fresh native RELEASE
artifact or ABI audit. Current native changes are separately covered by the
fresh ASAN build. The earlier broad SQL, compatibility, lifecycle and cost runs
below have not been repeated for the latest statement-reader changes; they must
not be interpreted as current-head acceptance.

## Earlier functional and failure coverage

The following records describe the earlier integration snapshot, before the
current review fixes and statement-owned reader integration.

- The complete RELEASE FE/BE build passes with zero FE Checkstyle violations.
  That snapshot's ASAN native subset runs 181 tests in 13 suites. FE capability,
  heartbeat, dispatch, split and provider coverage runs 125 tests; connector
  cache/SPI/Iceberg suites run 2,349 tests with zero failures/errors and 11
  existing skips (3,197 tests including reactor dependencies). Native formatting,
  header hygiene and scoped clang-tidy pass.
- Two enabled SQL matrix runs exercise eight independent data/position/equality
  key combinations, cache and batch settings, COUNT pushdown and pruning
  settings, projection/filter/aggregate/join, standalone COUNT, historical
  snapshots and native position deletes. SDK generic reads and Spark SQL
  establish the reference results. V3 planning remains synchronous; these runs
  do not establish encrypted V3 lazy SQL execution.
- Both default-off SQL suites pass and preserve `READ_DISABLED` admission.
  Undeclared file keys cannot bypass the read contract through raw SDK COUNT
  probes or a warmed manifest byte cache.
- Same-endpoint mock KMS Decrypt denial fails warmed SELECT and COUNT;
  restoration permits the original queries. This proves SDK error propagation,
  not production AWS IAM policy evaluation. Five physical object-read
  failure/recovery pairs also preserve subsequent successful reads.
- Forty concurrent reads, ten cancellations, five timeouts and five
  query-memory refusals preserve normal reads afterward. Tests use bounded
  worker pools so host CPU discovery cannot multiply unrelated test threads.
- Real authenticated fragment and split RPCs preserve distinct binary data,
  position-delete and equality-delete payloads, including NUL/non-UTF-8 AAD.
  Trust/hostname/plaintext rejection, reconnects, cache acquisition failures and
  restored configuration are covered without logging provider parser inputs.

The permanent enabled suite is
`test_iceberg_encryption_native_read_integration`. Its explicit deployment and
fixture setup are documented in the
[reference fixture guide](../../../docker/thirdparties/docker-compose/iceberg/encryption/README.md).
Run it through the repository regression entry point:

```sh
bash run-regression-test.sh --run -d external_table_p0/iceberg \
    -s test_iceberg_encryption_native_read_integration
```

The separate `test_iceberg_encryption_delete_reader` and
`test_iceberg_encrypted_hms_metadata` suites exercise the default-off deployment.
Required services fail an enabled suite rather than silently skipping coverage.

## Earlier lifecycle and diagnostic checks

Across the bounded stability phase, BE threads and file descriptors remain 590
and 385. BE RSS changes from 7,474,960 to 7,444,976 KiB. After explicit GC and
finalization, the FE live-object census grows by about 2 MiB and contains no live
`StandardEncryptionManager`, `IcebergFileEncryption` or `TFileEncryptionInfo`
instances. Profile capture runs separately so bounded profile-history retention
is not mistaken for reader ownership. These observations do not prove the
absence of leaks under every workload.

An exact-pattern audit of 19 actual fixture keys in 111 raw/base64/hex/array
encodings finds no matches in 500 rendered profiles or the examined runtime
logs, images and file-cache contents (580 files, 364,963,279 bytes). No keys or
credential values are part of this report. This does not prove arbitrary
transformed-key absence, deterministic JVM zeroization or protected crash dumps.

## Earlier runtime compatibility

A pre-capability BE participates in a plaintext aggregate alongside the new BE;
encrypted planning is refused before descriptors leave FE. An old FE with the
new BE preserves plaintext aggregate and V2 batch/delete results and refuses
encrypted execution. Tests cover dispatch-time capability loss, lease expiry,
failed/missing heartbeats, FE restart/image replay and old split callers. These
are bounded local interoperability checks, not production multi-FE rolling
upgrade certification. TLS activation requires coordinated configuration;
plaintext/TLS rolling activation is outside the transport contract.

## Cost protocol and retained failures

The measured source commits are `aa6e28438785` (baseline, including the D9/TLS
prerequisites) and `3c3baf9f91d2` (candidate). Both binaries use matching RELEASE
settings, the same fixed CPU affinity and bounded pools. The host is shared,
with no exclusive CPU reservation. Each
process receives exactly 1,000 scan/filter/count rounds (3,000 checked queries)
before two measurement passes. Each condition has five warmups and twenty timed
queries per pass; the gate uses the median of all forty samples. The predeclared
plaintext median regression limit remains 5%. Ciphertext cost runs separately
after the plaintext gate, without interleaving ciphertext into that comparison.

The fixtures contain the same 1,048,576 integer rows in four Parquet files, use
Zstandard compression and 64 KiB row groups, and verify count/sum with the SDK.
The workloads are `SUM(id)`, the high-selectivity predicate
`id IN (1,262144,262145,1048576)` with ordering, and standalone `COUNT(*)`.
Planning-cold creates a catalog per query and disables FE table caching and
Parquet page caching; warm reuses a catalog with those caches enabled. SQL and
file caches are disabled in both. OS and object-store caches are not evicted.

Earlier runs are retained as failed protocols, not removed from the record:

| Earlier protocol | Plaintext threshold failures |
| --- | --- |
| Uncontrolled placement, short preconditioning and mixed workload order | Cold filter +6.20%; warm filter +11.62% |
| Fixed affinity, plaintext-only order, 150-query preconditioning | Cold scan +6.93%; cold filter +6.25%; cold count +7.14%; warm scan +5.81%; warm count +7.80% |

Within-binary query times continued falling across those runs. The fixed long
preconditioning protocol was declared before either new measurement, without
latency-based early stopping. JVM compilation counters rose from 12,273 to
36,786 for the baseline and from 10,930 to 37,390 for the candidate. Fixed
300-query blocks were recorded throughout. This supports ongoing warmup as a
confounder, but does not exclude shared-host noise or establish a product speedup. An initial
object-store counter using cached Prometheus totals was rejected; the final
request totals use live S3 request events.

### Plaintext latency and CPU

| Condition | Baseline median ms | Candidate median ms | Change | Baseline FE/BE CPU ms per query | Candidate FE/BE CPU ms per query |
| --- | ---: | ---: | ---: | ---: | ---: |
| planning_cold_scan | 84.962 | 83.820 | -1.34% | 161.00/57.50 | 164.50/56.25 |
| planning_cold_filter | 81.011 | 77.252 | -4.64% | 149.25/29.75 | 150.75/30.50 |
| planning_cold_count | 67.039 | 64.738 | -3.43% | 138.75/11.75 | 126.25/12.25 |
| warm_scan | 34.674 | 34.248 | -1.23% | 25.50/51.50 | 40.25/50.00 |
| warm_filter | 36.474 | 30.526 | -16.31% | 35.25/22.25 | 33.50/22.00 |
| warm_count | 21.422 | 20.882 | -2.52% | 19.50/9.25 | 20.75/9.00 |

All six conditions satisfy the numeric 5% gate under this protocol. CPU totals
include process background work during the timed query; they are not exclusive
operator CPU. The local result requires performance review and does not certify
every architecture, deployment size or workload.

### Plaintext memory and object access

Peak RSS is for the whole process, sampled every 20 ms. Request and response
totals below cover forty measured query cycles; KMS decrypts are zero for all
plaintext conditions. Whole-fixture response bytes include metadata, range
prefetch and background traffic, rather than only decoded data bytes.

| Condition | Baseline FE/BE peak MiB | Candidate FE/BE peak MiB | Baseline Parquet/metadata GETs | Candidate Parquet/metadata GETs | Baseline/candidate response MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| planning_cold_scan | 3924.2/2508.4 | 5974.3/2487.5 | 3040/520 | 3042/520 | 130.54/130.64 |
| planning_cold_filter | 4317.7/2519.6 | 5985.0/2559.8 | 1129/524 | 1124/522 | 184.57/184.33 |
| planning_cold_count | 4701.1/2513.2 | 5987.4/2541.2 | 0/520 | 0/520 | 2.89/2.89 |
| warm_scan | 4800.7/2506.1 | 5987.8/2506.9 | 2560/160 | 2560/160 | 106.35/106.35 |
| warm_filter | 4904.4/2525.4 | 5988.0/2511.4 | 800/160 | 800/160 | 167.44/167.44 |
| warm_count | 4993.7/2521.2 | 5988.2/2499.4 | 0/160 | 0/160 | 1.20/1.20 |

Candidate FE process RSS is higher in this run. These peaks do not separate
committed JVM heap, JIT code, native buffers and live objects; they do not
establish a memory-overhead guarantee. The separate finalized stability census
addresses retained reader/key owners. After the final cost run, a full GC leaves
about 212 MiB of used heap out of 8 GiB committed G1 heap; this is a separate
observation from process RSS. The cost peaks still require review.

### Earlier candidate encrypted-read cost

Each row below represents twenty measured queries. RSS is sampled peak process
RSS, not incremental query memory; S3 totals include catalog lifecycle for
planning-cold and any background/prefetch traffic in the owned fixture window.
Diagnostic profile queries are excluded from the timing and request windows.

| Condition | Median ms | FE/BE CPU ms per query | FE/BE peak RSS MiB | Parquet/metadata GET totals | Response MiB | KMS decrypts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| planning_cold_scan | 97.730 | 158.00/77.50 | 5995.5/2485.2 | 2560/260 | 114.40 | 20 |
| planning_cold_filter | 98.706 | 142.50/50.00 | 5995.8/2504.5 | 1520/262 | 129.83 | 20 |
| planning_cold_count | 93.793 | 161.00/77.00 | 6005.9/2494.3 | 2560/260 | 114.40 | 20 |
| warm_scan | 57.429 | 50.50/74.50 | 6002.5/2495.9 | 2560/120 | 113.56 | 20 |
| warm_filter | 62.906 | 34.50/48.00 | 6003.4/2513.8 | 1520/120 | 128.98 | 20 |
| warm_count | 57.209 | 36.50/74.50 | 6003.9/2512.7 | 2560/120 | 113.56 | 20 |

Representative encrypted scan profiles read all 64 row groups. The selective
filter reports 44 row groups filtered by min/max and four read; completely
pruned files need not publish a reader profile. No Bloom-filter or page-index
pruning benefit is inferred from this particular predicate. Native/SQL pruning
tests exercise those switches separately.

Encrypted COUNT deliberately authenticates physical data instead of using a
metadata-only shortcut: twenty warm COUNT queries make 2,560 Parquet GETs and
twenty mock KMS Decrypt calls. The corresponding plaintext warm COUNT gate makes
zero Parquet GETs and zero KMS calls. The additional encrypted COUNT and metadata
authorization cost is an explicit security tradeoff, not a promised optimization.

## Release boundary

The deployment read switch remains false by default. The support matrix,
classified failures and activation requirements are in the read/wire contracts.
No encrypted writes, encrypted ORC/Puffin deletion vectors, encrypted JNI metadata
tables or custom transports without proof are admitted. Human performance and
security review are required before treating these local results as release
acceptance or enabling a deployment.
