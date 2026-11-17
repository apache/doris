# Iceberg encrypted-read contract

This is the admission contract for the first stage of native Iceberg encrypted
reads. Encrypted queries remain unavailable until key transport, native readers,
delete handling and backend capability negotiation are implemented. Building
Arrow with Parquet modular encryption alone does not establish those capabilities.

## Configuration and admission

`enable_encryption_read` in the Iceberg plugin's `iceberg.conf` defaults to
`false`. It is deployment configuration: set it consistently on every FE and
restart the FE after changes. It is neither a catalog property nor a session
variable. Enabling it expresses operator intent; it does not override missing
execution capabilities or enable encrypted writes.

Plaintext tables retain their existing paths, including metadata-only counts.
For encrypted tables the same check applies to metadata binding, snapshot
resolution, ordinary scans, metadata tables, streaming scans and COUNT pushdown.
The check runs against the base table before constructing a metadata table or
reading its manifests. An empty snapshot does not exempt an encrypted table.

The initial target matrix is:

| Dimension | Initial target | Current admission |
| --- | --- | --- |
| Catalog | HMS with the SDK standard encryption manager and an operator-configured KMS | Other catalogs are rejected |
| Table format | Iceberg v3 | Other versions are rejected for encrypted reads |
| Data and delete files | Native Parquet modular encryption, `AES_GCM_V1` | No encrypted ranges are emitted yet |
| Manifest/list files | SDK-managed encryption | No plaintext fallback is allowed |
| Backend | Native encrypted reader and protected FE/BE key transport | Both remain unavailable, so admission fails |
| Writes | Outside this read contract | The read switch grants no write capability |

Table encryption is recognized from `encryption.key-id`, retained v3
`encryption-keys`, or a non-plaintext SDK encryption manager. Retained keys matter
when an administrator removes the current write key while older snapshots still
reference encrypted files. The check must not log key identifiers, key metadata,
KMS credentials or key material.

The writer's `write.format.default` is not evidence of the format of existing
files. File format, algorithm, key metadata and AAD must be validated on the
actual data/delete tasks by the subsequent planning and reader implementations.
This stage rejects encrypted tables before that work, even with the switch on.
It does not inspect every plaintext table's manifests just to discover encryption.
Externally encrypted files with no Iceberg encryption declaration are outside
this table-level contract; data-task validation must reject them when encountered.

## Failure classification

Errors contain a stable `Iceberg encryption [CLASSIFICATION]` prefix:

| Classification | Meaning |
| --- | --- |
| `READ_DISABLED` | Encrypted reads were not enabled in the plugin configuration |
| `UNSUPPORTED_CATALOG` | The catalog is outside the initial HMS scope |
| `UNSUPPORTED_TABLE_FORMAT` | The encrypted table is not v3 |
| `UNSUPPORTED_ENCRYPTION_MANAGER` | Encryption detection observed a custom SDK encryption manager |
| `BACKEND_CAPABILITY_UNAVAILABLE` | The engine cannot prove encrypted-read support |

The first applicable failure is returned in this order. For a table with an
encryption declaration, backend admission fails without initializing its KMS
manager; manager and per-file validation belong to subsequent read implementation.
A rejected read must
not fall back to plaintext, metadata counts, a JNI reader or another catalog.
Plaintext reads in those catalogs continue to work.

## Requirements before enabling execution

The scan protocol carries each data/delete file's binary key and AAD independently.
FE and BE are deployed together for this feature; no heartbeat capability
negotiation is required. FE must reject encrypted dispatch and lazy split
responses until the native reader and protected internal key transport are ready.
BE must also reject encrypted data and deletes before reader, cache or COUNT
paths. Repeated split fetches must preserve an existing rejection.

The connector currently reports `BACKEND_CAPABILITY_UNAVAILABLE` before file
descriptors are created. The [scan wire contract](iceberg-encryption-wire.md)
defines per-file payloads, diagnostic redaction and the later dispatch guard.
File keys remain query-scoped, with bounded lifetimes; they must not enter EXPLAIN
output, profiles or persistent scan caches.

See the upstream [Iceberg encryption documentation](https://iceberg.apache.org/docs/nightly/encryption/)
and [table specification](https://iceberg.apache.org/spec/#encryption-keys) for
the SDK encryption scheme and retained table keys.

## SQL regression

`test_iceberg_encryption_read_guard` uses the standard external Iceberg REST and
MinIO fixture. With `enableIcebergTest=true`, run:

```sh
bash run-regression-test.sh --run -d external_table_p0/iceberg \
    -s test_iceberg_encryption_read_guard
```

The suite writes ordinary v3 tables, then uses the REST fixture to add an
encryption declaration to a populated table and an empty table. These are
metadata admission fixtures, not encrypted payloads: the expected failure must
precede file decryption and requires no KMS. It checks ordinary SELECT, COUNT
with pushdown enabled/disabled, DESC and system tables, with the table cache
enabled/disabled and repeated reads. Plaintext row/count results are checked
before and after rejection. Missing fixture services fail the enabled suite.

The suite runs against the default FE deployment, with `enable_encryption_read`
absent or set to `false` in every FE's Iceberg plugin configuration. It always
expects `READ_DISABLED`, including when the catalog requests encrypted reads;
there is no separate regression setting for the expected deployment state.
An incorrect error classification fails the suite.

Connector unit tests explicitly construct enabled and disabled plugin contexts
to cover both switch states, unsupported catalogs and table formats, retained
v3 keys, custom managers and unavailable backend capabilities. The SQL suite
and these unit tests validate admission; they do not exercise native decryption
or grant encrypted-write support.

## HMS/KMS metadata validation

The HMS catalog forwards `encryption.kms-type`, `encryption.kms-impl` and the
SDK KMS client properties without reimplementing the Iceberg encryption chain.
The existing Iceberg plugin closure includes the AWS KMS client and AWS SDK KMS
module. A connector test exercises binary-key wrap/unwrap through the SDK HTTP
client. Other KMS implementations require their corresponding operator-installed
local dependencies and are not validated by this fixture.

The authentication FileIO decorator preserves the SDK's typed data, delete,
manifest and manifest-list descriptors. Replacing them with path-only reads, or
inheriting FileIO's typed defaults, loses encryption metadata and rejects encrypted
files before the delegate can decrypt them. All four typed factory calls stay
inside the same authentication scope. Key metadata is parsed by the SDK; v1
`StandardKeyMetadata` in 1.12.0 also carries the physical length required for
AES-GCM metadata streams.

`test_iceberg_encrypted_hms_metadata` uses the mandatory encryption services in
the standard Iceberg Compose stack. Run it with `enableIcebergTest=true`:

```sh
bash docker/thirdparties/run-thirdparties-docker.sh -c iceberg
bash run-regression-test.sh --run -d external_table_p0/iceberg \
    -s test_iceberg_encrypted_hms_metadata
```

The isolated reference process uses checksum-pinned Iceberg 1.12.0 artifacts;
it does not replace the existing Spark fixture's SDK. It generates two v3 tables
in a dedicated bucket: one plaintext and one encrypted, each with two appended
files and two snapshots. Both the SDK generic reader and Spark SQL verify the
historical rows `[1, 2]` and `[1, 2, 3, 4]`. The fixture also verifies native
Parquet encrypted-footer magic, independent file key metadata, encrypted
manifests, encrypted manifest lists and the retained table key chain.

The test KMS is a disposable Moto service, with no production credentials or
keys. Missing KMS configuration fails actual HMS metadata planning. Injected AWS
protocol errors verify propagation of invalid-credential and access-denied
failures through that same path; this is not a live AWS IAM policy test. Replacing
the wrapping key material with a different KMS-wrapped key must fail the SDK's
AES-GCM authentication check before the manifest list can be read.

Doris SQL verifies plaintext rows/counts and exactly `READ_DISABLED` for the real
encrypted table, including SELECT, COUNT pushdown on/off, DESC and system tables,
with table caching on/off and repeated reads. No additional test-mode setting is
needed. The fixture fails rather than skipping when a required service or check
is unavailable. It prepares input for the subsequent file-key and native-reader
work; it does not admit encrypted SQL execution.

## Per-file key descriptors

Native data-file scan ranges carry the file data key and AAD from the SDK's
`StandardKeyMetadata`. Manifest decryption has already exposed this metadata;
extracting it does not make an additional KMS request for every split. A bounded
adapter validates encoded lengths before invoking the SDK codec, and rejects
unknown versions, corrupt metadata, invalid AES key lengths and absent AAD
without including binary payloads in errors.

The immutable file carrier is reused only across that file's byte-slices; every
Thrift descriptor receives independent mutable buffers. A null per-file metadata
field remains plaintext, including in a mixed snapshot. This is key transport
only: the deployment admission and backend dispatch guards remain closed until
native decryption, encrypted deletes and pruning are integrated.

Both metadata COUNT algorithms validate live file metadata while retaining only
one representative task. A per-file encryption marker causes an immediate
`Iceberg encryption [COUNT_READ_UNAVAILABLE]` rejection, including in a table
without a table-level encryption declaration. COUNT scheduling has already
disabled streaming; normal-scan fallback would retain all file tasks and ranges
before the dispatch guard rejects them. This deterministic rejection must not
trigger the manifest cache's SDK retry. Plaintext COUNT retains its existing
collapse and cache-failure fallback behavior.
