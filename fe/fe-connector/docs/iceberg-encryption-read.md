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
| Backend | Explicit encrypted-read protocol and reader capability on every eligible BE | Capability is currently unverified, so admission fails |
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

## Backend negotiation required before enabling execution

The protocol change must introduce an explicit, versioned encrypted-Iceberg-read
capability, advertised by each BE. An absent field, an older BE, an unknown
capability or a build without PME support means **unsupported**. A generic
execution version, the FE's Iceberg version and the FE configuration switch are
not substitutes for this advertisement.

The scheduler must restrict placement to compatible BEs and fail if it cannot
establish that all selected execution targets support the complete encrypted-read
contract. Reassignment and retries must repeat that validation. The native reader
must also reject an encrypted range it cannot handle, so a stale heartbeat cannot
silently downgrade a read. FE-to-BE key transport must have explicit ownership,
bounded lifetime and redaction rules; keys must not enter explain output, profiles
or persistent scan caches.

Until those protocol and reader changes land together, the connector deliberately
reports `BACKEND_CAPABILITY_UNAVAILABLE` rather than manufacturing a successful
capability result. No SPI or wire-format capability is added by this first stage.

See the upstream [Iceberg encryption documentation](https://iceberg.apache.org/docs/nightly/encryption/)
and [table specification](https://iceberg.apache.org/spec/#encryption-keys) for
the SDK encryption scheme and retained table keys.
