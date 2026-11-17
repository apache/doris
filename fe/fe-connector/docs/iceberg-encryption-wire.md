# Iceberg encrypted scan wire contract

This is protocol preparation. It does not enable encrypted SQL reads, implement
native decryption, or establish a protected RPC channel. D1 admission remains in
place; D4 also rejects secret-bearing ranges before dispatch and on the BE.

## File payload

`PlanNodes.thrift:TFileEncryptionInfo` is a query-scoped secret carrier.
Its optional fields are `1: version`, `2: binary key`, and `3: binary aad_prefix`.
Version 1 means Parquet AES_GCM_V1 with the SDK-unwrapped file key and exact AAD
prefix bytes. AAD is binary; do not convert it to text or reconstruct it from the
file path. Native reader initialization belongs to subsequent work.

Each `TIcebergFileDesc` has optional field **14** for the data-file payload.
Each `TIcebergDeleteFileDesc` has optional field **12** for its own payload.
Data and delete files never inherit each other's keys. This also preserves a
plaintext data file with encrypted deletes, and encrypted data with plaintext
deletes. These numbers were checked against master and branches 4.2, 4.1, 4.0
and 3.1; data-file field 13 remains reserved for historical equality-delete schemas.

Absence of the entire payload retains the existing plaintext wire semantics.
Presence requires an explicitly supported version, a 16/24/32-byte key and a
present AAD field, including an empty prefix. Empty structures, missing fields,
invalid key sizes and future versions fail with static classifications:
`INVALID_DESCRIPTOR` or `UNSUPPORTED_VERSION`. They never fall back to plaintext.
Table admission still detects encrypted declarations before file descriptors exist.

Binary and Compact Thrift preserve every byte, including NUL and non-UTF-8 bytes.
Java's existing object serialization also uses explicit Compact Thrift encoding.
These encodings are transport representations, not permission to persist keys.
Never put them in durable plan dumps, persistent caches or audit records.

## Dispatch

FE and BE are deployed together for this feature. This change does not introduce
heartbeat capability fields, version negotiation, downgrade handling or backend
capability persistence. Native encrypted execution remains uniformly disabled
until the reader and protected key transport are implemented.

The pipeline task builder validates assigned file parameters and ranges before
serialization. The direct typed fragment RPC entry has the same restriction.
Each lazy `SplitSource` batch is checked before the FE response is populated.
A rejected lazy batch stops its producers and keeps a terminal failure, including
when the final batch was already drained. Replanned or reassigned attempts repeat
the checks. Errors contain classifications, not payloads. The BE checks both data
and delete payloads before reader selection,
I/O, cache lookup and metadata COUNT handling, even if FE sent an unsupported range.

## Transport prerequisite

Before native reads can open, **both** fragment dispatch from FE to BE and lazy
split responses from FE to BE must have authenticated, confidential transport.
The latter uses the separate BE-to-FE Thrift connection. Every selected endpoint,
including retry targets, must meet this requirement. HTTPS for SQL, catalog or
object storage access does not establish protection of either internal channel.

The OSS fragment channel provider currently selects plaintext gRPC. D4 therefore
rejects every encrypted dispatch with `READ_UNAVAILABLE`. There is no configuration
flag that asserts the transport is secure. A subsequent change must implement the
native reader and verify actual protection of both channels before lifting this
restriction. Enabling the Iceberg plugin option cannot bypass it.

## Diagnostic redaction

Thrift 0.24 does not enforce sensitive annotations. The normal FE generation,
regression-framework generation and C++ generation apply the same narrowly scoped
`gensrc/script/redact_file_encryption.py` transform. A compiler shape mismatch
fails generation. The transform is idempotent and leaves wire field IDs intact. C++ redaction
targets only `TFileEncryptionInfo` inside the generated `PlanNodes_types.cpp`;
other plan descriptors retain their ordinary diagnostic output.

- Generated Java `toString()` and C++ `printTo()` redact the entire carrier,
  including when nested inside scan descriptors and plan parameters.
- C++ `ThriftDebugString` serializes through a debug protocol, so changing
  `printTo()` alone is insufficient. Only supported Binary/Compact wire protocol
  implementations receive the actual fields; diagnostic protocols receive a
  redacted placeholder. Java JSON Thrift protocols behave the same way.
- Generated Java instance fields are transient for reflective diagnostic Gson.
  Thrift explicitly reads/writes the fields, so RPC and existing Java object
  serialization are unchanged. Default Gson and FE's Gson configuration omit the
  carrier fields. Non-wire diagnostic JSON is deliberately unable to restore keys.

Do not log serialized RPC byte strings or manually format key/AAD buffers.
This change does not promise memory zeroization, protected crash dumps or a
key-scoped cache lifecycle; those remain required before final read enablement.

## Validation

`FileEncryptionInfoTest` covers Java Binary/Compact round trips, distinct data
and delete values, absent payloads, object serialization, nested strings, Gson
and both JSON Thrift protocols. `FileScanEncryptionTest` checks descriptor shape.
FE guard tests cover independent delete kinds, unknown payload versions, repeated
dispatch and rejection before execution. SplitSource tests preserve a terminal
error when a rejected last batch is fetched again.
`FileEncryptionInfoTest` in BE covers binary bytes, key sizes, stream printing,
nested containers, `ThriftDebugString` and JSON; FileScannerV2 has a pre-reader
rejection test. Generator self-tests cover idempotence and compiler drift.

Run the generator checks with:

```sh
python3 gensrc/script/test_redact_file_encryption.py
```

`test_iceberg_encryption_wire_guard` is an external SQL regression. It uses
plaintext v2/v3 tables, then a path-scoped debug point injects synthetic descriptors
at the dispatch/fetch guard. Eighty expected rejections cover data/delete payloads,
invalid/future descriptors, synchronous/streaming scans, count pushdown on/off and
repeated queries. EXPLAIN verifies that v2 actually takes the lazy path while
current v3 planning remains synchronous. Plaintext rows/counts are compared before
and after. This tests protocol enforcement; the real encrypted HMS fixture is separate metadata work.

```sh
bash run-regression-test.sh --run -d external_table_p0/iceberg \
    -s test_iceberg_encryption_wire_guard
```

The standard Iceberg stack and FE debug points must be enabled. No separate
feature-mode variable is needed. These tests do not replace a future complete
native encrypted SQL interoperability run.
