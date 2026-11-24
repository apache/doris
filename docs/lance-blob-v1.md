# Lance Blob v2 descriptor scans and FL_GET_BINARY

This file describes the current uncommitted refactor. The detailed design is in
[lance-blob-lazy-read-design.md](lance-blob-lazy-read-design.md).

## SQL contract

```sql
SET enable_file_scanner_v2 = true;

SELECT id, image FROM lance_table;                 -- descriptor FILE
SELECT id, fl_get_binary(image) FROM lance_table;  -- payload VARBINARY
```

There is no `lance_blob_as_descriptor` session variable. All four Blob v2 storage
kinds, including kind=0 Inline, produce descriptors during scans. Ordinary binary
columns retain their existing mapping. Nested Lance BLOB columns remain unsupported.

FL_GET_BINARY returns NULL for an outer SQL NULL, returns existing inline bytes
without remote I/O, or reads a bound Lance reference. Empty bytes remain distinct
from NULL. Descriptor-only FILEs without a bound source fail rather than guessing
storage credentials. The function does not change the input FILE.inline field.

## Read provenance

Private read provenance is dictionary-encoded inside each FILE column. Every row
stores a UInt32 binding ID and a UInt64 native row ID: **12 bytes of additional
column data**, excluding the public FILE fields, capacities and shared metadata.
The expected payload size comes from the public size field when known. External
size-zero markers stay NULL until the SDK resolves their length at consumption.

Each immutable binding contains the query identity, source scan node ID, dataset
URI, pinned version, column and optional origin BE. Equal bindings are interned;
column clones share the dictionary until a new binding requires copy-on-write.
Field round trips hold a shared binding pointer and a row ID, rather than copying
URI strings. Filtering, permutation and copies retain the relation. Cross-column
copies remap local IDs and merge equivalent bindings.

Storage options stay in the query's existing file scan parameter map, which
Coordinator sends to participating BEs. QueryContext owns cached fixed-version
Lance datasets independently of the original Scanner.

Block transport always carries the encoded binding dictionary once,
followed by the compact binding-ID and row-ID arrays. Internal PValues transport
carries the same three components and reconstructs the dictionary before merging
rows into a destination column. IDs are local to this dictionary, not process-wide
handles. FILE uses this single internal encoding without a separate execution-version
gate. Older FILE encodings are not supported by this implementation.

Standalone arena row serialization has no dictionary channel. It remains
self-contained (binding bytes plus row ID); restoration re-interns bindings in the
destination column. Thus column/Block transport benefits from this optimization,
while independently serialized arena rows can still repeat binding metadata.

Public JSON still contains only the canonical six fields. The URI is a query-scoped
`lance-blob://...` logical locator containing the source identity, redacted and
URL-encoded dataset identity, pinned version, column and native row ID. It is not
a physical object URL. The actual dataset URI and access context stay in the
private binding; password-bearing user information, query strings and fragments
are omitted from the public locator. FILE.offset is NULL.

Public JSON does not export the private binding. Exporting/importing JSON
does not preserve query-local read authorization. Local dataset/object references
must be consumed on the originating BE. JNI cannot silently drop provenance.

## Current read path

The table reader consumes the scan's existing Arrow Blob v2 descriptors and builds
logical references directly. It performs no per-row take, HEAD or physical URI
resolution. Struct NULL and kind-child NULL both remain SQL NULL. Managed Blob
lengths, including empty inline values, are retained; external size=0 remains
unknown. Dataset roots and registered bases are inspected once per open for
locality; absolute external object URIs are checked from the scan descriptor.
FL_GET_BINARY groups its input references by source/snapshot/column, deduplicates
row IDs within the call, obtains SDK handles in a batch and synchronously reads
each unique payload. A value's size is checked before payload allocation/read;
known-length changes since scanning fail. Unknown lengths are resolved before any
payload allocation/read. There is no Lance-specific payload size limit; lengths must
fit VARBINARY's UInt32 representation, and reads remain subject to query memory limits.
Equal-length external-object
replacement is not detected without versioned/conditional object reads.

This implementation resolves SDK handles by row ID only during content consumption;
it does not yet consume the scan descriptor directly in the payload API or expose
the Rust streaming batch reader. There is no asynchronous expression execution.

## Review entry points

- `lance_reader_helper.cpp` and `LanceTypeConverter.java`: Blob v2 type mapping.
- `lance_reader.cpp` and `lance_blob_column_reader.cpp`: descriptor-only output.
- `file_read_reference.{h,cpp}`, `column_file.*`, `data_type_file.cpp`: provenance.
- `file_content_reader.{h,cpp}` and `function_fl_get_binary.cpp`: content reads.
- `FileQueryScanNode.java` and `PlanNodes.thrift`: source ID and storage options.
- `thirdparty/patches/lance-c-base-paths.patch`: backports upstream
  `lance_dataset_base_paths_json` to the pinned SDK for read-only source-locality
  inspection. No FILE-specific SDK type or Blob resolver is added. The SDK must
  be rebuilt before linking Doris; dependency and Rust toolchain pins are unchanged.

## Scope and validation

CASE/IF branch selectors, further FE projection placement and async execution are
next-stage work. The current guarantee is that scans produce descriptors and
payload reads occur when FL_GET_BINARY executes; the function's current plan
position can still cause reads of rows later discarded by upper operators.

No FE/BE compilation, SDK rebuild or runtime tests were performed for this refactor,
at the user's request. Installed dependency artifacts have not been replaced.
