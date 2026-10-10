<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Canonical FILE format round trips

`test_outfile_file_type` uses an ORC fixture through stream load, internal
storage, local OUTFILE, re-import, and a second ORC OUTFILE. It covers ORC,
CSV (explicit and automatic quotes), and plain and gzip JSON round trips.
CSV covers explicit enclosure and automatic output quotes with a comma separator
and backslash escape. Re-import uses double-quote enclosure and the same escape
options. JSON is read as one object per line. The suite parses actual plain and
gzip JSON and checks that every non-NULL FILE, including nested values, has
exactly `uri`, `offset`, `size`, `content_type`, and `checksum`; `inline` is omitted.
A final ORC export after CSV re-import verifies every original inline byte.
After JSON re-import, the oracle explicitly expects NULL inline recursively,
including where the source had non-NULL empty bytes. All public fields and
container shapes must still match the original fixture.
It needs no external object store. Native FILE in Parquet is outside this scope.
Explicit SQL exception tests cover Parquet OUTFILE rejection for scalar FILE,
ARRAY<FILE>, the FILE-containing STRUCT, and MAP<STRING,FILE>, expecting
`Parquet OUTFILE does not support FILE`. No anonymous STRUCT fallback is used.

For the ORC input, the same six-field oracle also verifies ORC exports
after these SQL value-transport paths:

| Path | Payload and scalar keys |
|---|---|
| Shuffle join | `JOIN [shuffle]` on a scalar string derived from `id`, with payloads from both sides; EXPLAIN requires a partitioned join |
| Sort | Full sort on `id` with late materialization disabled |
| LAG / LEAD | All four FILE/container columns cross a neighboring row; a sentinel and inverse id shift restore ids 1–6 |
| ANY_VALUE | Two identical rows per scalar `id` group; all four payload columns, without DISTINCT |
| ARRAY_AGG | The same groups, followed by element extraction; includes FILE-containing ARRAY, STRUCT and MAP state |
| COALESCE / IF | Alternating branches carry all four FILE/container columns |

Every value-transport stage exports `id, f, files, holder, lookup` with the original fixture values.
The window's extra row exists only inside its query. These stages do not update
the source/restored tables or reconstruct FILE through its five public getters.

## Build and regenerate fixtures

Run from the repository root, with the installed Doris dependencies available:

```bash
python3 regression-test/tools/file_type/build.py \
  --cxx /path/to/clang++ --output /tmp/file_type_fixture
/tmp/file_type_fixture self-test /tmp/file-type-oracle-checks
/tmp/file_type_fixture generate regression-test/data/export_p0/file_type
/tmp/file_type_fixture verify orc regression-test/data/export_p0/file_type/canonical.orc
```

The build helper compiles only this standalone C++ utility using vanilla ORC
C++ APIs (`orc::Type`, readers, writers, and vector batches). It links ORC,
Snappy, Protobuf, zlib, LZ4, Zstandard, and their transitive dependencies.
It has no Arrow or Parquet headers, libraries, or FILE patch dependencies,
and neither builds Doris nor changes installed dependencies.
ORC 1.9.0 is the version used for standalone validation in this workspace;
`pkg-config` supplies the installed Protobuf and compression library flags.
`--prefix` overrides `thirdparty/installed`; `--cxx` defaults to `$CXX` or `c++`. A C++20 compiler,
Python 3, and `pkg-config` are required. The output executable is not checked in.

The checked-in `canonical.orc` fixture contains six rows:

| id | Top-level FILE | Nested coverage |
|---|---|---|
| 1 | 4,097 inline bytes, cycling through every byte value | ARRAY carries distinct bytes with identical public metadata; STRUCT and MAP retain the scalar payload; empty, NULL-inline and NULL FILE elements |
| 2 | Non-NULL, zero-length inline | Empty ARRAY/MAP and nested empty ARRAY |
| 3 | NULL inline and optional metadata | Nested external references; NULL STRUCT child |
| 4 | NULL FILE | NULL ARRAY, STRUCT and MAP ancestors |
| 5 | NULL FILE | Non-NULL containers with NULL FILE values; NULL nested ARRAY |
| 6 | Second 4,097-byte inline payload, offset `4294967303`, size `8589934609`, canonical CRC32 | Same range in ARRAY, STRUCT and MAP alongside the first binary payload |

The two nonempty payloads have equal length but differ at every position: byte
`i` is `i % 256` in the first and `255 - (i % 256)` in the second. These constants
are independent of Doris output. Both include NUL and every possible byte value.
Using both patterns within a row and across rows detects payload substitution
without relying on byte length or public metadata. NULL and empty inline values
remain separate cases.

Row 6's offset and size both exceed `UINT32_MAX`; their sum is `12884901912`,
within signed BIGINT. The shared range value supplies both the generated data
and the oracle at every nesting position, exposing 32-bit truncation.

Every canonical FILE schema has exactly six nullable children: `uri`, `offset`,
`size`, `content_type`, `checksum`, `inline`. ORC uses the ordinary STRUCT
attribute `doris.struct-type=FILE`; no ORC library patch is needed. There are five
FILE schema positions, including `STRUCT<asset:FILE,attachments:ARRAY<FILE>>`.

The generator never dereferences URIs. Raw spelling, including percent escapes
and dot segments, is part of the oracle. The canonical fixture does not exercise sparse schemas,
empty FILE groups, or malformed input. The suite's JSON inputs come from OUTFILE
and contain only the five public fields. JSON import still accepts all six
fields, including Base64 inline; CSV I/O, ORC, and Python UDF Arrow transport retain all six.

## Sparse ORC reader checks

The suite reads the chosen BE's `user_files_secure_path` with `show_be_config`
and generates additional marked ORC files in a new UUID directory directly
beneath that root using `generate-sparse`. It passes paths relative to that root
to `LOCAL()` and uses the same backend ID for every sparse read. Absolute paths
are also prefixed by `LOCAL()`, and `..` is rejected; the suite uses neither
parent traversal nor symlink escapes. No sparse binary fixtures are checked in.
Every FILE retains the required `uri` child and the canonical relative order:

| Case | Physical FILE children |
|---|---|
| `uri_only` | `uri` |
| `middle` | `uri`, `size`, `checksum`, `inline` |
| `inline` | `uri`, `inline` |
| `no_inline` | `uri`, `offset`, `size`, `content_type` |

All four cases use the six canonical rows and all five FILE schema positions:
top level, ARRAY element, STRUCT asset, STRUCT attachment ARRAY element, and MAP
value. The `middle` case leaves interior gaps before both integer and string
metadata; the `inline` case retains bytes after all earlier optional children
are omitted. Rows preserve parent NULLs, NULL FILE elements, empty containers,
and the original distinct binary payloads, including empty versus NULL inline.

For both `enable_file_scanner_v2=false` and `true`, the suite reads each file via
`LOCAL()`, records SQL public metadata and NULL/container results, and exports
the full values to ORC. `verify-sparse CASE FILE...` requires a canonical
six-child output schema, NULL for every physically omitted child, and exact
values and inline bytes for every retained child. It checks all five FILE
positions. This separate mode does not relax the original six-field oracle or
the five-field JSON output checks. The previous session setting is restored.

Twelve additional readable ORC fixtures have invalid live FILE values, separately
at the top level and under STRUCT: NULL URI, empty URI, negative size, negative
offset, offset without size, and offset-plus-size overflow. Each uses a sparse
schema and must produce its specific FILE validation error in both readers.
The missing-size schema is `uri,offset,inline`; only the target FILE has a
non-NULL offset. These fixtures test value validation after missing children
are filled with NULL, without changing required-URI or child-order policy.

The generator also writes `sparse_invalid_uri_type.orc`, a zero-row file with
physical schema `struct<f:struct<uri:bigint>>` and `doris.struct-type=FILE` on `f`.
Both readers must return `Invalid FILE ORC child uri type` during `LOCAL()` schema
discovery. This checks that the existing physical-type validation returns a query
error without crashing the BE, even when there are no rows to decode. The
standalone generator/self-test verifies the stored schema, marker, and zero row
count; the regression suite checks the Doris error.

To generate fixtures for manual reader testing, first read the chosen BE's
`user_files_secure_path`. Generate under that directory as seen by the runner:

```bash
/tmp/file_type_fixture generate-sparse /absolute/be/secure/root/doris-file-orc-sparse-red
# After SQL reads sparse_middle.orc and exports full values to ORC:
/tmp/file_type_fixture verify-sparse middle /path/to/sparse-restored.orc
```

The corresponding `LOCAL()` property is
`"file_path"="doris-file-orc-sparse-red/sparse_middle.orc"`, with the chosen
`backend_id`. The secure root defaults to the BE's `DORIS_HOME` (for example,
`output/be`), which need not match `fileTypeOutfileRoot`.

The standalone `self-test` also checks sparse fixture schema/value readability
and rejects damaged expanded outputs: changed inline at each FILE position,
non-NULL omitted metadata, changed parent NULLs, and missing rows. It does not
run Doris SQL or assert that Doris rejects the invalid input fixtures; those
checks belong to the regression suite.

## Local cluster prerequisites

This is an explicitly enabled local regression. Before running it:

1. Use a cluster with FILE enabled and compatible FE/BE versions.
2. Enable FE `enable_outfile_to_local=true`.
3. Run the regression runner on the BE host, or mount the **same absolute path**
   on the runner and every participating BE. All participants need read/write
   permissions in the test directory, including directories created by the
   runner. Using the same operating-system user on one host is sufficient.
   The chosen BE's `user_files_secure_path` must also be visible at its configured
   absolute path and writable by the runner; that BE must be able to read the
   generated files. The runner must be able to read BE config via `show_be_config`.
4. Build the standalone verifier on the regression runner host.
5. Set these properties in the isolated regression configuration:

```groovy
enableFileTypeLocalOutfile = true
fileTypeFixtureTool = "/tmp/file_type_fixture"
fileTypeOutfileRoot = "/tmp/doris-file-outfile"
```

Create `fileTypeOutfileRoot` before starting. Its path must contain only letters,
digits, `/`, `_`, `.`, or `-`. The suite creates a UUID output subdirectory there
and a separate UUID fixture subdirectory under the chosen BE's secure root.
It writes only inside those two directories and removes both afterward, including
on failure. It retains the two SQL tables
for diagnosis. It does not enable cluster settings itself or copy files between
hosts. Without the explicit opt-in, it skips with a prerequisite message; with
opt-in, missing prerequisites fail the test.

### Optional Python FileRef pickle round trip

The SQL/format paths above do not require the Doris Python UDF runtime. To also
verify scalar FILE through Python, enable this separately in the isolated runner
configuration (in addition to the local OUTFILE settings):

```groovy
enableFileTypePythonRoundtrip = true
pythonUdfRuntimeVersion = "3.11.13" // Must match the runtime installed on every participating BE.
```

This uses the existing `getPythonUdfRuntimeVersion()` helper from the Python UDF
suites. BEs must have `enable_python_udf_support=true`; the selected runtime must
support Doris Python UDFs and provide the FILE-aware `doris_udf.types` module.
With the option disabled or absent, the suite
neither queries the Python runtime version nor creates a Python function.

The optional scalar UDF checks the public `FileRef` type and returns
`pickle.loads(pickle.dumps(value))`, including SQL NULL. It cannot read inline
bytes through the public API: the subsequent ORC export and independent
oracle verify those bytes, including both 4,097-byte payloads and NULL versus empty
inline values. Nested columns pass through unchanged in this optional stage;
Python container/UDAF/UDTF round trips are outside its coverage. The temporary
function is dropped afterward, and both six-row SQL tables remain for diagnosis.

Use the official regression runner and isolated configuration:

```bash
./run-regression-test.sh --conf /path/to/isolated-regression-conf.groovy \
  -d export_p0 -s test_outfile_file_type -forceGenOut
./run-regression-test.sh --conf /path/to/isolated-regression-conf.groovy \
  -d export_p0 -s test_outfile_file_type
```

Regenerate the existing `.out` with `-forceGenOut` because the old golden may
contain removed Parquet cases; `-genOut` only generates a missing file.
Run these official runner invocations serially with other build/test work. Do
not hand-write or edit the `.out` file. The suite uses `qt` with deterministic ordering for schema and public-field SQL results. It uses
assertions only for transport status, actual JSON shape, and the independent
binary/schema oracle.

## Independent binary oracle

The verifier reads every output file, checks recursive format annotations and
canonical child types, and compares each of the six decoded children against
the built-in fixture values. It checks ancestor NULLs, array ordering, map values
by key, row completeness, and duplicate ids. It distinguishes NULL inline from
empty bytes and compares all binary bytes directly. It links no Doris code and
uses no SQL FILE equality or public five-field cast.

`verify orc FILE...` accepts multiple output fragments in any order. The
default always checks all six original fields, including exact inline bytes.
Only the ORC check after JSON re-import uses the explicit lossy mode:

```bash
/tmp/file_type_fixture verify orc --expect-null-inline /path/to/json-restored.orc
```

`--expect-null-inline` requires NULL inline at every non-NULL FILE while checking
the original five public fields, container contents, ancestor NULLs, and native
six-child schemas. It does not ignore inline or accept empty bytes in its place.
It must not be used for source fixtures, CSV round trips, or native format and
value-transport checks.

The `self-test` command creates valid files and readable negative controls in its
scratch directory, checking rejection of changed top-level/nested bytes,
swaps of distinct equal-length inline values at scalar, ARRAY, STRUCT and MAP
positions, NULL/empty swaps, changed ancestor NULLs, missing FILE markers, changed URIs,
32-bit truncation of top-level offset or nested size, and missing rows.
It also verifies split files in reverse order and maps with
reordered entries. Those controls are not checked in or stream-loaded.
Additional controls verify the NULL-inline mode, reject retained bytes at all
five FILE positions (including retained empty inline), reject changed public
metadata, ancestor NULLs and missing rows, and ensure that each oracle mode
rejects the other mode's valid files. The generator writes only ORC fixtures.
