<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Migrating files with the former Doris DATE encoding

The corrected DATE encoder writes proleptic Gregorian epoch days to ORC,
Parquet and Arrow. The former Doris encoder used MySQL calendar day numbers
minus 719528. For dates from `0000-01-01` through `0000-02-28`, the former value
is one day too large. For example, those two endpoints were written as
`-719527` and `-719469`, respectively; their correct values are `-719528` and
`-719470`. Dates from `0000-03-01` onward are unchanged.

A corrected reader cannot infer the intended date from an old ordinal:
`-719527` also encodes the legitimate Gregorian date `0000-01-02`.
Consequently, old files do not become compatible merely by upgrading the
reader. Identify files using writer provenance, not their date values.

## Explicit offline migration

This tool requires Python 3.8 or later and PyArrow with ORC and Parquet support. It
accepts only files **confirmed to have been produced with the former Doris
DATE encoding for every DATE field**. Do not run it on files from other
writers, mixed-origin files, or already migrated outputs. Running the
conversion twice can silently shift dates again; the confirmation flag is
an operator assertion, not automatic format detection. The lower-bound
check catches some repeated conversions but cannot identify all of them.

```bash
python tools/legacy_date_migration/migrate_legacy_dates.py \
  --input legacy.orc --output migrated.orc --format orc \
  --confirm-legacy-doris-date-encoding

python tools/legacy_date_migration/migrate_legacy_dates.py \
  --input legacy.parquet --output migrated.parquet --format parquet \
  --confirm-legacy-doris-date-encoding
```

The tool reads raw DATE32 integers and subtracts one only within the legacy
window `[-719527, -719469]`. It preserves NULLs, nested ARRAY/STRUCT/MAP fields
and other column values. It rejects DATE ordinals outside the legacy Doris
range. Conversion uses Parquet batches of at most 65536 rows or one ORC
stripe at a time; a large stripe or large nested values can still require
substantial memory. It never modifies its input or overwrites an existing
output. Output is published only after writing finishes, using an exclusive
hard link on the destination filesystem; filesystems without hard-link
support will fail without publishing output.

Arrow field/schema metadata and nullability are retained during conversion.
Parquet preserves this Arrow schema metadata. ORC has different metadata and
type conventions and cannot round-trip arbitrary Arrow metadata or field
nullability; the tool rewrites ORC data and schema supported by PyArrow,
not an exact copy of all writer-specific metadata. Compression, row groups,
stripes, statistics and indexes are regenerated. Unsupported types or
writer failures abort without publishing a partially converted file.

Keep the original files and record which files were converted. Query the
new files with `enable_file_scanner_v2=true`, comparing row counts, NULLs,
modern dates, and the year-zero boundary dates against authoritative source
values. Only switch consumers after these checks. The converted files must
be read by the corrected reader; enabling an old reader again is not a
rollback strategy for the new encoding.

## Optional legacy ORC read path

For confirmed legacy ORC files, the legacy scanner can recover the original
Doris dates before rewriting them through the corrected writer:

```sql
SET enable_file_scanner_v2 = false;
-- Read only the confirmed legacy ORC source and write to a NEW output location.
SELECT * FROM legacy_orc_source
INTO OUTFILE 's3://example-bucket/migrated/date_export_'
FORMAT AS ORC
PROPERTIES (...);
SET enable_file_scanner_v2 = true;
-- Validate the newly written files against the original source values.
```

The storage properties and source relation above are placeholders. Scope
the setting to a dedicated migration session. This is not a general
compatibility switch: legacy Parquet reading does not reliably restore
these dates, and a source containing both corrected and legacy files must
not be switched wholesale. Prefer the explicit offline tool when provenance
is known and the source format is Parquet.

## Iceberg and other table formats

Do not replace data files in place or edit their paths in existing metadata.
Existing snapshots, partition values and statistics still describe the old
files. Use migrated files as an independent source, then use the table
format's writer to populate a new table (or a supported transactional rewrite)
so that partition transforms and metadata are recomputed consistently.
Validate the new table before moving consumers and retain the old table and
snapshots for rollback. This utility migrates individual files; it does not
perform an Iceberg commit or change existing snapshots.

## Tests

```bash
python -m unittest discover -s tools/legacy_date_migration -v
```
