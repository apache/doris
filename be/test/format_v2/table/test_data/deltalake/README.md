# Delta Lake reader fixtures

Unmodified files from the Delta Lake project's Apache-2.0-licensed golden tables:

- Release: v4.4.1
- Commit: `cebdc44dc07d331383e72811177af4ffc8638990`
- Source: `connectors/golden-tables/src/main/resources/golden/log-replay-dv-key-cases`
- Generator: `connectors/golden-tables/src/test/scala/io/delta/golden/GoldenTables.scala`
- Upstream: https://github.com/delta-io/delta/tree/cebdc44dc07d331383e72811177af4ffc8638990/connectors/golden-tables

The generator writes `spark.range(50).repartition(1)` and then deletes id 0, 7, and
14 in successive commits. The Parquet file contains the original 50 rows. Each
DV starts at offset 1, after the file version byte; its sizeInBytes excludes the
four-byte length prefix and four-byte checksum. Tests also extract the portable
payload to exercise the inline representation without synthesizing a bitmap.

| File | sizeInBytes | Cardinality | Deleted positions | SHA-256 |
| --- | ---: | ---: | --- | --- |
| `deletion_vector_37d10da3-70a1-4730-bc58-3b44f9617505.bin` | 34 | 1 | 0 | `aa23945a1206ddf0f7551d62468a4d5b34d5d9a55c6a89d1ecfa56337a87cdf2` |
| `deletion_vector_3d8a467a-2fbd-4d35-8e3a-775894a30576.bin` | 36 | 2 | 0, 7 | `6aa4d9879e16533881cb15e8f5ae7b871939dcaf679d4fe14b37112370a3974a` |
| `deletion_vector_d12e7d16-e46d-48c9-8a71-b222c26dfc3b.bin` | 38 | 3 | 0, 7, 14 | `4d91ecfc1e7f6571cd2393ee99f40c49e645db89b81799c0c09a62011492b57e` |
| `part-00000-90177277-75c2-48db-92a2-20dcba39fd06-c000.snappy.parquet` | — | — | — | `23548eea29f5e4c9c57d2aed7a168dcd9732d1617a3fe0c3a524a338b89b2658` |

Additional unmodified Parquet files from the same commit cover type widening and
timestamp semantics. Expected rows were cross-checked with PyArrow 21.0.0. The
INT96/MICROS/MILLIS files each hold the instant 2020-02-01 08:09:10 UTC; the NTZ
file holds the wall-clock value 2021-11-18 02:30:00.123456. Widening files cover
values before and after schema evolution, including integer bounds and nested
struct/array/map values.

| Golden-table source path | SHA-256 |
| --- | --- |
| `type-widening/part-00000-1045efe0-45bb-4b99-9f83-5ffa04a63ab2-c000.snappy.parquet` | `696e42eb3c13413ff054801e661b4325b5661a2fc6289ea01fac5d6baa8cf908` |
| `type-widening/part-00000-cd317895-4ae0-4292-b918-62d4ca832bd7-c000.snappy.parquet` | `d2b7af9733b4039f159e7a3722aa09ceff2c5199760617f8795f1d8d2b800c1a` |
| `type-widening-nested/part-00000-138244f1-b939-40db-a4bd-d57cf3d214d2-c000.snappy.parquet` | `109375e0614eb11197629b4f9757d4ef0c17c8bcd94feafe95ec4e7fa58cde8d` |
| `type-widening-nested/part-00000-1f777f86-350c-4181-b7ef-73df70847eac-c000.snappy.parquet` | `f19ff0530f929f936603434c120257593f338b1aede47beb065f517c69562e9e` |
| `kernel-timestamp-INT96/part=2020-01-01 08%3A09%3A10.001/part-00000-bd889aef-417c-4493-b5f7-a9884ba4b247.c000.snappy.parquet` | `a5a227a34fd9166b33d7bd442237f4b470851452e597df727a77625b787b0211` |
| `kernel-timestamp-TIMESTAMP_MICROS/part=2020-01-01 08%3A09%3A10.001/part-00000-3cac2575-d0b4-4647-a7a3-b4a9d910cb32.c000.snappy.parquet` | `e347f366786d2736e3d7dcbf5ba89859ade96d333486f417b08d9c64e9205e60` |
| `kernel-timestamp-TIMESTAMP_MILLIS/part=2020-01-01 08%3A09%3A10.001/part-00000-4b5188f5-4784-47ce-b4ad-1d3eae80710e.c000.snappy.parquet` | `74b764568c8e19e47d1c9591bdb8e7a4f4f42bbae47b619ebe5aa9b3adb1ddb9` |
| `data-reader-timestamp_ntz/tsNtzPartition=2013-07-05 17%3A01%3A00.123456/part-00000-6240e68e-2304-449a-a1e6-0e24866d3508.c000.snappy.parquet` | `50ce09f271845ec07b67abef02a7cdcc5a981025aec93f4ef4da1fadcbba5db9` |
| `table-with-columnmapping-mode-id/part-00000-37fc7686-b5a9-432d-8cdc-8caa8cf999e5-c000.snappy.parquet` | `7c859554d119d148fa704d7c9a2a31ef6e59c02fe87a78bfd2dc2b977efaf136` |

The column-mapping fixture carries real Parquet field IDs. Projected field 4 and
nested field 13/15/16 both contain rows 0 and 4; the reader test renames every
projected logical field while retaining those IDs. The byte-range test generates
a separate Parquet file at runtime with six row groups and applies the real DV
above to positions 0, 7, and 14 across row-group boundaries.
