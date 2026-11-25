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

# Build the Legacy and REST DLF fixtures

Follow the [shared workflow](../README.md) first. DLF is a metadata service;
creating its catalog and databases does not write Paimon/Iceberg files. Build
native format metadata and data, register their actual locations, then read them
through both the native client and Doris.

## Allocate catalogs and storage

In the target region/account, prepare one Legacy HMS-type DLF catalog and separate
Paimon and Iceberg REST catalog names, plus dedicated OSS warehouse prefixes.
Read back their owner, region and storage locations. The validated DLF REST
control plane accepted catalog type `PAIMON` for both REST catalogs; the Iceberg
table is created through the Iceberg API. Do not infer table format from that
control-plane type. Confirm the target service's supported catalog types.

Legacy catalog preparation uses the DLF 2020-07-10 API; create databases
`regression_paimon` and `regression_iceberg`. The REST catalog control-plane
client used the DLFNext 2025-03-10 API. These operations and data-plane access
need their corresponding permissions; an empty database list is insufficient
proof that the writer can access OSS or commit metadata.

The validated data-writing runtimes were Python 3.12, `pypaimon` 2.1.0,
`pyiceberg-dlf` 0.11.1.post1 (exposes the `pyiceberg` module), and PyArrow 19.0.1.
Use a compatible DLF-enabled runtime and inspect its credential/signing options.
These REST writers are separate from the EMR Paimon 0.9 migration runtime.
Run the writer from a network that reaches its catalog endpoint and OSS. Python
HTTP and PyArrow object-storage clients can have different proxy configuration;
a successful metadata request does not prove data IO connectivity.

## Exact fixture contract

| Catalog path | Namespace.table | Schema | Rows / layout |
| --- | --- | --- | --- |
| Legacy Paimon | `regression_paimon.tb_simple` | `id INT, name STRING` | `(1,'a'), (2,'b')` in snapshot 1 |
| Legacy Iceberg | `regression_iceberg.tb_simple` | `id INT, name STRING` | `(1,'a'), (2,'b')` |
| REST Paimon | `new_dlf_paimon_db.user_samples` | `user_id INT, age INT, gender STRING, is_active BOOLEAN` | `(1,25,'M',true), (2,18,'F',false)` |
| REST Iceberg | `test_iceberg_db.test_iceberg_table` | `k1 INT, k2 STRING` | `(1,'abc'), (2,'def')`, exactly one PARQUET data file |

Create fresh tables and commit each two-row fixture once. Replaying inserts
changes row counts and Paimon history. Preserve Paimon snapshot 1 and its schema,
manifest and data files. Keep Iceberg metadata/manifests with their referenced
data files. The write regression suites create additional runtime tables; those
are not part of these four seeds.

## 1. Write and register Legacy tables

Create the Legacy catalog/databases before registration. For Paimon, a filesystem
catalog can create the native table at the selected OSS warehouse. The tested
factory options were:

```python
properties = {
    "metastore": "filesystem", "warehouse": warehouse,
    "fs.oss.endpoint": oss_endpoint, "fs.oss.region": region,
    "fs.oss.accessKeyId": access_key, "fs.oss.accessKeySecret": secret_key,
    "fs.oss.impl": "legacy",
}
```

Values come from the private target mapping/credential provider. Using
`pypaimon.CatalogFactory.create(properties)`, create the database and
`regression_paimon.tb_simple` with `ignore_if_exists=False`. Build the schema from
an Arrow table whose `id` is explicitly `pa.int32()` and whose `name` is string.
Use `bucket=-1`, `file.format=parquet` and snapshot retention that preserves
snapshot 1. Write one batch and commit it:

```python
builder = table.new_batch_write_builder()
writer, commit = builder.new_write(), builder.new_commit()
try:
    writer.write_arrow(data)
    commit.commit(writer.prepare_commit())
finally:
    writer.close()
    commit.close()
```

Reload the table and obtain its actual `table_path`. Register that location in
Legacy DLF; a guessed warehouse path can register a different or empty table.

For Legacy Iceberg, use an Iceberg writer/catalog with the selected OSS warehouse,
for example the tested PyIceberg SQL staging catalog backed by a private SQLite
file. Create the two nullable fields with IDs 1/2, format version 2 and
`write.format.default=parquet`, then append the two-row Arrow batch once. Reload
it and obtain `table.location()` and its **post-append** `metadata_location`.
Keep the staging catalog until DLF registration and native/Doris readback pass.

Register the two tables with the Legacy DLF `CreateTable` API:

- `catalog_id` is the newly allocated Legacy catalog; database names and table
  name `tb_simple` follow the contract above.
- Set `TableInput.table_type=EXTERNAL_TABLE`, no partition keys, and storage
  descriptor columns `id:int`, `name:string` with the actual table location.
- Set parameters `EXTERNAL=TRUE`, `table_type=PAIMON` or `ICEBERG`.
  Iceberg also requires `metadata_location` from the committed table.
- Set the format-specific descriptor classes below; use `SerDeInfo` with its
  `serialization_lib`, not a plain row-schema-only registration.

| Format | Input format / output format / SerDe |
| --- | --- |
| Paimon | `org.apache.paimon.hive.mapred.PaimonInputFormat` / `org.apache.paimon.hive.mapred.PaimonOutputFormat` / `org.apache.paimon.hive.PaimonSerDe` |
| Iceberg | `org.apache.iceberg.mr.hive.HiveIcebergInputFormat` / `org.apache.iceberg.mr.hive.HiveIcebergOutputFormat` / `org.apache.iceberg.mr.hive.HiveIcebergSerDe` |

Read back DLF `GetTable` and compare the descriptor location, schema and
parameters with the native table. Native file creation without DLF registration,
or DLF registration without format metadata, does not complete the fixture.

## 2. Write REST Paimon

Configure `CatalogFactory.create` for the target REST catalog:

```python
properties = {
    "metastore": "rest", "uri": paimon_rest_uri, "warehouse": rest_catalog_name,
    "token.provider": "dlf", "dlf.region": region,
    "dlf.access-key-id": access_key, "dlf.access-key-secret": secret_key,
    "dlf.oss-endpoint": oss_endpoint,
}
```

Create `new_dlf_paimon_db` and `user_samples`, refusing existing objects. Use
explicit Arrow `int32` for user_id/age, string for gender and bool for is_active;
create a Paimon schema from it with the same bucket/format/retention options as
above. Use one batch commit for the exact rows in the contract. Reload and scan
with a new native read builder before accepting the write as complete.

## 3. Write REST Iceberg

With the tested DLF-enabled PyIceberg runtime, configure `load_catalog` with:

```python
properties = {
    "type": "rest", "uri": iceberg_rest_uri, "warehouse": rest_catalog_name,
    "rest.signing-name": "DlfNext", "rest.signing-region": region,
    "rest.sigv4-enabled": "true", "client.region": region,
    "client.access-key-id": access_key, "client.secret-access-key": secret_key,
}
```

Create `test_iceberg_db.test_iceberg_table` with nullable `k1`/`k2` field IDs 1/2
and IntegerType/StringType, format version 2 and PARQUET. Append one Arrow batch,
explicitly typing k1 as int32. Reload and verify both rows and
`table.inspect.files()['file_format'].to_pylist() == ['PARQUET']`. Multiple appends
or a different writer layout can produce multiple files even with two visible
rows, which violates this suite's fixture contract.

## 4. Configure Doris and verify

Use `enablePaimonTest=true` and `enableIcebergTest=true`. Put target credentials
in the existing private `dlf_access_key` / `dlf_secret_key` settings, and set:

| Key | Purpose |
| --- | --- |
| `dlf_uid`, `dlf_region`, `dlf_catalog_id` | Legacy catalog owner, region and target catalog ID |
| `dlf_vpc_endpoint`, `dlf_public_endpoint`, `s3Endpoint` | Regional Legacy DLF endpoints and OSS endpoint used by parameter-validation cases |
| `dlfWarehouse` | Legacy OSS warehouse; old default is `oss://selectdb-qa-datalake-test/p2_regression_case` |
| `paimonDlfRestCatalog` | Paimon catalog property string: type, REST URI, target warehouse, DLF token provider and credentials |
| `icebergDlfRestCatalog` | Iceberg catalog property string: type, REST URI, target warehouse, SigV4 signing and credentials |

The Beijing REST URI shape exercised was
`https://cn-beijing-vpc.dlf.aliyuncs.com`; Iceberg used the additional `/iceberg`
suffix. Use the target service's actual endpoints. The Doris Iceberg properties
include `iceberg.rest.signing-name=DlfNext`, signing-region, vended credentials,
`io-impl=org.apache.iceberg.rest.DlfFileIO` and
`fs.oss.support=true`; the SDK examples above use their own client's key names.
Do not copy client-specific credential keys between libraries without checking.

Run the four Legacy cases and three REST cases explicitly:

```bash
./run-regression-test.sh --run --conf /path/to/private-regression-conf.groovy \
  -g p2 -d external_table_p2/paimon,external_table_p2/iceberg \
  -s test_paimon_dlf_catalog,test_paimon_dlf_catalog_new_param,test_paimon_dlf_catalog_miss_dlf_param,test_iceberg_dlf_catalog,test_paimon_dlf_rest_catalog,test_paimon_rest_catalog_write,test_iceberg_dlf_rest_catalog \
  -parallel 1
```

Verify latest rows and `FOR VERSION AS OF 1` for Legacy Paimon, and inspect the
Iceberg files table. Record native metadata locations, snapshots and row/file
readback beside the Doris suite results. Retain service policies and warehouse
mappings in the private migration record, not in these reusable recipes.
