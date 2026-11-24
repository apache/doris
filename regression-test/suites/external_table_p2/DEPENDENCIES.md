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

# External P2 dependency inventory

A fixture is more than its visible rows when a suite checks migration, snapshots,
deletes, compression or file splitting. Preserve the matching metadata and file
layout. Other suites generate their own tables and only need a service and access
configuration. Check enable flags and unconditional skip guards before provisioning.

| Family | Required environment or preloaded data | Preparation |
| --- | --- | --- |
| EMR migration | Hive HMS/HDFS; four Iceberg and two Paimon tables | [Reproduce migration history](../../../tools/emr_storage_regression/p2_fixtures/README.md), including Hive source files and later appends |
| ORC stream regression | `regression.test_orc_merge_io_input_streams_table`, ZLIB file with consecutive stripes whose row groups all reject the target | Use `GenerateOrcMergeFixture.java` and `04-orc-merge-table.sql`; the suite checks the actual row-group and merged-IO path through its Profile |
| MaxCompute | Read projects, schema project, write project and Storage API quota access | See [configuration](README.md); write suites create their own tables |
| DLF | Legacy and REST metadata plus OSS warehouses | See [configuration](README.md); preserve Paimon snapshot 1 and Iceberg metadata |
| Iceberg equality deletes | `flink_db.sample` with data, position-delete and equality-delete files | Preserve the complete Iceberg table, including manifests and delete files |
| Hive compression | Hive 3 HMS/HDFS; `multi_catalog.test_compress_partitioned` | Use the published bootstrap archive; preserve 16 files, 734,675,596 bytes and eight partitions |
| Hudi | Docker Hive/HDFS and generated Hudi tables | Retain the Docker suites and their generator data; obsolete disabled EMR suites are removed |
| Elasticsearch | ES 5/6/7/8 and version-specific mappings/documents | Replay repository scripts and JSON under `docker/thirdparties/docker-compose/elasticsearch/` |
| JDBC and CDC | DB2, OceanBase, SQLServer and matching drivers; OceanBase CDC service for CDC suites | Reuse thirdparties initialization scripts; many cases create rows themselves |
| Trino connector | Doris embedded connector artifacts plus Hive TPC-H ORC/Parquet or SQLServer fixtures | Hive suites read eight tables in `tpch1_orc` and `tpch1_parquet`; these suites do not require a standalone Trino server |
| Paimon filesystem | OSS/COS/OBS warehouses with `db1.all_table`, including snapshot 1 | Preserve table metadata and history, not just Parquet or ORC files |
| Paimon HMS | HMS/HDFS and `external_test_table` in HDFS/OSS/OBS/COS/AWS/GCS namespaces | Include metastore registrations and the dedicated Kerberos fixture/keytabs; the writer suite creates its own tables in `hdfs_db` |
| Iceberg V3 row lineage | Docker Iceberg REST catalog and storage | Use the matching Docker bootstrap and Iceberg version |
| Glue and S3Tables | AWS Glue, S3Tables namespace/buckets and permissions | Active Glue-backed variants need AWS resources; Aliyun OSS does not exercise this path |
| Multi-cloud storage | AWS, COS, OBS, Azure Blob/ADLS and GCS access, endpoints and buckets | Storage parameter, TVF and export suites generally create their own files; preserve the transport-specific variants |
| OSS-HDFS | Actual OSS-HDFS service and configured Paimon query fixtures | Ordinary OSS is insufficient; see `refactor_catalog_param/oss_hdfs_catalog_test.groovy` |
| Ranger | Ranger service, policies and Hive fixtures | Row migration alone cannot provision authorization behavior |

## Reuse published bootstrap data

`docker/thirdparties/docker-compose/hive/scripts/prepare-hive-data.sh` downloads
from `https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data`.
The relevant archives include:

- `multi_catalog/test_compress_partitioned/data.tar.gz` for the compression case.
- `tpch1.db.tar.gz` for the Hive bootstrap's TPC-H fixtures.

Use the existing bootstrap's extraction, loading and format-conversion steps.
For compression, validate both physical files and query results: total row count
1,510,010; `dt IN ('gzip', 'mix')` count 600,005; watch ID 4611870011201662970
count 15. The suite also checks splitting behavior, so a row-count-only rebuild
is insufficient.

## Check before scheduling

The external-tagged P2 suites also occur outside this directory: `ranger_p2`,
`mtmv_p2` (ES), `export_p2`, `job_p0/streaming_job` (OceanBase CDC), and
`doc/external/jdbc`. Include these when mapping a pipeline's external dependencies.

Several legacy Hive/MySQL/PostgreSQL suites return under unconditional
`ignoreP2=true` guards. Three native S3Tables variants similarly use
`run_test=false`; the active alternatives use Glue-backed S3Tables. Do not count
those early returns as executed coverage or migrate data for them without first
deciding whether to re-enable them. The Docker Hudi suites remain enabled by
their own configuration and must not be confused with the removed EMR suites.
