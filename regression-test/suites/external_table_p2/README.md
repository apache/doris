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

# External P2 fixture configuration

Run these suites with `run-regression-test.sh --run --conf <private-config>` against
a Doris cluster that can reach the external services. The regression runner can
run on a different machine from the FE and BE. Keep credentials in the private
configuration or its environment, outside version control.

The MaxCompute suites accept these optional configuration keys:

| Key | Purpose | Default |
| --- | --- | --- |
| `mcDefaultProject` | Read fixtures and generated tables in the base project | `mc_datalake` |
| `mcCrossProject` | Cross-project read fixtures | `other_mc_datalake_test` |
| `mcSchemaProject` | Read fixtures in `default`, `analytics` and `iot` schemas | `mc_datalake_schema` |
| `mcWriteProject` | Schemas and tables created by the write suites | `mc_doris_test_write` |
| `mcEndpoint` | MaxCompute service endpoint reachable by Doris | Existing suite endpoint |

Set `enableMaxComputeTest=true` and supply `ak` and `sk`. The write and schema
projects need project-level Schema support. The `pay-as-you-go` Storage API
resource must be enabled, and the test user needs its quota usage permission in
addition to the project permissions. The write suites create and drop test data;
use dedicated projects. The large-data suite generates two million rows.

Legacy DLF Paimon and Iceberg suites accept `dlfWarehouse`, defaulting to
`oss://selectdb-qa-datalake-test/p2_regression_case`. Set it together with the
existing `dlf_catalog_id`, region and credential settings for another fixture
catalog. Their preloaded tables are `regression_paimon.tb_simple` and
`regression_iceberg.tb_simple`; Paimon's version-1 snapshot must remain available.

DLF REST suites use the existing `paimonDlfRestCatalog` and
`icebergDlfRestCatalog` property strings. Their preloaded tables are
`new_dlf_paimon_db.user_samples` and `test_iceberg_db.test_iceberg_table`.
The REST write suites create their own tables. Enable the corresponding
`enablePaimonTest` or `enableIcebergTest` flag when selecting these suites.

`oss_hdfs_catalog_test` needs actual OSS-HDFS storage and its separately
configured Paimon query tables. An ordinary OSS warehouse does not replace
that fixture.

EMR migration suites use `emrCatalogCommonProp` for the HMS and Hadoop properties.
`test_hive_migrate_paimon` also accepts `emrPaimonWarehouse`, preserving the original
warehouse when unset. See the [initialization recipes](../../../tools/emr_storage_regression/p2_fixtures/README.md)
for the four Iceberg and two Paimon tables, including the required migration and
schema history. That directory also contains a deterministic generator for the
ORC stream case and its Hive table registration SQL.

See [DEPENDENCIES.md](DEPENDENCIES.md) for the remaining external services,
preloaded fixtures, bootstrap sources and disabled suites.
