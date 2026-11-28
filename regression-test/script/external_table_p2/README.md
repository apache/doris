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

# Build and migrate external P2 datasets

This is the entry point for an operator or agent preparing external P2 data in a
new account or cluster. Initialization belongs under `regression-test/script`,
outside `suites`: its SQL runs in Hive, Spark or MaxCompute, not in Doris. Keep
service addresses, account IDs, secrets and CI deployment instructions in a
private configuration outside version control.

## 1. Establish the source and target

Before writing data, record the Doris source commit, the suites to run, the target
account/region, project and catalog names, warehouse prefixes, and the FE/BE build
that will read them. Inspect each suite's enable flags and skip guards. A suite
that returns before assertions does not establish coverage, even if the runner
reports success. Use [dependencies.md](dependencies.md) to identify services and
fixtures outside `suites/external_table_p2` too.

Classify each fixture before selecting its migration method:

| Fixture | Required method | Acceptance evidence |
| --- | --- | --- |
| MaxCompute read tables | Recreate DDL and literal inserts from suite seed comments | All 28 tables, partitions, type/NULL boundaries and nanosecond timestamps |
| Legacy/REST DLF | Create format-native tables and register their actual metadata | Four exact schemas/row sets, Paimon snapshot 1 and the expected Iceberg data file |
| Hive→Iceberg/Paimon migrations | Create Hive files, migrate, append and evolve in order | Original Hive files plus later files, schema history and snapshot reads |
| ORC merged streams | Generate the prescribed stripe/row-group layout | Predicate rejects every row group after stripe stats pass; Profile proves merged IO |
| Iceberg equality deletes | Preserve the complete table and metadata | Data, position-delete and equality-delete files and unchanged query results |
| Hive compression | Restore the published bootstrap archive | File count, byte count, partitions, query counts and splitting behavior |
| Self-writing suites | Provision service, namespace and permissions | Execute the existing suite; no preloaded row copy is needed |

Use fresh dedicated targets. The initialization SQL and ORC generator refuse
existing tables/files. A partial failure is not a reason to drop the target or
rerun every insert: inspect what committed and resume from the failed operation.
Keep native job/commit IDs and a statement journal to prevent duplicate rows.

## 2. Prepare services and credentials

The regression runner connects to Doris SQL and FE HTTP. FE/BE processes connect
to external services and storage. Verify connectivity from the actual FE/BE
network, including DataNodes and KDC for Kerberos, rather than only the runner.
Deploy required realm/keytab files with the service user's permissions on every
FE/BE that can read the tables. Keep HTTP authentication enabled for Profile reads.

Provision the target services using the provider's APIs or console, and read back
resource ownership, region, locations and permissions before loading data. Use
provider-native SDKs or Spark/Hive to build fixtures; no Doris production code is
needed. Supply credentials through a private config or SDK credential provider.

## 3. Construct data in this order

1. [MaxCompute](maxcompute/README.md): allocate four project roles, extract the
   current suite seeds, load the base/cross/schema projects, and verify Storage API.
2. [DLF](dlf/README.md): allocate separate Legacy and REST catalogs, write native
   Paimon/Iceberg fixtures and verify catalog registration and history.
3. [EMR/HMS](emr/README.md): run the Hive source SQL once, migrate each format,
   apply the prescribed appends/renames, then generate/register the ORC fixture.
4. Restore physical-layout fixtures and initialize other services using their
   existing [bootstrap sources](dependencies.md). Preserve complete metadata when
   a fixture tests snapshots, deletes or file layout.

The first three families have reproducible seed contracts here. The dependency
inventory links other bootstrap sources and identifies assets that still need
preservation or service-specific preparation; it does not promise a universal
initializer for every external P2 suite.

## 4. Verify through the native service, then Doris

First inspect native schemas, partitions, rows and format history. Read back newly
created objects; for copied files retain checksums and object lists. A table that
returns the right final rows can still lose migration, snapshot or delete coverage.
Then run the corresponding Doris suites using the existing golden files:

```bash
./run-regression-test.sh --run --conf /path/to/private-regression-conf.groovy \
  -g p2 -d external_table_p2/iceberg,external_table_p2/paimon \
  -s test_hive_migrate_iceberg,test_hive_migrate_partition_iceberg,test_hive_migrate_paimon \
  -parallel 1
```

The private configuration must allow P2; an `excludeGroups` containing `p2` still
excludes explicitly named suites. Enable each selected family. Start with one
suite and one build at a time, since write suites reuse table/schema names. Other
families have their exact enable keys and suite names in the linked recipes.

Check actual SQL/golden execution as well as nonzero suite counts and zero
failed/fatal/skipped summaries. The ORC case additionally verifies its Profile
path and disables condition cache. A different file layout, missing schema or
failed authentication must be resolved before changing expected results.

## 5. Leave a migration record for the next operator or agent

Store this record outside the repository, with secrets omitted:

- Source and target account/region and an explicit old→new mapping for projects,
  catalogs, namespaces, warehouse URIs and credential references.
- Source commit and seed-source hashes; generated SQL hashes; installed SDK,
  Hive/Spark and format runtime versions.
- Each completed statement's native job/commit ID, schema context and status;
  the failed operation and a concrete resume point for partial execution.
- Table schemas, partition lists, row counts, snapshot/schema IDs, metadata
  locations, file formats, file/object lists and checksums where layout matters.
- Native readback results and the exact Doris build, suite selection, results
  and relevant Profile counters. Distinguish missing services from assertion
  failures and deliberately disabled suites.

Keep the original source fixtures until target validation is complete. Update the
private regression config with target mappings, keep deployment-specific service
addresses in configuration, and keep account-specific CI deployment files outside the PR.
