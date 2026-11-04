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

# Timestamp integration tests

The regular unit tests verify the scanner's UTC contract without an external service.
The opt-in integration tests exercise real JDBC drivers and servers, including JVM/session
timezone mismatches, nulls, microseconds, and both instants in a daylight-saving overlap.
They restore the JVM default timezone after each test. Run them without parallel test execution.

From `fe/`, run:

```bash
mvn -pl be-java-extensions/jdbc-scanner -am test \
  -Dtest=MySqlTimestampIntegrationTest,JdbcZonedTimestampIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dmysql.integration.url='jdbc:mysql://localhost:3306/test?useSSL=false' \
  -Dmysql.integration.driverJar=/path/to/mysql-driver.jar \
  -Dmysql.integration.user=test_user \
  -Dmysql.integration.password=TEST_PASSWORD
```

MySQL needs an existing test database and permission to create temporary tables. Each
connection creates only a temporary table, which is removed when the connection closes.
The test covers client/server prepared statements, three JVM timezones, two initial
session timezones, and four Connector/J timezone configurations.
Set `mysql.integration.tableType=OCEANBASE` to exercise the OceanBase MySQL-mode executor
branch against the same wire protocol. This does not replace testing an OceanBase server.

For ClickHouse, SQL Server, PostgreSQL, Oracle, Trino, or PrestoSQL, set the corresponding
`clickhouse.integration.*`, `sqlserver.integration.*`, `postgresql.integration.*`,
`oracle.integration.*`, `trino.integration.*`, or `presto.integration.*` properties (`url`,
`driverJar`, `user`, `password`). Omitted URLs disable the corresponding tests. PostgreSQL
also varies the session timezone and reads nested timestamp arrays. SQL Server creates a
connection-local temporary table for writes.

The other write round trips require CREATE/INSERT/SELECT/DROP privileges in a test schema.
They create a unique table and drop it in `finally`, exercising microseconds, negative epochs,
midnight, NULL, and both DST-fold instants across three JVM zones. Oracle covers both TZ and
LOCAL TIME ZONE columns; ClickHouse uses explicit Tokyo and Los Angeles column zones. Use
ClickHouse JDBC V2 for these instant write round trips. Trino/PrestoSQL URLs must name a
writable catalog/schema supporting TIMESTAMP(6) WITH TIME ZONE, such as a memory catalog.
The PrestoSQL test loads `io.prestosql.jdbc.PrestoDriver`.
The ClickHouse read tests can also exercise v1 where the driver offers it
(`clickhouse.jdbc.v1=true` selects v1 in the 0.9 driver). For SQL Server, include 6.2, 6.4,
7.0 and a current driver: unsupported typed getters throw different exception classes
across legacy releases. Reads resolve the stored offset with `getTimestamp`; writes bind
an explicit UTC ISO timestamp, avoiding `Timestamp` parameters that lose their offset.

MySQL execution sessions are explicitly set to UTC on each connection checkout. Both
timestamp binding and retrieval use the UTC session's civil fields, avoiding legacy
Connector/J Calendar conversions and the JVM default timezone. Session-sensitive expressions in remote SQL consequently execute
in UTC; unzoned `DATETIME` columns retain their wall-clock fields when read.
