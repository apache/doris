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

For ClickHouse or SQL Server, set the corresponding `clickhouse.integration.*` or
`sqlserver.integration.*` properties (`url`, `driverJar`, `user`, `password`). Those tests
only execute constant `SELECT` queries. Omitted URLs disable the corresponding tests.
For ClickHouse, test both the v1 and v2 implementations where the driver offers them
(`clickhouse.jdbc.v1=true` selects v1 in the 0.9 driver). For SQL Server, include a legacy
driver without JDBC 4.2 typed timestamp support as well as a current driver.

MySQL execution sessions are explicitly set to UTC on each connection checkout. Both
timestamp binding and retrieval use a UTC Calendar, independently of Connector/J's
configured timezone. Session-sensitive expressions in remote SQL consequently execute
in UTC; unzoned `DATETIME` columns retain their wall-clock fields when read.
