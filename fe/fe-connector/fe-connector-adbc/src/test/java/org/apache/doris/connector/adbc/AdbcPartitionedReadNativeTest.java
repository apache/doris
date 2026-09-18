// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.arrow.adbc.core.AdbcStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Puts the partitioned-read downgrade in front of a real driver.
 *
 * <p>The unit tests script a driver that answers {@code NOT_IMPLEMENTED}, which proves the connector reacts
 * correctly to that status -- but the status itself is an assumption those tests cannot check. SQLite is a
 * driver that genuinely has no partitioned execution, so this is the one place that shows the connector
 * recognizes a real refusal, arriving through the real JNI bridge and C driver manager, rather than a
 * refusal shaped the way the test author expected.
 *
 * <p>The other half -- a driver that DOES partition -- needs a live Flight SQL source and lives in the
 * regression suite, not here.
 *
 * <p>Skips loudly without the native libraries; a skipped run has verified nothing.
 */
class AdbcPartitionedReadNativeTest {

    private static final AdbcTableHandle T1 =
            new AdbcTableHandle(new AdbcNamespace("main", ""), "t1");

    private static AdbcClient sqliteClient(Path dbFile) {
        return new AdbcClient(AdbcNativeTestSupport.sqliteDriver(), "libadbc_driver_sqlite.so",
                null, "file:" + dbFile, null, null, Map.of());
    }

    private static void seed(AdbcClient client) {
        client.withConnection(connection -> {
            for (String sql : new String[] {
                    "CREATE TABLE t1 (id INTEGER, name TEXT)",
                    "INSERT INTO t1 VALUES (1, 'a')",
                    "INSERT INTO t1 VALUES (2, 'b')"}) {
                try (AdbcStatement statement = connection.createStatement()) {
                    statement.setSqlQuery(sql);
                    statement.executeUpdate();
                }
            }
            return null;
        });
    }

    @Test
    void realDriverWithoutPartitionedExecutionPlansOneStatementInstead(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("partition.db"))) {
            seed(client);
            AdbcPartitionedReadSupport support = new AdbcPartitionedReadSupport();
            AdbcScanPlanProvider planner = new AdbcScanPlanProvider(
                    AdbcCatalogProperties.of(Map.of(
                            AdbcCatalogProperties.URI, "file:" + tempDir.resolve("partition.db"),
                            AdbcCatalogProperties.DRIVER_URL,
                            AdbcNativeTestSupport.sqliteDriver().toString())),
                    AdbcNativeTestSupport.sqliteDriver(),
                    new AdbcDialectSelector(AnsiDialect.NAME),
                    () -> client, support);
            List<ConnectorColumnHandle> columns = List.of(new NamedColumnHandle("id"));

            List<ConnectorScanRange> ranges = planner.planScan(null,
                    ConnectorScanRequest.builder(T1, columns).build());

            Assertions.assertEquals(1, ranges.size());
            TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
            ranges.get(0).populateRangeParams(formatDesc, new TFileRangeDesc());
            Assertions.assertEquals("SELECT \"id\" FROM \"main\".\"t1\"",
                    formatDesc.getAdbcParams().get("query_sql"));
            // The refusal was recognized as "this driver cannot", not merely as some failure, so the next
            // scan of this catalog goes straight to a statement.
            Assertions.assertTrue(support.isKnownUnsupported(),
                    "a real NOT_IMPLEMENTED must be remembered, or every scan pays for the round trip");
        }
    }
}
