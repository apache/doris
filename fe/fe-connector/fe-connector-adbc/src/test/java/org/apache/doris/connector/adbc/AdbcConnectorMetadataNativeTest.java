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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.arrow.adbc.core.AdbcStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

/**
 * The two metadata cases that need a real driver but never read a result: a missing table must not be
 * reported as a driver gap, and the table descriptor handed to the scan path must be typed.
 *
 * <p>Both stop before any Arrow data is materialized -- the first ends in a thrown exception, the second
 * never asks the source -- which is what makes them safe to run in FE UT. See {@link AdbcNativeTestSupport}
 * for the rule: a test that iterates an {@code ArrowReader} also needs arrow-c-data's JNI shim, which is an
 * upstream binary that cannot load on every host Doris supports, and those tests belong in the regression
 * suites instead. The rest of the metadata surface -- listings, schema mapping, views, handles -- is
 * asserted end to end by {@code regression-test/suites/external_table_p0/adbc}, which is where it can be.
 */
class AdbcConnectorMetadataNativeTest {

    private static AdbcClient sqliteClient(Path dbFile) {
        return new AdbcClient(AdbcNativeTestSupport.sqliteDriver(), "libadbc_driver_sqlite.so",
                null, "file:" + dbFile, null, null, Map.of());
    }

    /**
     * A fresh cache per call, so each test reads the source rather than an earlier test's answers.
     */
    private static AdbcConnectorMetadata metadataOn(AdbcClient client) {
        return new AdbcConnectorMetadata(client, new AdbcSchemaStrategy(),
                AdbcDialectRegistry::defaultDialect, new AdbcMetadataCache(Map.of()));
    }

    private static void seed(AdbcClient client) {
        client.withConnection(connection -> {
            for (String sql : new String[] {
                    "CREATE TABLE IF NOT EXISTS t1 (c_int INTEGER, c_dbl REAL, c_txt TEXT, c_blob BLOB)",
                    "INSERT INTO t1 VALUES (1, 1.5, 'a', x'00ff')",
                    "CREATE TABLE IF NOT EXISTS t2 (a INTEGER)",
                    "CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t1"}) {
                try (AdbcStatement statement = connection.createStatement()) {
                    statement.setSqlQuery(sql);
                    statement.executeUpdate();
                }
            }
            return null;
        });
    }

    @Test
    void missingTableIsReportedAsSuchNotAsADriverGap(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);
            AdbcConnectorMetadata metadata = metadataOn(client);
            // Build a handle for a table that does not exist, bypassing getTableHandle's existence check.
            AdbcTableHandle ghost = new AdbcTableHandle(new AdbcNamespace("main", ""), "no_such_table");

            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.getTableSchema(null, ghost));

            // The fallback to executeSchema must fire only on NOT_IMPLEMENTED. Falling back on every error
            // would answer a plain missing table with "this driver implements neither method", sending the
            // user to look at their driver instead of their table name.
            Assertions.assertTrue(e.getMessage().contains("no_such_table"), e.getMessage());
            Assertions.assertFalse(e.getMessage().contains("implements neither"), e.getMessage());
        }
    }

    @Test
    void tableDescriptorIsTypedForTheScanPath(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);

            TTableDescriptor descriptor = metadataOn(client)
                    .buildTableDescriptor(null, 7L, "t1", "main", "t1", 4, 42L);

            // Returning null (the SPI default) would let fe-core fall back to SCHEMA_TABLE, and BE would
            // then build a SchemaTableDescriptor rather than the one the file-scan path expects.
            Assertions.assertEquals(TTableType.HIVE_TABLE, descriptor.getTableType());
            Assertions.assertTrue(descriptor.isSetHiveTable());
        }
    }
}
