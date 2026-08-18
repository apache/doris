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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;

import org.apache.paimon.CoreOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The row-level DML shape gate against a REAL Paimon catalog.
 *
 * <p>{@link PaimonRowLevelDeleteTest} pins the gate's logic against a hand-written fake whose
 * {@code primaryKeys()} / {@code options()} return whatever the test sets. This one builds the three table
 * shapes FOR REAL and lets the gate read them back through Paimon's own metadata — so a table that Paimon
 * does not actually create the way we assumed (e.g. an option that does not survive CREATE TABLE) fails
 * here rather than silently making the gate decide on values no real table ever has.
 *
 * <p>Skipped unless {@code PAIMON_WAREHOUSE} is set (mirrors {@link PaimonLiveConnectivityTest}).
 */
public class PaimonLiveRowLevelDmlGateTest {

    private static final String DB = "paimon_live_dml_db";

    private static ConnectorContext testContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "paimon_live_dml";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return Collections.emptyMap();
            }
        };
    }

    private static ConnectorColumn column(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), null, true, null);
    }

    /** Creates (or recreates) a table with the given properties and returns its handle. */
    private static ConnectorTableHandle createTable(ConnectorMetadata metadata, String table,
            Map<String, String> properties) {
        metadata.getTableHandle(null, DB, table).ifPresent(h -> metadata.dropTable(null, h));
        metadata.createTable(null, ConnectorCreateTableRequest.builder()
                .dbName(DB)
                .tableName(table)
                .columns(Arrays.asList(column("id", "INT"), column("note", "STRING")))
                .properties(properties)
                .build());
        return metadata.getTableHandle(null, DB, table)
                .orElseThrow(() -> new AssertionError("table not created: " + DB + "." + table));
    }

    @Test
    public void liveShapeGateAcceptsAndRejectsTheRightTables() {
        String warehouse = System.getenv("PAIMON_WAREHOUSE");
        Assumptions.assumeTrue(warehouse != null && !warehouse.isEmpty(),
                "skipped: set PAIMON_WAREHOUSE to run the live row-level DML gate test");

        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);
        String catalogType = System.getenv("PAIMON_CATALOG_TYPE");
        if (catalogType != null && !catalogType.isEmpty()) {
            props.put(PaimonCatalogProperties.PAIMON_CATALOG_TYPE, catalogType);
        }

        try (PaimonConnector connector = new PaimonConnector(props, testContext())) {
            ConnectorMetadata metadata = connector.getMetadata(null);
            try {
                metadata.createDatabase(null, DB, Collections.emptyMap());
            } catch (Throwable ignored) {
                // db may already exist from a previous run
            }

            // ---- shape 1: primary-key table -> all three ops allowed, no option needed ----
            Map<String, String> pkProps = new HashMap<>();
            pkProps.put("primary-key", "id");
            pkProps.put("bucket", "1");
            ConnectorTableHandle pkHandle = createTable(metadata, "live_pk", pkProps);
            for (WriteOperation op : new WriteOperation[] {
                    WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE}) {
                Assertions.assertDoesNotThrow(
                        () -> metadata.validateRowLevelDmlMode(null, pkHandle, op),
                        op + " must be allowed on a real primary-key table");
            }

            // ---- shape 2: unaware-bucket append WITH deletion vectors -> DELETE allowed ----
            Map<String, String> dvProps = new HashMap<>();
            dvProps.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
            ConnectorTableHandle dvHandle = createTable(metadata, "live_dv", dvProps);
            Assertions.assertDoesNotThrow(
                    () -> metadata.validateRowLevelDmlMode(null, dvHandle, WriteOperation.DELETE),
                    "DELETE must be allowed on a real unaware-bucket deletion-vector table");
            for (WriteOperation op : new WriteOperation[] {
                    WriteOperation.UPDATE, WriteOperation.MERGE}) {
                DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                        () -> metadata.validateRowLevelDmlMode(null, dvHandle, op),
                        op + " must stay rejected on an append-only table");
                Assertions.assertTrue(ex.getMessage().contains("Only DELETE is supported"),
                        ex.getMessage());
            }

            // ---- shape 3: plain append-only -> DELETE rejected with the option that fixes it ----
            Map<String, String> plainProps = new HashMap<>();
            ConnectorTableHandle plainHandle = createTable(metadata, "live_plain", plainProps);
            DorisConnectorException noDv = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.validateRowLevelDmlMode(null, plainHandle, WriteOperation.DELETE),
                    "DELETE must be rejected on a real plain append-only table");
            Assertions.assertTrue(
                    noDv.getMessage().contains(CoreOptions.DELETION_VECTORS_ENABLED.key()),
                    "the rejection must name the option that enables it: " + noDv.getMessage());

            // ---- shape 4: bucketed append (pinned bucket count) -> rejected even with vectors ----
            // The vector must be filed under the file's REAL bucket, which the locator does not carry.
            Map<String, String> bucketedProps = new HashMap<>();
            bucketedProps.put("bucket", "2");
            bucketedProps.put("bucket-key", "id");
            bucketedProps.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
            ConnectorTableHandle bucketedHandle = createTable(metadata, "live_bucketed", bucketedProps);
            DorisConnectorException pinned = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.validateRowLevelDmlMode(null, bucketedHandle, WriteOperation.DELETE));
            Assertions.assertTrue(pinned.getMessage().contains("unaware-bucket"), pinned.getMessage());

            // ---- a plain INSERT/OVERWRITE into that same table must still be fine ----
            Assertions.assertDoesNotThrow(
                    () -> metadata.validateRowLevelDmlMode(null, plainHandle, WriteOperation.INSERT),
                    "the gate must fire ONLY for row-level DML");

            // ---- cleanup ----
            for (String table : new String[] {"live_pk", "live_dv", "live_plain", "live_bucketed"}) {
                metadata.getTableHandle(null, DB, table).ifPresent(h -> metadata.dropTable(null, h));
            }
        } catch (Exception e) {
            throw new AssertionError("live row-level DML gate test failed for warehouse " + warehouse, e);
        }
    }
}
