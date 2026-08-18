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
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPath;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Column evolution against a REAL Paimon catalog — the offline
 * {@link PaimonConnectorMetadataColumnEvolutionTest} pins which {@code SchemaChange}es are built, this one
 * proves Paimon actually applies them.
 *
 * <p>Skipped unless {@code PAIMON_WAREHOUSE} is set (mirrors {@link PaimonLiveConnectivityTest}); point it
 * at a scratch filesystem warehouse, e.g.:
 *
 * <pre>
 * PAIMON_WAREHOUSE=/tmp/paimon-live-wh mvn test -pl fe-connector/fe-connector-paimon \
 *     -Dtest=PaimonLiveColumnEvolutionTest
 * </pre>
 *
 * <p>Every assertion reads the schema BACK through the connector's own read path, so a change that was
 * built correctly but rejected (or silently ignored) by Paimon fails here — which the offline test, whose
 * seam records the call and returns, cannot catch.
 *
 * <p>The test drops and recreates its own table, so it is safe to re-run against the same warehouse.
 */
public class PaimonLiveColumnEvolutionTest {

    private static final String DB = "paimon_live_evo_db";
    private static final String TBL = "live_evo";

    private static ConnectorContext testContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "paimon_live_evo";
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

    /** The table's current column names, read back through the connector's own schema path. */
    private static List<String> columnNames(ConnectorMetadata metadata, ConnectorTableHandle handle) {
        ConnectorTableSchema schema = metadata.getTableSchema(null, handle);
        return schema.getColumns().stream().map(ConnectorColumn::getName).collect(Collectors.toList());
    }

    private static ConnectorColumn findColumn(ConnectorMetadata metadata, ConnectorTableHandle handle,
            String name) {
        return metadata.getTableSchema(null, handle).getColumns().stream()
                .filter(c -> c.getName().equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("column not found after ALTER: " + name));
    }

    /** Re-resolves the handle so the read path sees the post-ALTER schema id, not a cached one. */
    private static ConnectorTableHandle handle(ConnectorMetadata metadata) {
        return metadata.getTableHandle(null, DB, TBL)
                .orElseThrow(() -> new AssertionError("table disappeared: " + DB + "." + TBL));
    }

    @Test
    public void liveColumnEvolutionRoundTrip() {
        String warehouse = System.getenv("PAIMON_WAREHOUSE");
        Assumptions.assumeTrue(warehouse != null && !warehouse.isEmpty(),
                "skipped: set PAIMON_WAREHOUSE to run live column evolution");

        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);
        String catalogType = System.getenv("PAIMON_CATALOG_TYPE");
        if (catalogType != null && !catalogType.isEmpty()) {
            props.put(PaimonCatalogProperties.PAIMON_CATALOG_TYPE, catalogType);
        }

        try (PaimonConnector connector = new PaimonConnector(props, testContext())) {
            ConnectorMetadata metadata = connector.getMetadata(null);

            // ---- fixture: a scratch table with a known 3-column shape ----
            try {
                metadata.dropTable(null, handle(metadata));
            } catch (Throwable ignored) {
                // first run: nothing to drop
            }
            try {
                metadata.createDatabase(null, DB, Collections.emptyMap());
            } catch (Throwable ignored) {
                // db may already exist from a previous run
            }
            metadata.createTable(null, ConnectorCreateTableRequest.builder()
                    .dbName(DB)
                    .tableName(TBL)
                    .columns(Arrays.asList(
                            column("id", "INT"), column("score", "INT"), column("note", "STRING")))
                    .properties(Collections.emptyMap())
                    .build());
            Assertions.assertEquals(Arrays.asList("id", "score", "note"), columnNames(metadata, handle(metadata)));

            // ---- ADD COLUMN (append) ----
            metadata.addColumn(null, handle(metadata), column("extra", "STRING"), null);
            Assertions.assertEquals(Arrays.asList("id", "score", "note", "extra"),
                    columnNames(metadata, handle(metadata)),
                    "a null position must append at the end");

            // ---- ADD COLUMN FIRST / AFTER: position must be honored by Paimon, not just requested ----
            metadata.addColumn(null, handle(metadata), column("lead_col", "INT"),
                    ConnectorColumnPosition.FIRST);
            Assertions.assertEquals("lead_col", columnNames(metadata, handle(metadata)).get(0));

            metadata.addColumn(null, handle(metadata), column("mid_col", "INT"),
                    ConnectorColumnPosition.after("score"));
            List<String> afterPositions = columnNames(metadata, handle(metadata));
            Assertions.assertEquals(afterPositions.indexOf("score") + 1, afterPositions.indexOf("mid_col"),
                    "AFTER must place the column directly behind its anchor");

            // ---- ADD COLUMNS (batch, one commit) ----
            metadata.addColumns(null, handle(metadata),
                    Arrays.asList(column("batch_a", "INT"), column("batch_b", "STRING")));
            Assertions.assertTrue(columnNames(metadata, handle(metadata)).containsAll(
                    Arrays.asList("batch_a", "batch_b")));

            // ---- RENAME COLUMN ----
            metadata.renameColumn(null, handle(metadata), "extra", "extra_renamed");
            List<String> afterRename = columnNames(metadata, handle(metadata));
            Assertions.assertTrue(afterRename.contains("extra_renamed"));
            Assertions.assertFalse(afterRename.contains("extra"), "the old name must be gone");

            // ---- MODIFY COLUMN: type widening ----
            metadata.modifyColumn(null, handle(metadata), column("score", "BIGINT"), null);
            Assertions.assertEquals("BIGINT",
                    findColumn(metadata, handle(metadata), "score").getType().getTypeName().toUpperCase(),
                    "int -> bigint widening must be applied");

            // ---- MODIFY COLUMN COMMENT (sole entrypoint, flat path) ----
            metadata.modifyColumnComment(null, handle(metadata), ConnectorColumnPath.of("note"),
                    "the note column");
            Assertions.assertEquals("the note column",
                    findColumn(metadata, handle(metadata), "note").getComment());

            // ---- REORDER COLUMNS ----
            List<String> current = columnNames(metadata, handle(metadata));
            List<String> reversed = new java.util.ArrayList<>(current);
            Collections.reverse(reversed);
            metadata.reorderColumns(null, handle(metadata), reversed);
            Assertions.assertEquals(reversed, columnNames(metadata, handle(metadata)),
                    "the chained FIRST/AFTER moves must produce exactly the requested order");

            // ---- DROP COLUMN ----
            metadata.dropColumn(null, handle(metadata), "mid_col");
            Assertions.assertFalse(columnNames(metadata, handle(metadata)).contains("mid_col"));

            // ---- rejection: a type Paimon cannot represent must fail BEFORE touching the table ----
            List<String> beforeReject = columnNames(metadata, handle(metadata));
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.addColumn(null, handle(metadata), column("bad", "TIME"), null));
            Assertions.assertEquals(beforeReject, columnNames(metadata, handle(metadata)),
                    "a rejected ALTER must not have partially applied");

            // ---- cleanup ----
            metadata.dropTable(null, handle(metadata));
        } catch (Exception e) {
            throw new AssertionError("live column evolution failed for warehouse " + warehouse, e);
        }
    }
}
