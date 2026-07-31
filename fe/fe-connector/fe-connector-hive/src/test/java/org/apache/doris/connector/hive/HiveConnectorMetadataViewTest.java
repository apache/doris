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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.ConnectorViewDefinition;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tests the hive connector VIEW SPI (§4.2 read-side SPI): {@link HiveConnectorMetadata#viewExists},
 * {@link HiveConnectorMetadata#getViewDefinition}, {@link HiveConnectorMetadata#dropView}, the empty
 * {@code listViewNames} default, and {@link HiveConnector}'s {@code SUPPORTS_VIEW} declaration.
 *
 * <p>WHY these assertions matter:
 * <ul>
 *   <li>{@code viewExists} keys off the PRESENCE OF VIEW TEXT (legacy {@code HMSExternalTable.isView}), not the
 *       {@code tableType}. It drives both {@code PluginDrivenExternalTable.isView()} AND the unconditional
 *       {@code PluginDrivenExternalCatalog.dropTable -> viewExists -> dropView} routing, so it MUST return
 *       {@code false} for a base table — otherwise every hive DROP TABLE would misroute into dropView.</li>
 *   <li>{@code getViewDefinition} reproduces legacy {@code getViewText} bit-for-bit (expanded-first; skip the
 *       bare {@code /* Presto View *}{@code /} sentinel; else base64-decode the Presto/Trino {@code originalSql};
 *       raw-original fallback on any decode failure) and supplies the view's columns — fe-core builds a flipped
 *       view's schema SOLELY from here, so the column list must be non-empty.</li>
 *   <li>{@code listViewNames} MUST stay empty: hive {@code listTableNames} already includes views, and the
 *       fe-core SHOW TABLES merge is a plain {@code addAll} with no dedup, so a non-empty value double-lists.</li>
 * </ul>
 */
public class HiveConnectorMetadataViewTest {

    private static final String DB = "db";
    private static final String VIRTUAL_VIEW = "VIRTUAL_VIEW";

    private HiveConnectorMetadata metadataOf(FakeHmsClient client) {
        return new HiveConnectorMetadata(client, Collections.emptyMap(), new FakeConnectorContext());
    }

    private static ConnectorColumn col(String name, String typeName) {
        return new ConnectorColumn(name, ConnectorType.of(typeName), null, true, null);
    }

    /** A hive VIEW carrying a plain (non-Presto) expanded text and ordinary columns. */
    private static HmsTableInfo plainView(String name, String expandedText) {
        return HmsTableInfo.builder()
                .dbName(DB).tableName(name)
                .tableType(VIRTUAL_VIEW)
                .viewExpandedText(expandedText)
                .viewOriginalText(expandedText)
                .columns(Arrays.asList(col("id", "INT"), col("name", "STRING")))
                .parameters(Collections.emptyMap())
                .build();
    }

    /** A base table: no view text. */
    private static HmsTableInfo baseTable(String name) {
        return HmsTableInfo.builder()
                .dbName(DB).tableName(name)
                .tableType("MANAGED_TABLE")
                .columns(Arrays.asList(col("id", "INT")))
                .parameters(Collections.emptyMap())
                .build();
    }

    /** A Presto/Trino-authored hive view: sentinel expanded text + base64 JSON original text. */
    private static HmsTableInfo prestoView(String name, String originalSql) {
        String json = "{\"originalSql\":\"" + originalSql + "\",\"catalog\":\"hive\",\"schema\":\"db\"}";
        String base64 = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        return HmsTableInfo.builder()
                .dbName(DB).tableName(name)
                .tableType(VIRTUAL_VIEW)
                .viewExpandedText("/* Presto View */")
                .viewOriginalText("/* Presto View: " + base64 + " */")
                .columns(Arrays.asList(col("id", "INT")))
                .parameters(Collections.emptyMap())
                .build();
    }

    @Test
    public void viewExistsTrueForViewFalseForBaseTableAndMissing() {
        FakeHmsClient client = new FakeHmsClient();
        client.put(plainView("v", "SELECT 1"));
        client.put(baseTable("t"));
        HiveConnectorMetadata metadata = metadataOf(client);

        Assertions.assertTrue(metadata.viewExists(null, DB, "v"), "a table with view text is a view");
        Assertions.assertFalse(metadata.viewExists(null, DB, "t"),
                "a base table must NOT be a view (else DROP TABLE misroutes to dropView)");
        Assertions.assertFalse(metadata.viewExists(null, DB, "missing"),
                "a missing table is not a view (getTable throws HmsClientException -> false)");
    }

    @Test
    public void getViewDefinitionReturnsExpandedTextColumnsAndPlaceholderDialect() {
        FakeHmsClient client = new FakeHmsClient();
        client.put(plainView("v", "SELECT id, name FROM base"));
        HiveConnectorMetadata metadata = metadataOf(client);

        ConnectorViewDefinition def = metadata.getViewDefinition(null, DB, "v");
        Assertions.assertEquals("SELECT id, name FROM base", def.getSql(),
                "a plain view returns its expanded text verbatim");
        Assertions.assertEquals("hive", def.getDialect(), "dialect is the placeholder (fe-core never reads it)");
        Assertions.assertEquals(Arrays.asList("id", "name"),
                def.getColumns().stream().map(ConnectorColumn::getName).collect(Collectors.toList()),
                "view columns come from the metastore table columns (non-empty)");
    }

    @Test
    public void getViewDefinitionDecodesPrestoBase64OriginalSql() {
        FakeHmsClient client = new FakeHmsClient();
        client.put(prestoView("pv", "SELECT * FROM employees"));
        HiveConnectorMetadata metadata = metadataOf(client);

        ConnectorViewDefinition def = metadata.getViewDefinition(null, DB, "pv");
        Assertions.assertEquals("SELECT * FROM employees", def.getSql(),
                "the sentinel expanded text is skipped and the base64 originalSql is decoded");
    }

    @Test
    public void getViewDefinitionFallsBackToRawOriginalOnMalformedBase64() {
        FakeHmsClient client = new FakeHmsClient();
        HmsTableInfo malformed = HmsTableInfo.builder()
                .dbName(DB).tableName("bad")
                .tableType(VIRTUAL_VIEW)
                .viewExpandedText("/* Presto View */")
                .viewOriginalText("/* Presto View: @@not-base64@@ */")
                .columns(Arrays.asList(col("id", "INT")))
                .parameters(Collections.emptyMap())
                .build();
        client.put(malformed);
        HiveConnectorMetadata metadata = metadataOf(client);

        ConnectorViewDefinition def = metadata.getViewDefinition(null, DB, "bad");
        Assertions.assertEquals("/* Presto View: @@not-base64@@ */", def.getSql(),
                "a decode failure falls back to the raw original text (legacy parity)");
    }

    @Test
    public void dropViewRoutesToMetastoreDropTable() {
        FakeHmsClient client = new FakeHmsClient();
        client.put(plainView("v", "SELECT 1"));
        HiveConnectorMetadata metadata = metadataOf(client);

        metadata.dropView(null, DB, "v");
        Assertions.assertEquals(Collections.singletonList(DB + ".v"), client.droppedTables,
                "a view drop is a metastore dropTable (hive has no separate drop-view)");
    }

    @Test
    public void listViewNamesIsEmptyToAvoidDoubleListing() {
        HiveConnectorMetadata metadata = metadataOf(new FakeHmsClient());
        Assertions.assertTrue(metadata.listViewNames(null, DB).isEmpty(),
                "hive listTableNames already includes views; listViewNames must be empty or SHOW TABLES doubles");
    }

    @Test
    public void connectorDeclaresSupportsView() {
        HiveConnector connector = new HiveConnector(Collections.emptyMap(), new FakeConnectorContext());
        Assertions.assertTrue(connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_VIEW),
                "the hive connector must declare SUPPORTS_VIEW so the generic view path lights up post-flip");
    }

    /**
     * Recording fake {@link HmsClient}: serves prebuilt {@link HmsTableInfo}s by name (throwing
     * {@link HmsClientException} for an unknown name, like the real client) and records dropTable calls. All
     * other operations are unused by the view SPI and throw.
     */
    private static final class FakeHmsClient implements HmsClient {
        private final Map<String, HmsTableInfo> tables = new HashMap<>();
        private final List<String> droppedTables = new ArrayList<>();

        void put(HmsTableInfo info) {
            tables.put(info.getTableName(), info);
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            HmsTableInfo info = tables.get(tableName);
            if (info == null) {
                throw new HmsClientException("table not found: " + dbName + "." + tableName);
            }
            return info;
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return tables.containsKey(tableName);
        }

        @Override
        public void dropTable(String dbName, String tableName) {
            droppedTables.add(dbName + "." + tableName);
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName,
                List<String> partNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
