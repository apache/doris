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

package org.apache.doris.connector.iceberg;

import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Guards the iceberg read-path table-descriptor contract (P6.5-T06).
 *
 * <p>WHY this matters: after the iceberg SPI cutover (P6.6), a SELECT (normal OR system table) routes
 * through {@code PluginDrivenExternalTable.toThrift()} -&gt; {@code metadata.buildTableDescriptor(...)}.
 * Legacy iceberg ({@code IcebergExternalTable.toThrift} and {@code IcebergSysExternalTable.toThrift})
 * FORK on the catalog type: an {@code hms}-backed catalog sends {@code TTableType.HIVE_TABLE} with a
 * {@code THiveTable}; every other flavor sends {@code TTableType.ICEBERG_TABLE} with a {@code TIcebergTable}.
 * Without this override the SPI default returns {@code null}, fe-core falls back to
 * {@code TTableType.SCHEMA_TABLE}, and BE's {@code DescriptorTbl::create} builds the WRONG descriptor type —
 * a latent descriptor-parity bug for BOTH normal and system iceberg plugin tables. Iceberg is the FIRST
 * connector whose {@code buildTableDescriptor} forks on the catalog type (paimon/es/maxcompute emit a single
 * fixed type); each assertion encodes a legacy byte-shape requirement, not just the method's shape (Rule 9).</p>
 *
 * <p>The override reads only the {@code iceberg.catalog.type} property and its method args; it never
 * dereferences catalogOps / context, so passing {@code null}/empty for them keeps the test offline.</p>
 */
public class IcebergBuildTableDescriptorTest {

    private static final long TABLE_ID = 42L;
    private static final String TABLE_NAME = "local_table";
    private static final String DB_NAME = "remote_db";
    private static final String REMOTE_NAME = "remote_table";
    private static final int NUM_COLS = 7;
    private static final long CATALOG_ID = 100L;

    private static IcebergConnectorMetadata metadataWithCatalogType(String catalogType) {
        Map<String, String> props = new HashMap<>();
        if (catalogType != null) {
            props.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, catalogType);
        }
        return new IcebergConnectorMetadata(null, props, new RecordingConnectorContext());
    }

    private static TTableDescriptor build(IcebergConnectorMetadata metadata) {
        return metadata.buildTableDescriptor(
                null, TABLE_ID, TABLE_NAME, DB_NAME, REMOTE_NAME, NUM_COLS, CATALOG_ID);
    }

    @Test
    public void buildsHiveTableDescriptorForHmsCatalog() {
        // hms branch: legacy IcebergSysExternalTable.toThrift / IcebergExternalTable.toThrift send
        // TTableType.HIVE_TABLE + THiveTable. MUTATION: dropping the hms fork -> ICEBERG_TABLE here -> red.
        TTableDescriptor desc = build(metadataWithCatalogType(IcebergConnectorProperties.TYPE_HMS));

        Assertions.assertNotNull(desc,
                "buildTableDescriptor must return a typed descriptor, never null (null -> SCHEMA_TABLE fallback)");
        Assertions.assertEquals(TTableType.HIVE_TABLE, desc.getTableType(),
                "hms-backed iceberg catalog must report HIVE_TABLE (legacy parity)");
        Assertions.assertTrue(desc.isSetHiveTable(),
                "hms branch must set THiveTable (legacy IcebergSysExternalTable hms branch)");
        Assertions.assertFalse(desc.isSetIcebergTable(),
                "hms branch must NOT set TIcebergTable");
        Assertions.assertEquals(TABLE_NAME, desc.getHiveTable().getTableName());
        Assertions.assertEquals(DB_NAME, desc.getHiveTable().getDbName());
        assertAddressing(desc);
    }

    @Test
    public void buildsIcebergTableDescriptorForRestCatalog() {
        // non-hms (rest) branch: legacy sends TTableType.ICEBERG_TABLE + TIcebergTable.
        // MUTATION: always emitting HIVE_TABLE (paimon-style single type) -> red here.
        TTableDescriptor desc = build(metadataWithCatalogType(IcebergConnectorProperties.TYPE_REST));

        Assertions.assertNotNull(desc, "non-hms catalog descriptor must not be null");
        Assertions.assertEquals(TTableType.ICEBERG_TABLE, desc.getTableType(),
                "non-hms iceberg catalog must report ICEBERG_TABLE (legacy else-branch parity)");
        Assertions.assertTrue(desc.isSetIcebergTable(),
                "non-hms branch must set TIcebergTable (legacy IcebergSysExternalTable else branch)");
        Assertions.assertFalse(desc.isSetHiveTable(),
                "non-hms branch must NOT set THiveTable");
        Assertions.assertEquals(TABLE_NAME, desc.getIcebergTable().getTableName());
        Assertions.assertEquals(DB_NAME, desc.getIcebergTable().getDbName());
        assertAddressing(desc);
    }

    @Test
    public void defaultsToIcebergTableWhenCatalogTypeAbsent() {
        // No iceberg.catalog.type property at all: legacy predicate getIcebergCatalogType().equals("hms")
        // is false for any non-"hms" value, so the else (ICEBERG_TABLE) branch is taken. MUTATION: an
        // unguarded properties.get(...).equals("hms") would NPE here -> red; a wrong default -> red.
        TTableDescriptor desc = build(metadataWithCatalogType(null));

        Assertions.assertEquals(TTableType.ICEBERG_TABLE, desc.getTableType(),
                "absent catalog type must default to ICEBERG_TABLE (not hms)");
        Assertions.assertTrue(desc.isSetIcebergTable());
    }

    private static void assertAddressing(TTableDescriptor desc) {
        Assertions.assertEquals(TABLE_ID, desc.getId(), "descriptor id must carry the tableId");
        Assertions.assertEquals(NUM_COLS, desc.getNumCols(), "descriptor numCols must carry numCols");
        Assertions.assertEquals(0, desc.getNumClusteringCols(),
                "numClusteringCols must be 0 (sys/iceberg tables have no distribution; legacy parity)");
        Assertions.assertEquals(TABLE_NAME, desc.getTableName(), "descriptor tableName must carry the tableName");
        Assertions.assertEquals(DB_NAME, desc.getDbName(), "descriptor dbName must carry the dbName");
        // Empty property map in the nested table struct (legacy sent new HashMap<>()).
        Assertions.assertEquals(Collections.emptyMap(),
                desc.getTableType() == TTableType.HIVE_TABLE
                        ? desc.getHiveTable().getProperties()
                        : desc.getIcebergTable().getProperties(),
                "nested table struct must carry an empty properties map (legacy parity)");
    }
}
