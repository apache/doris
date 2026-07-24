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

import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Guards the paimon read-path table descriptor contract (P5-T19 Part B).
 *
 * <p>WHY this matters: after the paimon cutover, a SELECT (normal OR system table) routes through
 * {@code PluginDrivenExternalTable.toThrift()} -&gt; {@code metadata.buildTableDescriptor(...)}.
 * Legacy paimon ({@code PaimonExternalTable.toThrift} and {@code PaimonSysExternalTable.toThrift})
 * both send {@code TTableType.HIVE_TABLE} with a {@code THiveTable}. BE's
 * {@code DescriptorTbl::create} builds a {@code HiveTableDescriptor} for {@code HIVE_TABLE} but a
 * {@code SchemaTableDescriptor} for {@code SCHEMA_TABLE}. Without this override the SPI default
 * returns {@code null}, fe-core falls back to {@code SCHEMA_TABLE}, and BE builds the wrong
 * descriptor — a latent descriptor-parity bug for BOTH normal and system paimon plugin tables.
 * Each assertion below encodes a BE-side requirement, not just the method's shape (Rule 9): this
 * test FAILS if the override returns null (SPI default) or any non-HIVE_TABLE descriptor.</p>
 *
 * <p>The ctor only assigns its args; {@code buildTableDescriptor} never dereferences catalogOps /
 * context / properties, so passing {@code null}/empty for them is safe and keeps the test offline.</p>
 */
public class PaimonBuildTableDescriptorTest {

    @Test
    public void buildsHiveTableDescriptorWithAddressing() {
        PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(
                null, Collections.emptyMap(), new RecordingConnectorContext());

        long tableId = 42L;
        String tableName = "local_table";
        String dbName = "remote_db";
        String remoteName = "remote_table";
        int numCols = 7;
        long catalogId = 100L;

        TTableDescriptor desc = metadata.buildTableDescriptor(
                null, tableId, tableName, dbName, remoteName, numCols, catalogId);

        // (1) must not be null — null triggers the SCHEMA_TABLE fallback in fe-core.
        Assertions.assertNotNull(desc,
                "buildTableDescriptor must return a typed descriptor, never null (BE expects HIVE type)");
        // (2) BE builds a HiveTableDescriptor only for HIVE_TABLE; SCHEMA_TABLE would build the
        // wrong descriptor (SchemaTableDescriptor) — the descriptor-parity bug this fix closes.
        Assertions.assertEquals(TTableType.HIVE_TABLE, desc.getTableType(),
                "table type must be HIVE_TABLE; SCHEMA_TABLE builds the wrong BE descriptor");
        // (3) BE reads hiveTable; it must be set (legacy paimon always set a THiveTable).
        Assertions.assertTrue(desc.isSetHiveTable(),
                "hiveTable must be set; legacy paimon (normal + sys) always sent a THiveTable");
        // (4) addressing + column count must be carried through.
        Assertions.assertEquals(tableName, desc.getHiveTable().getTableName(),
                "THiveTable.tableName must carry the tableName param");
        Assertions.assertEquals(dbName, desc.getHiveTable().getDbName(),
                "THiveTable.dbName must carry the dbName param");
        Assertions.assertEquals(tableId, desc.getId(), "descriptor id must carry the tableId");
        Assertions.assertEquals(numCols, desc.getNumCols(), "descriptor numCols must carry numCols");
        Assertions.assertEquals(tableName, desc.getTableName(),
                "descriptor tableName must carry the tableName param");
        Assertions.assertEquals(dbName, desc.getDbName(),
                "descriptor dbName must carry the dbName param");
    }
}
