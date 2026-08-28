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

package org.apache.doris.catalog.constraint;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableAttributes;
import org.apache.doris.catalog.Type;
import org.apache.doris.persist.ModifyTablePropertyOperationLog;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DistributionMappingConstraintPersistTest {

    @Test
    void tableAttributesRoundTripAndRemainReadableByLegacyCode() {
        DistributionMappingConstraint mapping = newBoundMapping();
        TableAttributes attributes = new TableAttributes();
        attributes.getDistributionMappingConstraints().put(mapping.getName(), mapping);

        String json = GsonUtils.GSON.toJson(attributes);
        TableAttributes restored = GsonUtils.GSON.fromJson(json, TableAttributes.class);
        DistributionMappingConstraint restoredMapping =
                restored.getDistributionMappingConstraints().get(mapping.getName());
        Assertions.assertEquals(mapping, restoredMapping);
        Assertions.assertEquals(mapping.getBaseSchemaVersion(), restoredMapping.getBaseSchemaVersion());
        Assertions.assertEquals(
                mapping.getDeterminantColumnUniqueIds(), restoredMapping.getDeterminantColumnUniqueIds());
        Assertions.assertEquals(
                mapping.getDistributionColumnUniqueIds(), restoredMapping.getDistributionColumnUniqueIds());
        Assertions.assertEquals(
                mapping.getDeterminantColumnTypeSignatures(), restoredMapping.getDeterminantColumnTypeSignatures());
        Assertions.assertEquals(
                mapping.getDistributionColumnTypeSignatures(), restoredMapping.getDistributionColumnTypeSignatures());

        LegacyTableAttributes legacy = GsonUtils.GSON.fromJson(json, LegacyTableAttributes.class);
        Assertions.assertTrue(legacy.constraints.isEmpty());
        Assertions.assertFalse(json.contains("\"clazz\":\"DistributionMappingConstraint\""));

        TableAttributes restoredFromLegacy = GsonUtils.GSON.fromJson(
                "{\"constraints\":{},\"visibleVersion\":1,\"visibleVersionTime\":1}",
                TableAttributes.class);
        Assertions.assertTrue(restoredFromLegacy.getDistributionMappingConstraints().isEmpty());
    }

    @Test
    void tablePropertyJournalRoundTripAndRemainReadableByLegacyCode() throws Exception {
        DistributionMappingConstraint mapping = newBoundMapping();
        ModifyTablePropertyOperationLog addLog =
                ModifyTablePropertyOperationLog.addDistributionMappingConstraint(
                        1L, 2L, "table", mapping);
        ModifyTablePropertyOperationLog restoredAddLog = GsonUtils.GSON.fromJson(
                addLog.toJson(), ModifyTablePropertyOperationLog.class);

        Assertions.assertEquals(mapping, restoredAddLog.getDistributionMappingConstraint());
        Assertions.assertNull(restoredAddLog.getDroppedDistributionMappingConstraint());
        Assertions.assertTrue(restoredAddLog.getProperties().isEmpty());

        ModifyTablePropertyOperationLog dropLog =
                ModifyTablePropertyOperationLog.dropDistributionMappingConstraint(
                        1L, 2L, "table", mapping.getName());
        ModifyTablePropertyOperationLog restoredDropLog = GsonUtils.GSON.fromJson(
                dropLog.toJson(), ModifyTablePropertyOperationLog.class);
        Assertions.assertNull(restoredDropLog.getDistributionMappingConstraint());
        Assertions.assertEquals(mapping.getName(),
                restoredDropLog.getDroppedDistributionMappingConstraint());

        LegacyModifyTablePropertyOperationLog legacy = GsonUtils.GSON.fromJson(
                addLog.toJson(), LegacyModifyTablePropertyOperationLog.class);
        Assertions.assertEquals(1L, legacy.dbId);
        Assertions.assertEquals(2L, legacy.tableId);
        Assertions.assertEquals("table", legacy.tableName);
        Assertions.assertTrue(legacy.properties.isEmpty());
    }

    private DistributionMappingConstraint newBoundMapping() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Column determinant = new Column("d1", Type.INT);
        Column distribution = new Column("k1", Type.BIGINT);
        determinant.setUniqueId(10);
        distribution.setUniqueId(20);
        Mockito.when(table.getBaseSchemaVersion()).thenReturn(7);
        Mockito.when(table.getColumn("d1")).thenReturn(determinant);
        Mockito.when(table.getColumn("k1")).thenReturn(distribution);
        return new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("d1"), List.of("k1")).bindTo(table);
    }

    private static class LegacyTableAttributes {
        @SerializedName("constraints")
        private Map<String, Object> constraints = new HashMap<>();
    }

    private static class LegacyModifyTablePropertyOperationLog {
        @SerializedName("dbId")
        private long dbId;
        @SerializedName("tableId")
        private long tableId;
        @SerializedName("tableName")
        private String tableName;
        @SerializedName("properties")
        private Map<String, String> properties = new HashMap<>();
    }
}
