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
import org.apache.doris.catalog.TableProperty;
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
    void tablePropertyRoundTripPreservesMappingsInStableOrder() {
        DistributionMappingConstraint second = newBoundMapping("mapping_b", "mapping_b_id");
        DistributionMappingConstraint first = newBoundMapping("mapping_a", "mapping_a_id");
        TableProperty tableProperty = new TableProperty(new HashMap<>());
        tableProperty.addDistributionMappingConstraint(second);
        tableProperty.addDistributionMappingConstraint(first);

        String snapshot = tableProperty.getProperties()
                .get(TableProperty.DISTRIBUTION_MAPPING_CONSTRAINTS_PROPERTY);
        Assertions.assertTrue(snapshot.indexOf("mapping_a") < snapshot.indexOf("mapping_b"));

        String json = GsonUtils.GSON.toJson(tableProperty);
        Assertions.assertFalse(json.contains("\"distributionMappingConstraints\""));
        TableProperty restored = GsonUtils.GSON.fromJson(json, TableProperty.class);
        DistributionMappingConstraint restoredMapping = restored.getDistributionMappingConstraints().get("mapping_a");
        Assertions.assertEquals(List.of(first, second), restored.getDistributionMappingConstraints().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toList());
        Assertions.assertEquals(first, restoredMapping);
        Assertions.assertEquals(first.getBaseSchemaVersion(), restoredMapping.getBaseSchemaVersion());
        Assertions.assertEquals(
                first.getDeterminantColumnUniqueIds(), restoredMapping.getDeterminantColumnUniqueIds());
        Assertions.assertEquals(
                first.getDistributionColumnUniqueIds(), restoredMapping.getDistributionColumnUniqueIds());
        Assertions.assertEquals(
                first.getDeterminantColumnTypeSignatures(), restoredMapping.getDeterminantColumnTypeSignatures());
        Assertions.assertEquals(
                first.getDistributionColumnTypeSignatures(), restoredMapping.getDistributionColumnTypeSignatures());
    }

    @Test
    void oldFrontendReplayAndCheckpointPreserveAddAndDropSnapshots() {
        DistributionMappingConstraint mapping = newBoundMapping("mapping", "mapping_id");
        TableProperty currentProperty = new TableProperty(new HashMap<>());
        currentProperty.addDistributionMappingConstraint(mapping);
        ModifyTablePropertyOperationLog addLog = new ModifyTablePropertyOperationLog(
                1L, 2L, "table", currentProperty.getDistributionMappingConstraintProperties());

        LegacyModifyTablePropertyOperationLog legacyAddLog = GsonUtils.GSON.fromJson(
                addLog.toJson(), LegacyModifyTablePropertyOperationLog.class);
        Assertions.assertEquals(1L, legacyAddLog.dbId);
        Assertions.assertEquals(2L, legacyAddLog.tableId);
        Assertions.assertEquals("table", legacyAddLog.tableName);
        Assertions.assertTrue(legacyAddLog.properties.containsKey(
                TableProperty.DISTRIBUTION_MAPPING_CONSTRAINTS_PROPERTY));

        LegacyTableProperty legacyTableProperty = new LegacyTableProperty();
        legacyTableProperty.properties.putAll(legacyAddLog.properties);
        legacyTableProperty.properties.put("in_memory", "false");
        TableProperty restoredAfterOldCheckpoint = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(legacyTableProperty), TableProperty.class);
        Assertions.assertEquals(mapping,
                restoredAfterOldCheckpoint.getDistributionMappingConstraints().get(mapping.getName()));

        currentProperty.removeDistributionMappingConstraint(mapping.getName());
        ModifyTablePropertyOperationLog dropLog = new ModifyTablePropertyOperationLog(
                1L, 2L, "table", currentProperty.getDistributionMappingConstraintProperties());
        LegacyModifyTablePropertyOperationLog legacyDropLog = GsonUtils.GSON.fromJson(
                dropLog.toJson(), LegacyModifyTablePropertyOperationLog.class);
        Assertions.assertEquals("[]", legacyDropLog.properties.get(
                TableProperty.DISTRIBUTION_MAPPING_CONSTRAINTS_PROPERTY));

        legacyTableProperty.properties.putAll(legacyDropLog.properties);
        TableProperty restoredAfterOldDropCheckpoint = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(legacyTableProperty), TableProperty.class);
        Assertions.assertTrue(restoredAfterOldDropCheckpoint.getDistributionMappingConstraints().isEmpty());
        Assertions.assertEquals("[]", restoredAfterOldDropCheckpoint.getProperties().get(
                TableProperty.DISTRIBUTION_MAPPING_CONSTRAINTS_PROPERTY));
    }

    private DistributionMappingConstraint newBoundMapping(String name, String mappingId) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Column determinant = new Column("d1", Type.INT);
        Column distribution = new Column("k1", Type.BIGINT);
        determinant.setUniqueId(10);
        distribution.setUniqueId(20);
        Mockito.when(table.getBaseSchemaVersion()).thenReturn(7);
        Mockito.when(table.getColumn("d1")).thenReturn(determinant);
        Mockito.when(table.getColumn("k1")).thenReturn(distribution);
        return new DistributionMappingConstraint(
                name, mappingId, List.of("d1"), List.of("k1")).bindTo(table);
    }

    private static class LegacyTableProperty {
        @SerializedName("properties")
        private Map<String, String> properties = new HashMap<>();
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
