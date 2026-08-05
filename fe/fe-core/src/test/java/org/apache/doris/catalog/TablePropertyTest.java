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

package org.apache.doris.catalog;

import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.resource.Tag;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class TablePropertyTest {
    private static final String DEFAULT_REPLICATION_NUM =
            "default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;
    private static final String DEFAULT_REPLICATION_ALLOCATION =
            "default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION;
    private static final String REPLICATION_ALLOCATION =
            "tag.location.group_0: 1, tag.location.group_1: 1, tag.location.group_2: 1";

    @Test
    public void testModifyDefaultReplicaAllocationRemovesLegacyReplicationNum() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DEFAULT_REPLICATION_NUM, "3");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildReplicaAllocation();

        Map<String, String> modifiedProperties = Maps.newHashMap();
        modifiedProperties.put(DEFAULT_REPLICATION_ALLOCATION, REPLICATION_ALLOCATION);
        tableProperty.modifyTableProperties(modifiedProperties);
        tableProperty.buildReplicaAllocation();

        assertReplicaAllocationWins(tableProperty);
    }

    @Test
    public void testDeserializeConflictingDefaultReplicaPropertiesPreservesNumericPrecedence() throws IOException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DEFAULT_REPLICATION_NUM, "3");
        properties.put(DEFAULT_REPLICATION_ALLOCATION, REPLICATION_ALLOCATION);
        TableProperty tableProperty = new TableProperty(properties);

        tableProperty.gsonPostProcess();

        Assert.assertTrue(tableProperty.getProperties().containsKey(DEFAULT_REPLICATION_NUM));
        Assert.assertTrue(tableProperty.getProperties().containsKey(DEFAULT_REPLICATION_ALLOCATION));
        Assert.assertEquals(Short.valueOf((short) 3),
                tableProperty.getReplicaAllocation().getReplicaNumByTag(Tag.DEFAULT_BACKEND_TAG));
    }

    @Test
    public void testResetPropertiesForRestoreRemovesLegacyReplicationNum() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DEFAULT_REPLICATION_NUM, "3");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildReplicaAllocation();

        ReplicaAllocation restoredReplicaAllocation = new ReplicaAllocation((short) 2);
        tableProperty.resetPropertiesForRestore(false, false, restoredReplicaAllocation);

        Assert.assertFalse(tableProperty.getProperties().containsKey(DEFAULT_REPLICATION_NUM));
        Assert.assertEquals(restoredReplicaAllocation.toCreateStmt(),
                tableProperty.getProperties().get(DEFAULT_REPLICATION_ALLOCATION));
        Assert.assertEquals((short) 2, tableProperty.getReplicaAllocation().getTotalReplicaNum());
    }

    @Test
    public void testModifyDefaultReplicationNumRemovesExistingAllocation() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DEFAULT_REPLICATION_ALLOCATION, REPLICATION_ALLOCATION);
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildReplicaAllocation();

        Map<String, String> modifiedProperties = Maps.newHashMap();
        modifiedProperties.put(DEFAULT_REPLICATION_NUM, "2");
        tableProperty.modifyTableProperties(modifiedProperties);
        tableProperty.buildReplicaAllocation();

        Assert.assertFalse(tableProperty.getProperties().containsKey(DEFAULT_REPLICATION_ALLOCATION));
        Assert.assertEquals(Short.valueOf((short) 2),
                tableProperty.getReplicaAllocation().getReplicaNumByTag(Tag.DEFAULT_BACKEND_TAG));
    }

    private void assertReplicaAllocationWins(TableProperty tableProperty) {
        Assert.assertFalse(tableProperty.getProperties().containsKey(DEFAULT_REPLICATION_NUM));
        Assert.assertEquals(Short.valueOf((short) 1),
                tableProperty.getReplicaAllocation()
                        .getReplicaNumByTag(Tag.createNotCheck(Tag.TYPE_LOCATION, "group_0")));
        Assert.assertEquals(Short.valueOf((short) 1),
                tableProperty.getReplicaAllocation()
                        .getReplicaNumByTag(Tag.createNotCheck(Tag.TYPE_LOCATION, "group_1")));
        Assert.assertEquals(Short.valueOf((short) 1),
                tableProperty.getReplicaAllocation()
                        .getReplicaNumByTag(Tag.createNotCheck(Tag.TYPE_LOCATION, "group_2")));
    }
}
