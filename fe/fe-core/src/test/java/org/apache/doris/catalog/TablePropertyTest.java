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

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TablePropertyTest {

    // A non-whitelisted dynamic_partition.* key is ignored (skipped via continue), so it is not
    // collected at all and the table is neither built as dynamic nor flagged as incomplete.
    @Test
    public void testIgnoreInvalidDynamicPartitionPropertyKey() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DynamicPartitionProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX + "not_a_real_key", "1");
        TableProperty tableProperty = new TableProperty(properties).buildDynamicProperty();
        Assert.assertFalse(tableProperty.getDynamicPartitionProperty().isExist());
        Assert.assertFalse(tableProperty.hasInvalidDynamicPartition());
    }

    // Only storage_medium (a leftover from a failed ALTER on a non-dynamic table): incomplete,
    // must be downgraded to a non-dynamic-partition table instead of crashing on parseInt(null).
    @Test
    public void testIncompleteStorageMediumIsDowngraded() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DynamicPartitionProperty.STORAGE_MEDIUM, "hdd");
        TableProperty tableProperty = new TableProperty(properties).buildDynamicProperty();
        Assert.assertFalse(tableProperty.getDynamicPartitionProperty().isExist());
        Assert.assertTrue(tableProperty.hasInvalidDynamicPartition());
    }

    // Symmetric to storage_medium: a leftover storage_policy alone is also incomplete.
    @Test
    public void testIncompleteStoragePolicyIsDowngraded() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DynamicPartitionProperty.STORAGE_POLICY, "test_policy");
        TableProperty tableProperty = new TableProperty(properties).buildDynamicProperty();
        Assert.assertFalse(tableProperty.getDynamicPartitionProperty().isExist());
        Assert.assertTrue(tableProperty.hasInvalidDynamicPartition());
    }

    // time_unit present but end missing: still incomplete (covers the END required-key branch).
    @Test
    public void testIncompleteMissingEndIsDowngraded() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DynamicPartitionProperty.TIME_UNIT, "DAY");
        TableProperty tableProperty = new TableProperty(properties).buildDynamicProperty();
        Assert.assertFalse(tableProperty.getDynamicPartitionProperty().isExist());
        Assert.assertTrue(tableProperty.hasInvalidDynamicPartition());
    }

    // time_unit + end present but prefix missing (covers the PREFIX required-key branch).
    @Test
    public void testIncompleteMissingPrefixIsDowngraded() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DynamicPartitionProperty.TIME_UNIT, "DAY");
        properties.put(DynamicPartitionProperty.END, "3");
        TableProperty tableProperty = new TableProperty(properties).buildDynamicProperty();
        Assert.assertFalse(tableProperty.getDynamicPartitionProperty().isExist());
        Assert.assertTrue(tableProperty.hasInvalidDynamicPartition());
    }

    // time_unit + end + prefix present but buckets missing (covers the BUCKETS required-key branch).
    @Test
    public void testIncompleteMissingBucketsIsDowngraded() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DynamicPartitionProperty.TIME_UNIT, "DAY");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        TableProperty tableProperty = new TableProperty(properties).buildDynamicProperty();
        Assert.assertFalse(tableProperty.getDynamicPartitionProperty().isExist());
        Assert.assertTrue(tableProperty.hasInvalidDynamicPartition());
    }

    // All required keys present: a real DynamicPartitionProperty is built, not downgraded.
    @Test
    public void testCompleteDynamicPartitionIsBuilt() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(DynamicPartitionProperty.ENABLE, "true");
        properties.put(DynamicPartitionProperty.TIME_UNIT, "DAY");
        properties.put(DynamicPartitionProperty.END, "3");
        properties.put(DynamicPartitionProperty.PREFIX, "p");
        properties.put(DynamicPartitionProperty.BUCKETS, "1");
        TableProperty tableProperty = new TableProperty(properties).buildDynamicProperty();
        Assert.assertTrue(tableProperty.getDynamicPartitionProperty().isExist());
        Assert.assertFalse(tableProperty.hasInvalidDynamicPartition());
        Assert.assertEquals(3, tableProperty.getDynamicPartitionProperty().getEnd());
        Assert.assertEquals(1, tableProperty.getDynamicPartitionProperty().getBuckets());
    }
}
