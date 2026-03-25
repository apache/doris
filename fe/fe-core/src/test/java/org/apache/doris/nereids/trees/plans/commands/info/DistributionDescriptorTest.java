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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DistributionDescriptorTest {

    private Map<String, ColumnDefinition> createColumnMap() {
        Map<String, ColumnDefinition> columnMap = Maps.newHashMap();
        ColumnDefinition col1 = new ColumnDefinition("col1", IntegerType.INSTANCE, false);
        columnMap.put("col1", col1);
        ColumnDefinition col2 = new ColumnDefinition("col2", IntegerType.INSTANCE, false);
        columnMap.put("col2", col2);
        return columnMap;
    }

    @Test
    public void testBucketNumMaxLimit() {
        Map<String, ColumnDefinition> columnMap = createColumnMap();
        int originalValue = Config.max_bucket_num_per_partition;

        try {
            // Test 1: normal bucket number within limit
            Config.max_bucket_num_per_partition = 100;
            DistributionDescriptor desc1 = new DistributionDescriptor(
                    true, false, 50, Lists.newArrayList("col1"));
            desc1.validate(columnMap, KeysType.DUP_KEYS); // should not throw

            // Test 2: bucket number exceeds limit
            DistributionDescriptor desc2 = new DistributionDescriptor(
                    true, false, 150, Lists.newArrayList("col1"));
            AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                    () -> desc2.validate(columnMap, KeysType.DUP_KEYS));
            Assertions.assertTrue(ex.getMessage().contains("exceeds the maximum allowed value (100)"));
            Assertions.assertTrue(ex.getMessage().contains("max_bucket_num_per_partition"));

            // Test 3: disable limit by setting to 0
            Config.max_bucket_num_per_partition = 0;
            DistributionDescriptor desc3 = new DistributionDescriptor(
                    true, false, 10000, Lists.newArrayList("col1"));
            desc3.validate(columnMap, KeysType.DUP_KEYS); // should not throw

            // Test 4: auto bucket is not limited by this config
            Config.max_bucket_num_per_partition = 10;
            DistributionDescriptor desc4 = new DistributionDescriptor(
                    true, true, 1000, Lists.newArrayList("col1"));
            desc4.validate(columnMap, KeysType.DUP_KEYS); // auto bucket should not throw

            // Test 5: random distribution also respects limit
            Config.max_bucket_num_per_partition = 100;
            DistributionDescriptor desc5 = new DistributionDescriptor(
                    false, false, 50, Lists.newArrayList());
            desc5.validate(columnMap, KeysType.DUP_KEYS); // should not throw

            DistributionDescriptor desc6 = new DistributionDescriptor(
                    false, false, 150, Lists.newArrayList());
            AnalysisException ex2 = Assertions.assertThrows(AnalysisException.class,
                    () -> desc6.validate(columnMap, KeysType.DUP_KEYS));
            Assertions.assertTrue(ex2.getMessage().contains("exceeds the maximum allowed value (100)"));

        } finally {
            Config.max_bucket_num_per_partition = originalValue;
        }
    }

    @Test
    public void testBucketNumZeroOrNegative() {
        Map<String, ColumnDefinition> columnMap = createColumnMap();

        // hash distribution with bucket <= 0
        DistributionDescriptor desc1 = new DistributionDescriptor(
                true, false, 0, Lists.newArrayList("col1"));
        AnalysisException ex1 = Assertions.assertThrows(AnalysisException.class,
                () -> desc1.validate(columnMap, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex1.getMessage().contains("greater than zero"));

        DistributionDescriptor desc2 = new DistributionDescriptor(
                true, false, -1, Lists.newArrayList("col1"));
        AnalysisException ex2 = Assertions.assertThrows(AnalysisException.class,
                () -> desc2.validate(columnMap, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex2.getMessage().contains("greater than zero"));

        // random distribution with bucket <= 0
        DistributionDescriptor desc3 = new DistributionDescriptor(
                false, false, 0, Lists.newArrayList());
        AnalysisException ex3 = Assertions.assertThrows(AnalysisException.class,
                () -> desc3.validate(columnMap, KeysType.DUP_KEYS));
        Assertions.assertTrue(ex3.getMessage().contains("greater than zero"));
    }
}
