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

package org.apache.doris.common.util;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.util.AutoBucketCalculator.AutoBucketContext;
import org.apache.doris.common.util.AutoBucketCalculator.AutoBucketResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AutoBucketCalculatorTest {

    OlapTable table = Mockito.mock(OlapTable.class);

    @Test
    public void testCalculateAutoBucketsNotAutoBucket() {
        Mockito.when(table.isAutoBucket()).thenReturn(false);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, 10);
        AutoBucketResult result = AutoBucketCalculator.calculateAutoBuckets(context);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(10, result.getBuckets());
        Assertions.assertEquals("not auto bucket table", result.getErrorMessage());
    }

    @Test
    public void testCalculateAutoBucketsExecuteFirstTime() {
        Mockito.when(table.isAutoBucket()).thenReturn(true);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", true, 10);
        AutoBucketResult result = AutoBucketCalculator.calculateAutoBuckets(context);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(10, result.getBuckets());
        Assertions.assertEquals("executeFirstTime", result.getErrorMessage());
    }

    @Test
    public void testCalculateAutoBucketsWithBoundsCheck() {
        Mockito.when(table.isAutoBucket()).thenReturn(false);
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);

        AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, 10);
        int buckets = AutoBucketCalculator.calculateAutoBucketsWithBoundsCheck(context);

        Assertions.assertEquals(10, buckets);
    }

    @Test
    public void testAutoBucketContextWithCurrentUtcBorder() {
        Mockito.when(table.getName()).thenReturn("test_table");
        Mockito.when(table.getId()).thenReturn(1L);

        String currentUtcBorder = "2026-07-06 00:00:00+00:00";

        // 6-param constructor: currentUtcBorder is explicitly provided.
        AutoBucketContext contextWithUtc = new AutoBucketContext(
                table, "p1", "p2", false, 10, currentUtcBorder);
        Assertions.assertEquals(currentUtcBorder, contextWithUtc.getCurrentUtcBorder());
        Assertions.assertEquals("p1", contextWithUtc.getPartitionName());
        Assertions.assertEquals("p2", contextWithUtc.getNowPartitionName());
        Assertions.assertFalse(contextWithUtc.isExecuteFirstTime());
        Assertions.assertEquals(10, contextWithUtc.getDefaultBuckets());

        // 5-param backward-compat constructor: currentUtcBorder defaults to null.
        AutoBucketContext contextNoUtc = new AutoBucketContext(
                table, "p3", "p4", true, 5);
        Assertions.assertNull(contextNoUtc.getCurrentUtcBorder());
        Assertions.assertEquals("p3", contextNoUtc.getPartitionName());
        Assertions.assertTrue(contextNoUtc.isExecuteFirstTime());
        Assertions.assertEquals(5, contextNoUtc.getDefaultBuckets());
    }
}
