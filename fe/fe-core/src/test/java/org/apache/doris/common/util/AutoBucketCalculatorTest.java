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

import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.AutoBucketCalculator.AutoBucketContext;
import org.apache.doris.common.util.AutoBucketCalculator.AutoBucketResult;
import org.apache.doris.rpc.RpcException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class AutoBucketCalculatorTest {

    OlapTable table = Mockito.mock(OlapTable.class);
    private int originalMinBuckets;

    @BeforeEach
    public void setUp() {
        originalMinBuckets = Config.autobucket_min_buckets;
        Config.autobucket_min_buckets = 4;
        RangePartitionInfo partitionInfo = Mockito.mock(RangePartitionInfo.class);
        Mockito.when(partitionInfo.getIdToItem(false)).thenReturn(Collections.emptyMap());
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
    }

    @AfterEach
    public void tearDown() {
        Config.autobucket_min_buckets = originalMinBuckets;
    }

    @Test
    public void testFixedBucketsBelowMinimum() {
        for (int buckets : new int[] {1, 2}) {
            AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, buckets);
            Assertions.assertEquals(buckets, AutoBucketCalculator.calculateAutoBuckets(context).getBuckets());
            Assertions.assertEquals(buckets, AutoBucketCalculator.calculateAutoBucketsWithBoundsCheck(context));
        }
    }

    @Test
    public void testFirstExecutionUsesCurrentMinimum() {
        Mockito.when(table.isAutoBucket()).thenReturn(true);
        for (int buckets : new int[] {1, 2, 8}) {
            AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", true, buckets);
            Assertions.assertEquals(Math.max(4, buckets),
                    AutoBucketCalculator.calculateAutoBuckets(context).getBuckets());
            Assertions.assertEquals(Math.max(4, buckets),
                    AutoBucketCalculator.calculateAutoBucketsWithBoundsCheck(context));
        }
    }

    @Test
    public void testNoHistoricalPartitionsUsesCurrentMinimum() throws RpcException {
        assertFallbackUsesCurrentMinimum(Collections.emptyList(), Collections.emptyList(), "no data partitions");
    }

    @Test
    public void testEmptyHistoricalPartitionUsesCurrentMinimum() throws RpcException {
        assertFallbackUsesCurrentMinimum(Collections.singletonList(Mockito.mock(Partition.class)),
                Collections.singletonList(1L), "no data partitions");
    }

    @Test
    public void testVisibleVersionsMismatchUsesCurrentMinimum() throws RpcException {
        assertFallbackUsesCurrentMinimum(Collections.singletonList(Mockito.mock(Partition.class)),
                Collections.emptyList(), "visible versions mismatch");
    }

    @Test
    public void testMissingVisibleVersionsUsesCurrentMinimum() throws RpcException {
        assertFallbackUsesCurrentMinimum(Collections.singletonList(Mockito.mock(Partition.class)),
                null, "visible versions mismatch");
    }

    @Test
    public void testVisibleVersionRpcFailureUsesCurrentMinimum() throws RpcException {
        Mockito.when(table.isAutoBucket()).thenReturn(true);
        List<Partition> partitions = Collections.singletonList(Mockito.mock(Partition.class));
        try (MockedStatic<DynamicPartitionScheduler> scheduler = Mockito.mockStatic(
                    DynamicPartitionScheduler.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Partition> partitionMethods = Mockito.mockStatic(Partition.class)) {
            scheduler.when(() -> DynamicPartitionScheduler.getHistoricalPartitions(table, "p2"))
                    .thenReturn(partitions);
            partitionMethods.when(() -> Partition.getVisibleVersions(partitions))
                    .thenThrow(new RpcException("", "test failure"));
            assertFallbackBuckets("RpcException: test failure");
        }
    }

    @Test
    public void testUnknownHistoricalSizesUsesCurrentMinimum() throws RpcException {
        Mockito.when(table.isAutoBucket()).thenReturn(true);
        Partition partition = Mockito.mock(Partition.class);
        HashDistributionInfo distribution = Mockito.mock(HashDistributionInfo.class);
        Mockito.when(partition.getDistributionInfo()).thenReturn(distribution);
        Mockito.when(partition.getDataSizeExcludeEmptyReplica(true)).thenReturn(0L);
        List<Partition> partitions = Collections.singletonList(partition);
        try (MockedStatic<DynamicPartitionScheduler> scheduler = Mockito.mockStatic(
                    DynamicPartitionScheduler.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Partition> partitionMethods = Mockito.mockStatic(Partition.class)) {
            scheduler.when(() -> DynamicPartitionScheduler.getHistoricalPartitions(table, "p2"))
                    .thenReturn(partitions);
            partitionMethods.when(() -> Partition.getVisibleVersions(partitions))
                    .thenReturn(Collections.singletonList(2L));
            for (int buckets : new int[] {1, 2, 8}) {
                Mockito.when(distribution.getBucketNum()).thenReturn(buckets);
                AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, 1);
                AutoBucketResult result = AutoBucketCalculator.calculateAutoBuckets(context);
                Assertions.assertTrue(result.isSuccess());
                Assertions.assertEquals(Math.max(4, buckets), result.getBuckets());
                Assertions.assertEquals(buckets, result.getPreviousBuckets());
                Assertions.assertEquals(Math.max(4, buckets),
                        AutoBucketCalculator.calculateAutoBucketsWithBoundsCheck(context));
            }
        }
    }

    private void assertFallbackUsesCurrentMinimum(List<Partition> partitions, List<Long> versions, String reason)
            throws RpcException {
        Mockito.when(table.isAutoBucket()).thenReturn(true);
        try (MockedStatic<DynamicPartitionScheduler> scheduler = Mockito.mockStatic(
                    DynamicPartitionScheduler.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Partition> partitionMethods = Mockito.mockStatic(Partition.class)) {
            scheduler.when(() -> DynamicPartitionScheduler.getHistoricalPartitions(table, "p2"))
                    .thenReturn(partitions);
            partitionMethods.when(() -> Partition.getVisibleVersions(partitions)).thenReturn(versions);
            assertFallbackBuckets(reason);
        }
    }

    private void assertFallbackBuckets(String reason) {
        for (int buckets : new int[] {1, 2, 8}) {
            AutoBucketContext context = new AutoBucketContext(table, "p1", "p2", false, buckets);
            AutoBucketResult result = AutoBucketCalculator.calculateAutoBuckets(context);
            Assertions.assertFalse(result.isSuccess());
            Assertions.assertEquals(reason, result.getErrorMessage());
            Assertions.assertEquals(Math.max(4, buckets), result.getBuckets());
            Assertions.assertEquals(Math.max(4, buckets),
                    AutoBucketCalculator.calculateAutoBucketsWithBoundsCheck(context));
        }
    }

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
}
