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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Set;

public class MTMVExpandPartitionTest {

    private static final Column DATE_COL = new Column("c1", ScalarType.createType(PrimitiveType.DATE),
            true, null, "", "");

    private MTMVRelatedTableIf rangeTable;
    private MTMVRelatedTableIf listTable;
    private Map<String, PartitionItem> monthlyMvPartitions;

    @Before
    public void setUp() throws Exception {
        Map<String, PartitionItem> dailyBasePartitions = Maps.newHashMap();
        dailyBasePartitions.put("p20210101", buildRange("2021-01-01", "2021-01-02"));
        dailyBasePartitions.put("p20210102", buildRange("2021-01-02", "2021-01-03"));
        dailyBasePartitions.put("p20210103", buildRange("2021-01-03", "2021-01-04"));
        dailyBasePartitions.put("p20210201", buildRange("2021-02-01", "2021-02-02"));
        dailyBasePartitions.put("p20210202", buildRange("2021-02-02", "2021-02-03"));

        monthlyMvPartitions = Maps.newHashMap();
        monthlyMvPartitions.put("mv_202101", buildRange("2021-01-01", "2021-02-01"));
        monthlyMvPartitions.put("mv_202102", buildRange("2021-02-01", "2021-03-01"));

        rangeTable = createMockTable(PartitionType.RANGE, dailyBasePartitions);
        listTable = createMockTable(PartitionType.LIST, Maps.newHashMap());
    }

    @Test
    public void testExpandSinglePartitionToMonth() throws Exception {
        Set<String> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                Sets.newHashSet("p20210102"), monthlyMvPartitions, rangeTable);

        Assert.assertEquals(Sets.newHashSet("p20210101", "p20210102", "p20210103"), result);
    }

    @Test
    public void testExpandMultipleMonths() throws Exception {
        Set<String> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                Sets.newHashSet("p20210102", "p20210202"), monthlyMvPartitions, rangeTable);

        Assert.assertEquals(Sets.newHashSet(
                "p20210101", "p20210102", "p20210103", "p20210201", "p20210202"), result);
    }

    @Test
    public void testListPartitionPassthrough() throws Exception {
        Set<String> queryUsedPartitions = Sets.newHashSet("p1");

        Assert.assertSame(queryUsedPartitions, MTMVPartitionExpander.expandToMvPartitionGranularity(
                queryUsedPartitions, monthlyMvPartitions, listTable));
    }

    @Test
    public void testNonExistentPartition() throws Exception {
        Set<String> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                Sets.newHashSet("p_nonexistent"), monthlyMvPartitions, rangeTable);

        Assert.assertTrue(result.isEmpty());
    }

    private static MTMVRelatedTableIf createMockTable(
            PartitionType partitionType, Map<String, PartitionItem> partitionItems) {
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "getPartitionType":
                    return partitionType;
                case "getAndCopyPartitionItems":
                    return partitionItems;
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    throw new UnsupportedOperationException(
                            "MTMVExpandPartitionTest mock does not support: " + method.getName());
            }
        };
        return (MTMVRelatedTableIf) Proxy.newProxyInstance(
                MTMVRelatedTableIf.class.getClassLoader(), new Class<?>[] {MTMVRelatedTableIf.class}, handler);
    }

    private static RangePartitionItem buildRange(String lower, String upper) throws AnalysisException {
        PartitionKey lowerKey = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(lower)), Lists.newArrayList(DATE_COL));
        PartitionKey upperKey = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(upper)), Lists.newArrayList(DATE_COL));
        return new RangePartitionItem(Range.closedOpen(lowerKey, upperKey));
    }
}
