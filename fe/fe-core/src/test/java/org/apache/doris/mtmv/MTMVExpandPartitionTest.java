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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link MTMVPartitionExpander#expandToMvPartitionGranularity}.
 * Scenario: base table has daily RANGE partitions, MV has monthly RANGE partitions.
 * The expansion should map a queried daily partition to ALL daily partitions within
 * the same MV (monthly) partition range, ensuring complete partition mappings for
 * isSyncWithPartitions correctness while reducing the number of partitions processed
 * by the rollup pipeline.
 * Uses Java dynamic proxy to create lightweight MTMVRelatedTableIf mocks
 * without triggering class-loading of the full TableIf hierarchy.
 */
public class MTMVExpandPartitionTest {

    private static final Column DATE_COL = new Column("c1", ScalarType.createType(PrimitiveType.DATE),
            true, null, "", "");

    private static final List<String> RANGE_TABLE_QUALIFIERS = Lists.newArrayList("internal", "db", "daily_base");
    private static final List<String> LIST_TABLE_QUALIFIERS = Lists.newArrayList("internal", "db", "list_base");

    private MTMVRelatedTableIf rangeTable;
    private MTMVRelatedTableIf listTable;

    private Map<String, PartitionItem> dailyBasePartitions;
    private Map<String, PartitionItem> monthlyMvPartitions;

    @Before
    public void setUp() throws Exception {
        dailyBasePartitions = Maps.newHashMap();
        dailyBasePartitions.put("p20210101", buildRange("2021-01-01", "2021-01-02"));
        dailyBasePartitions.put("p20210102", buildRange("2021-01-02", "2021-01-03"));
        dailyBasePartitions.put("p20210103", buildRange("2021-01-03", "2021-01-04"));
        dailyBasePartitions.put("p20210201", buildRange("2021-02-01", "2021-02-02"));
        dailyBasePartitions.put("p20210202", buildRange("2021-02-02", "2021-02-03"));

        monthlyMvPartitions = Maps.newHashMap();
        monthlyMvPartitions.put("mv_202101", buildRange("2021-01-01", "2021-02-01"));
        monthlyMvPartitions.put("mv_202102", buildRange("2021-02-01", "2021-03-01"));

        rangeTable = createMockTable(RANGE_TABLE_QUALIFIERS, PartitionType.RANGE, dailyBasePartitions);
        listTable = createMockTable(LIST_TABLE_QUALIFIERS, PartitionType.LIST, Maps.newHashMap());
    }

    @Test
    public void testExpandSinglePartitionToMonth() throws Exception {
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(RANGE_TABLE_QUALIFIERS, Sets.newHashSet("p20210102"));

        Map<List<String>, Set<String>> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                queryUsed, monthlyMvPartitions, Sets.newHashSet(rangeTable));

        Set<String> expanded = result.get(RANGE_TABLE_QUALIFIERS);
        Assert.assertNotNull(expanded);
        Assert.assertEquals(Sets.newHashSet("p20210101", "p20210102", "p20210103"), expanded);
    }

    @Test
    public void testExpandMultipleMonths() throws Exception {
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(RANGE_TABLE_QUALIFIERS, Sets.newHashSet("p20210101", "p20210202"));

        Map<List<String>, Set<String>> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                queryUsed, monthlyMvPartitions, Sets.newHashSet(rangeTable));

        Set<String> expanded = result.get(RANGE_TABLE_QUALIFIERS);
        Assert.assertNotNull(expanded);
        Assert.assertEquals(
                Sets.newHashSet("p20210101", "p20210102", "p20210103", "p20210201", "p20210202"),
                expanded);
    }

    @Test
    public void testListPartitionPassthrough() throws Exception {
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(LIST_TABLE_QUALIFIERS, Sets.newHashSet("p1"));

        Map<List<String>, Set<String>> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                queryUsed, monthlyMvPartitions, Sets.newHashSet(listTable));

        Set<String> expanded = result.get(LIST_TABLE_QUALIFIERS);
        Assert.assertNotNull(expanded);
        Assert.assertEquals(Sets.newHashSet("p1"), expanded);
    }

    @Test
    public void testNonExistentPartition() throws Exception {
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(RANGE_TABLE_QUALIFIERS, Sets.newHashSet("p_nonexistent"));

        Map<List<String>, Set<String>> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                queryUsed, monthlyMvPartitions, Sets.newHashSet(rangeTable));

        Set<String> expanded = result.get(RANGE_TABLE_QUALIFIERS);
        Assert.assertNotNull(expanded);
        Assert.assertTrue(expanded.isEmpty());
    }

    @Test
    public void testBasePartitionOutsideMvRange() throws Exception {
        Map<String, PartitionItem> janOnlyMv = Maps.newHashMap();
        janOnlyMv.put("mv_202101", buildRange("2021-01-01", "2021-02-01"));

        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(RANGE_TABLE_QUALIFIERS, Sets.newHashSet("p20210101"));

        Map<List<String>, Set<String>> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                queryUsed, janOnlyMv, Sets.newHashSet(rangeTable));

        Set<String> expanded = result.get(RANGE_TABLE_QUALIFIERS);
        Assert.assertNotNull(expanded);
        Assert.assertEquals(Sets.newHashSet("p20210101", "p20210102", "p20210103"), expanded);
    }

    @Test
    public void testPctTableNotInFilter() throws Exception {
        Map<List<String>, Set<String>> queryUsed = Maps.newHashMap();
        queryUsed.put(Lists.newArrayList("internal", "db", "other_table"),
                Sets.newHashSet("p20210101"));

        Map<List<String>, Set<String>> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                queryUsed, monthlyMvPartitions, Sets.newHashSet(rangeTable));

        Assert.assertNull(result.get(RANGE_TABLE_QUALIFIERS));
    }

    @Test
    public void testEmptyFilter() throws Exception {
        Map<List<String>, Set<String>> result = MTMVPartitionExpander.expandToMvPartitionGranularity(
                Maps.newHashMap(), monthlyMvPartitions, Sets.newHashSet(rangeTable));

        Assert.assertTrue(result.isEmpty());
    }

    // --- helpers ---

    private static MTMVRelatedTableIf createMockTable(List<String> qualifiers,
            PartitionType partitionType, Map<String, PartitionItem> partitionItems) {
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "getFullQualifiers":
                    return qualifiers;
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
                MTMVRelatedTableIf.class.getClassLoader(),
                new Class<?>[] {MTMVRelatedTableIf.class},
                handler);
    }

    private static RangePartitionItem buildRange(String lower, String upper) throws AnalysisException {
        PartitionKey lowerKey = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(lower)), Lists.newArrayList(DATE_COL));
        PartitionKey upperKey = PartitionKey.createPartitionKey(
                Lists.newArrayList(new PartitionValue(upper)), Lists.newArrayList(DATE_COL));
        return new RangePartitionItem(Range.closedOpen(lowerKey, upperKey));
    }
}
