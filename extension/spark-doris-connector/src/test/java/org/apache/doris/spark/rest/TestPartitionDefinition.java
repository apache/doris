/**
 * Copyright (c) 2019.  Baidu.com, Inc. All Rights Reserved.
 * Author: zhangwenxin01
 * Date: 2019-08-07
 */

package org.apache.doris.spark.rest;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class TestPartitionDefinition {
    private static final String DATABASE_1 = "database1";
    private static final String TABLE_1 = "table1";
    private static final String BE_1 = "be1";
    private static final String QUERY_PLAN_1 = "queryPlan1";
    private static final long TABLET_ID_1 = 1L;

    private static final String DATABASE_2 = "database2";
    private static final String TABLE_2 = "table2";
    private static final String BE_2 = "be2";
    private static final String QUERY_PLAN_2 = "queryPlan2";
    private static final long TABLET_ID_2 = 2L;

    @Test
    public void testCompare() throws Exception {
        Set<Long> tabletSet1 = new HashSet<>();
        tabletSet1.add(TABLET_ID_1);
        Set<Long> tabletSet2 = new HashSet<>();
        tabletSet2.add(TABLET_ID_2);
        Set<Long> tabletSet3 = new HashSet<>();
        tabletSet3.add(TABLET_ID_1);
        tabletSet3.add(TABLET_ID_2);

        PartitionDefinition pd1 = new PartitionDefinition(
                DATABASE_1, TABLE_1, null, BE_1, tabletSet1, QUERY_PLAN_1);
        PartitionDefinition pd3 = new PartitionDefinition(
                DATABASE_2, TABLE_1, null, BE_1, tabletSet1, QUERY_PLAN_1);
        PartitionDefinition pd4 = new PartitionDefinition(
                DATABASE_1, TABLE_2, null, BE_1, tabletSet1, QUERY_PLAN_1);
        PartitionDefinition pd5 = new PartitionDefinition(
                DATABASE_1, TABLE_1, null, BE_2, tabletSet1, QUERY_PLAN_1);
        PartitionDefinition pd6 = new PartitionDefinition(
                DATABASE_1, TABLE_1, null, BE_1, tabletSet2, QUERY_PLAN_1);
        PartitionDefinition pd7 = new PartitionDefinition(
                DATABASE_1, TABLE_1, null, BE_1, tabletSet3, QUERY_PLAN_1);
        PartitionDefinition pd8 = new PartitionDefinition(
                DATABASE_1, TABLE_1, null, BE_1, tabletSet1, QUERY_PLAN_2);
        Assert.assertTrue(pd1.compareTo(pd3) < 0);
        Assert.assertTrue(pd1.compareTo(pd4) < 0);
        Assert.assertTrue(pd1.compareTo(pd5) < 0);
        Assert.assertTrue(pd1.compareTo(pd6) < 0);
        Assert.assertTrue(pd1.compareTo(pd7) < 0);
        Assert.assertTrue(pd1.compareTo(pd8) < 0);
    }
}
