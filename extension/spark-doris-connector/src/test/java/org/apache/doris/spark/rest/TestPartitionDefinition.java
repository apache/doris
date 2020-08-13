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
