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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PartitionColumnFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

class PruneOlapScanTabletTest extends SqlTestBase implements MemoPatternMatchSupported {

    @Test
    void testPruneOlapScanTablet(@Mocked OlapTable olapTable,
            @Mocked Partition partition, @Mocked MaterializedIndex index,
            @Mocked HashDistributionInfo distributionInfo) {
        List<Long> tabletIds = Lists.newArrayListWithExpectedSize(300);
        for (long i = 0; i < 300; i++) {
            tabletIds.add(i);
        }

        List<Column> columns = Lists.newArrayList(
                new Column("k0", PrimitiveType.DATE, false),
                new Column("k1", PrimitiveType.INT, false),
                new Column("k2", PrimitiveType.INT, false),
                new Column("k3", PrimitiveType.INT, false),
                new Column("k4", PrimitiveType.INT, false)
        );

        PartitionColumnFilter k0Filter = new PartitionColumnFilter();
        k0Filter.setLowerBound(new StringLiteral("2019-08-22"), true);
        k0Filter.setUpperBound(new StringLiteral("2019-08-22"), true);

        PartitionColumnFilter k1Filter = new PartitionColumnFilter();
        List<Expr> inList = ImmutableList.of(
                new IntLiteral(100),
                new IntLiteral(200),
                new IntLiteral(300),
                new IntLiteral(400),
                new IntLiteral(500));
        k1Filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, "k1"), inList, false));

        PartitionColumnFilter k2Filter = new PartitionColumnFilter();
        List<Expr> inList2 = ImmutableList.of(
                new IntLiteral(900),
                new IntLiteral(1100));
        k2Filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, "k2"), inList2, false));

        PartitionColumnFilter k3Filter = new PartitionColumnFilter();
        List<Expr> inList3 = ImmutableList.of(
                new IntLiteral(1),
                new IntLiteral(3));
        k3Filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, "k3"), inList3, false));

        PartitionColumnFilter k4Filter = new PartitionColumnFilter();
        List<Expr> inList4 = Lists.newArrayList();
        inList4.add(new IntLiteral(2));
        k4Filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, "k4"), inList4, false));

        new Expectations() {
            {
                olapTable.getPartitionIds();
                result = ImmutableList.of(1L);

                olapTable.getBaseSchema(true);
                result = columns;

                olapTable.getName();
                result = "t1";
                olapTable.getPartition(anyLong);
                result = partition;
                partition.getIndex(anyLong);
                result = index;
                partition.getDistributionInfo();
                result = distributionInfo;
                index.getTabletIdsInOrder();
                result = tabletIds;
                distributionInfo.getDistributionColumns();
                result = columns;
                distributionInfo.getType();
                result = DistributionInfo.DistributionInfoType.HASH;
                distributionInfo.getBucketNum();
                result = tabletIds.size();
            }
        };

        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), olapTable);

        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(scan.getOutput().get(0),
                new DateLiteral("2019-08-22"));
        LessThanEqual lessThanEqual = new LessThanEqual(scan.getOutput().get(0), new DateLiteral("2019-08-22"));
        InPredicate inPredicate1 = new InPredicate(scan.getOutput().get(1), ImmutableList.of(Literal.of(101),
                Literal.of(201), Literal.of(301), Literal.of(401), Literal.of(500)));
        InPredicate inPredicate2 = new InPredicate(scan.getOutput().get(2), ImmutableList.of(Literal.of(901),
                Literal.of(1101)));
        InPredicate inPredicate3 = new InPredicate(scan.getOutput().get(3), ImmutableList.of(Literal.of(1),
                Literal.of(3)));
        EqualTo equalTo = new EqualTo(scan.getOutput().get(4), Literal.of(10));
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(greaterThanEqual, lessThanEqual, inPredicate1, inPredicate2, inPredicate3, equalTo),
                scan);

        Assertions.assertEquals(0, filter.child().getSelectedTabletIds().size());

        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PruneOlapScanTablet())
                .matches(
                        logicalFilter(
                                logicalOlapScan().when(s -> s.getSelectedTabletIds().size() == 19)
                        )
                );
    }

    @Test
    void testPruneOlapScanTabletWithManually() {
        String sql = "select * from T4 TABLET(110) where id > 8";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyTopDown(new PruneOlapScanTablet())
                .matches(
                        logicalFilter(
                                logicalOlapScan().when(s ->
                                        Objects.equals(s.getSelectedTabletIds(), Lists.newArrayList(110L))
                                                && Objects.equals(s.getManuallySpecifiedTabletIds(),
                                                Lists.newArrayList(110L))
                                )
                        )
                );
    }
}
