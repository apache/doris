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

package org.apache.doris.nereids.rules.rewrite.logical;

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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.planner.PartitionColumnFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PruneOlapScanTabletTest {

    @Test
    public void testPruneOlapScanTablet(@Mocked OlapTable olapTable,
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
        List<Expr> inList = Lists.newArrayList();
        inList.add(new IntLiteral(100));
        inList.add(new IntLiteral(200));
        inList.add(new IntLiteral(300));
        inList.add(new IntLiteral(400));
        inList.add(new IntLiteral(500));
        k1Filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, "k1"), inList, false));

        PartitionColumnFilter k2Filter = new PartitionColumnFilter();
        List<Expr> inList2 = Lists.newArrayList();
        inList2.add(new IntLiteral(900));
        inList2.add(new IntLiteral(1100));
        k2Filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, "k2"), inList2, false));

        PartitionColumnFilter k3Filter = new PartitionColumnFilter();
        List<Expr> inList3 = Lists.newArrayList();
        inList3.add(new IntLiteral(1));
        inList3.add(new IntLiteral(3));
        k3Filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, "k3"), inList3, false));

        PartitionColumnFilter k4Filter = new PartitionColumnFilter();
        List<Expr> inList4 = Lists.newArrayList();
        inList4.add(new IntLiteral(2));
        k4Filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, "k4"), inList4, false));

        SlotReference k0 = new SlotReference("k0", DataType.convertFromCatalogDataType(Type.INT), false, ImmutableList.of());
        SlotReference k1 = new SlotReference("k1", DataType.convertFromCatalogDataType(Type.INT), false, ImmutableList.of());
        SlotReference k2 = new SlotReference("k2", DataType.convertFromCatalogDataType(Type.INT), false, ImmutableList.of());
        SlotReference k3 = new SlotReference("k3", DataType.convertFromCatalogDataType(Type.INT), false, ImmutableList.of());
        SlotReference k4 = new SlotReference("k4", DataType.convertFromCatalogDataType(Type.INT), false, ImmutableList.of());

        GreaterThanEqual greaterThanEqual = new GreaterThanEqual(k0, new DateLiteral("2019-08-22"));
        LessThanEqual lessThanEqual = new LessThanEqual(k0, new DateLiteral("2019-08-22"));

        InPredicate inPredicate1 = new InPredicate(k1, ImmutableList.of(new IntegerLiteral(101),
                new IntegerLiteral(201),
                new IntegerLiteral(301),
                new IntegerLiteral(401),
                new IntegerLiteral(500)));
        InPredicate inPredicate2 = new InPredicate(k2, ImmutableList.of(new IntegerLiteral(901),
                new IntegerLiteral(1101)));
        InPredicate inPredicate3 = new InPredicate(k3, ImmutableList.of(new IntegerLiteral(1),
                new IntegerLiteral(3)));
        EqualTo equalTo = new EqualTo(k4, new IntegerLiteral(10));

        new Expectations() {
            {
                olapTable.getPartitionIds();
                result = ImmutableList.of(1L);

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

        Expression expr = ExpressionUtils.and(greaterThanEqual, lessThanEqual, inPredicate1, inPredicate2, inPredicate3, equalTo);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(expr,
                new LogicalOlapScan(RelationId.createGenerator().getNextId(), olapTable));

        Assertions.assertEquals(0, filter.child().getSelectedTabletId().size());

        CascadesContext context = MemoTestUtils.createCascadesContext(filter);
        context.topDownRewrite(ImmutableList.of(new PruneOlapScanTablet().build()));

        LogicalFilter<LogicalOlapScan> filter1 = ((LogicalFilter<LogicalOlapScan>) context.getMemo().copyOut());
        LogicalOlapScan olapScan = filter1.child();
        Assertions.assertEquals(19, olapScan.getSelectedTabletId().size());
    }
}
