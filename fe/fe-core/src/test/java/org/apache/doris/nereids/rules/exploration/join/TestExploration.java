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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.exploration.join.JoinCommuteHelper.SwapType;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExploration extends SSBTestBase {

    /**
     *  before commute:
     *
     * <pre>
     * Group[GroupId#5]
     * +--logicalExpressions
     *    +--LogicalProject ( projects=[lo_orderkey#0, lo_linenumber#1, lo_custkey#2, lo_partkey#3, lo_suppkey#4, lo_orderdate#5, lo_orderpriority#6, lo_shippriority#7, lo_quantity#8, lo_extendedprice#9, lo_ordtotalprice#10, lo_discount#11, lo_revenue#12, lo_supplycost#13, lo_tax#14, lo_commitdate#15, lo_shipmode#16, p_partkey#17, p_name#18, p_mfgr#19, p_category#20, p_brand#21, p_color#22, p_type#23, p_size#24, p_container#25] )
     *       +--Group[GroupId#4]
     *          +--logicalExpressions
     *             +--LogicalJoin ( type=INNER_JOIN, hashJoinCondition=[], otherJoinCondition=Optional[(lo_partkey#3 = p_partkey#17)] )
     *                |--Group[GroupId#0]
     *                |  +--logicalExpressions
     *                |     +--LogicalOlapScan ( qualified=default_cluster:test.lineorder, output=[lo_orderkey#0, lo_linenumber#1, lo_custkey#2, lo_partkey#3, lo_suppkey#4, lo_orderdate#5, lo_orderpriority#6, lo_shippriority#7, lo_quantity#8, lo_extendedprice#9, lo_ordtotalprice#10, lo_discount#11, lo_revenue#12, lo_supplycost#13, lo_tax#14, lo_commitdate#15, lo_shipmode#16] )
     *                +--Group[GroupId#2]
     *                   +--logicalExpressions
     *                      +--LogicalOlapScan ( qualified=default_cluster:test.part, output=[p_partkey#17, p_name#18, p_mfgr#19, p_category#20, p_brand#21, p_color#22, p_type#23, p_size#24, p_container#25] )
     * </pre>
     *
     *
     * after:
     *
     * <pre>
     * Group[GroupId#5]
     * +--logicalExpressions
     *    +--LogicalProject ( projects=[lo_orderkey#0, lo_linenumber#1, lo_custkey#2, lo_partkey#3, lo_suppkey#4, lo_orderdate#5, lo_orderpriority#6, lo_shippriority#7, lo_quantity#8, lo_extendedprice#9, lo_ordtotalprice#10, lo_discount#11, lo_revenue#12, lo_supplycost#13, lo_tax#14, lo_commitdate#15, lo_shipmode#16, p_partkey#17, p_name#18, p_mfgr#19, p_category#20, p_brand#21, p_color#22, p_type#23, p_size#24, p_container#25] )
     *       +--Group[GroupId#4]
     *          +--logicalExpressions
     *             |--LogicalJoin ( type=INNER_JOIN, hashJoinCondition=[], otherJoinCondition=Optional[(lo_partkey#3 = p_partkey#17)] )
     *             |  |--Group[GroupId#0]
     *             |  |  +--logicalExpressions
     *             |  |     +--LogicalOlapScan ( qualified=default_cluster:test.lineorder, output=[lo_orderkey#0, lo_linenumber#1, lo_custkey#2, lo_partkey#3, lo_suppkey#4, lo_orderdate#5, lo_orderpriority#6, lo_shippriority#7, lo_quantity#8, lo_extendedprice#9, lo_ordtotalprice#10, lo_discount#11, lo_revenue#12, lo_supplycost#13, lo_tax#14, lo_commitdate#15, lo_shipmode#16] )
     *             |  +--Group[GroupId#2]
     *             |     +--logicalExpressions
     *             |        +--LogicalOlapScan ( qualified=default_cluster:test.part, output=[p_partkey#17, p_name#18, p_mfgr#19, p_category#20, p_brand#21, p_color#22, p_type#23, p_size#24, p_container#25] )
     *             +--LogicalJoin ( type=INNER_JOIN, hashJoinCondition=[], otherJoinCondition=Optional[(lo_partkey#3 = p_partkey#17)] )
     *                |--Group[GroupId#2]
     *                |  +--logicalExpressions
     *                |     +--LogicalOlapScan ( qualified=default_cluster:test.part, output=[p_partkey#17, p_name#18, p_mfgr#19, p_category#20, p_brand#21, p_color#22, p_type#23, p_size#24, p_container#25] )
     *                +--Group[GroupId#0]
     *                   +--logicalExpressions
     *                      +--LogicalOlapScan ( qualified=default_cluster:test.lineorder, output=[lo_orderkey#0, lo_linenumber#1, lo_custkey#2, lo_partkey#3, lo_suppkey#4, lo_orderdate#5, lo_orderpriority#6, lo_shippriority#7, lo_quantity#8, lo_extendedprice#9, lo_ordtotalprice#10, lo_discount#11, lo_revenue#12, lo_supplycost#13, lo_tax#14, lo_commitdate#15, lo_shipmode#16] )
     * </pre>
     */
    @Test
    public void testCommute() {
        PlanChecker.from(connectContext)
                .analyze("select * from lineorder l join part p on l.lo_partkey = p.p_partkey")
                .checkGroupNum(4)
                .checkGroupExpressionNum(4)
                .transform(new JoinCommute(false, SwapType.ALL).build())
                .checkGroupNum(4)
                .checkGroupExpressionNum(5)
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Group joinGroup = root.getLogicalExpression().child(0);
                    Assertions.assertEquals(2, joinGroup.getLogicalExpressions().size());

                    GroupExpression originJoin = joinGroup.getLogicalExpressions().get(0);
                    GroupExpression commuteJoin = joinGroup.getLogicalExpressions().get(1);

                    Assertions.assertEquals(originJoin.child(0), commuteJoin.child(1));
                    Assertions.assertEquals(originJoin.child(1), commuteJoin.child(0));
                });
    }
}
