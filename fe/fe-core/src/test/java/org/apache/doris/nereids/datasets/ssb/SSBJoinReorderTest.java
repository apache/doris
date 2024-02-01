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

package org.apache.doris.nereids.datasets.ssb;

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SSBJoinReorderTest extends SSBTestBase implements MemoPatternMatchSupported {
    @Test
    public void q4_1() {
        test(
                SSBUtils.Q4_1,
                ImmutableList.of(
                        "(lo_orderdate = d_datekey)",
                        "(lo_custkey = c_custkey)",
                        "(lo_suppkey = s_suppkey)",
                        "(lo_partkey = p_partkey)"
                ),
                ImmutableList.of(
                        "(c_region = 'AMERICA')",
                        "(s_region = 'AMERICA')",
                        "p_mfgr IN ('MFGR#1', 'MFGR#2')"
                )
        );
    }

    @Test
    public void q4_2() {
        test(
                SSBUtils.Q4_2,
                ImmutableList.of(
                        "(lo_orderdate = d_datekey)",
                        "(lo_custkey = c_custkey)",
                        "(lo_suppkey = s_suppkey)",
                        "(lo_partkey = p_partkey)"
                ),
                ImmutableList.of(
                        "d_year IN (1997, 1998)",
                        "(c_region = 'AMERICA')",
                        "(s_region = 'AMERICA')",
                        "p_mfgr IN ('MFGR#1', 'MFGR#2')"
                )
        );
    }

    @Test
    public void q4_3() {
        test(
                SSBUtils.Q4_3,
                ImmutableList.of(
                        "(lo_orderdate = d_datekey)",
                        "(lo_custkey = c_custkey)",
                        "(lo_suppkey = s_suppkey)",
                        "(lo_partkey = p_partkey)"
                ),
                ImmutableList.of(
                        "d_year IN (1997, 1998)",
                        "(s_nation = 'UNITED STATES')",
                        "(p_category = 'MFGR#14')"
                )
        );
    }

    private void test(String sql, List<String> expectJoinConditions, List<String> expectFilterPredicates) {
        PlanChecker planChecker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree();

        for (String expectJoinCondition : expectJoinConditions) {
            planChecker.matches(
                    innerLogicalJoin().when(
                            join -> join.getHashJoinConjuncts().get(0).toSql().equals(expectJoinCondition))
            );
        }

        for (String expectFilterPredicate : expectFilterPredicates) {
            planChecker.matches(
                    logicalFilter().when(filter -> filter.getPredicate().toSql().equals(expectFilterPredicate))
            );
        }
    }
}
