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

package org.apache.doris.nereids.jobs.joinorder;

import org.apache.doris.nereids.datasets.tpch.TPCHTestBase;
import org.apache.doris.nereids.datasets.tpch.TPCHUtils;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Test;

public class TPCHTest extends TPCHTestBase implements MemoPatternMatchSupported {
    @Test
    void testQ5() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q5)
                .rewrite()
                .deriveStats()
                .dpHypOptimize()
                .printlnBestPlanTree();
    }


    // count(*) projects on children key columns
    @Test
    void testCountStarProject() {
        String sql = "select\n"
                + "    count(*) as order_count\n"
                + "from\n"
                + "    orders\n"
                + "where\n"
                + "    o_orderdate >= date '1993-07-01'\n"
                + "    and o_orderdate < date '1993-07-01' + interval '3' month\n"
                + "    and exists (\n"
                + "        select\n"
                + "            *\n"
                + "        from\n"
                + "            lineitem\n"
                + "        where\n"
                + "            l_orderkey = o_orderkey\n"
                + "            and l_commitdate < l_receiptdate\n"
                + "    );";

        // o_orderstatus is smaller than o_orderdate, but o_orderstatus is not used in this sql
        // it is better to choose the column which is already used to represent count(*)
        PlanChecker.from(connectContext)
                .disableNereidsRules("PRUNE_EMPTY_PARTITION")
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalResultSink(
                                logicalAggregate(
                                    logicalProject().when(
                                            project -> project.getProjects().size() == 1
                                                    && project.getProjects().get(0) instanceof SlotReference
                                                    && "o_orderdate".equals(project.getProjects().get(0).toSql()))))
                );
    }
}
