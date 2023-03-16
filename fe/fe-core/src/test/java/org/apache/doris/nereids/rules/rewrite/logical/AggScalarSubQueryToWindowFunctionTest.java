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

import org.apache.doris.nereids.datasets.tpch.TPCHTestBase;
import org.apache.doris.nereids.jobs.batch.ApplyToJoin;
import org.apache.doris.nereids.jobs.batch.CorrelateApplyToUnCorrelateApply;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.stream.Collectors;

public class AggScalarSubQueryToWindowFunctionTest extends TPCHTestBase implements MemoPatternMatchSupported {
    @Test
    public void testRewriteSubQueryToWindowFunction() {
        String sql = "SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly\n"
                + "    FROM lineitem, part\n"
                + "    WHERE p_partkey = l_partkey AND\n"
                + "    p_brand = 'Brand#23' AND\n"
                + "    p_container = 'MED BOX' AND\n"
                + "    l_quantity<(SELECT 0.2*avg(l_quantity)\n"
                + "    FROM lineitem\n"
                + "    WHERE l_partkey = p_partkey);";
        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .applyTopDown(new CorrelateApplyToUnCorrelateApply())
                .applyTopDown(new ApplyToJoin())
                .applyTopDown(new PushFilterInsideJoin())
                .applyTopDown(new FindHashConditionForJoin())
                .applyTopDown(new AggScalarSubQueryToWindowFunction())
                .getPlan();

        System.out.println(plan.treeString());
    }
}
