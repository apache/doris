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

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

/**
 * UT for {@link DecomposeRepeatWithPreAggregation}.
 */
public class DecomposeRepeatWithPreAggregationTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("decompose_repeat_with_preagg");
        createTable(
                "create table decompose_repeat_with_preagg.t1 (\n"
                        + "a int, b int, c int, d int\n"
                        + ")\n"
                        + "distributed by hash(a) buckets 1\n"
                        + "properties('replication_num' = '1');"
        );
        connectContext.setDatabase("default_cluster:decompose_repeat_with_preagg");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void rewriteRollupSumShouldGenerateCteAndUnion() {
        String sql = "select a,b,c,sum(d) from t1 group by rollup(a,b,c);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEAnchor());
    }

    @Test
    void noRewriteWhenGroupingSetsSizeLe3() {
        String sql = "select a,b,sum(d) from t1 group by rollup(a,b);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalCTEAnchor());
    }

    @Test
    void noRewriteWhenDistinctAgg() {
        String sql = "select a,b,c,sum(distinct d) from t1 group by rollup(a,b,c);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalCTEAnchor());
    }

    @Test
    void noRewriteWhenUnsupportedAgg() {
        String sql = "select a,b,c,count(d) from t1 group by rollup(a,b,c);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalCTEAnchor());

    }

    @Test
    void noRewriteWhenHasGroupingScalarFunction() {
        String sql = "select a,b,c,sum(d),grouping_id(a) from t1 group by rollup(a,b,c);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalCTEAnchor());
    }

    @Test
    void rewriteWhenMaxGroupingSetNotFirst() {
        String sql = "select a,b,c,sum(d) from t1 group by grouping sets((a),(a,b,c),(a,b),());";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEAnchor());
    }
}
