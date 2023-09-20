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

import org.apache.doris.nereids.pattern.GeneratedPlanPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class UnCorrelatedApplyProjectFilterTest extends TestWithFeService implements GeneratedPlanPatterns {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("testApplyProjectFilter");
        connectContext.setDatabase("default_cluster:testApplyProjectFilter");
        createTables(
                "CREATE TABLE t1 (c1 int, c2 int) DISTRIBUTED BY HASH(c1)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");",
                "CREATE TABLE t2 (c1 int, c2 int) DISTRIBUTED BY HASH(c1)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");"
        );
    }

    @Test
    void testCorrelatedExistsProjectFilter1() {
        String sql = "select * from t1 where exists (select * from t2 where t2.c2 > t1.c2)";
        PlanChecker.from(connectContext)
                .parse(sql)
                .analyze()
                .applyBottomUp(new UnCorrelatedApplyProjectFilter())
                .matchesFromRoot(logicalResultSink(logicalProject(logicalFilter(
                        logicalProject(logicalApply(logicalOlapScan(), logicalProject(logicalOlapScan())))))));
    }

    @Test
    void testCorrelatedExistsProjectFilter2() {
        String sql = "select * from t1 where exists (select * from t2 where t2.c2 > t1.c2 and t2.c1 > 0)";
        PlanChecker.from(connectContext)
                .parse(sql)
                .analyze()
                .applyBottomUp(new UnCorrelatedApplyProjectFilter())
                .matchesFromRoot(logicalResultSink(logicalProject(logicalFilter(
                        logicalProject(logicalApply(logicalOlapScan(), logicalProject(logicalFilter(logicalOlapScan()))))))));
    }

    @Test
    void testCorrelatedInProjectFilter1() {
        String sql = "select * from t1 where t1.c2 in (select t2.c2 from t2 where t2.c1 > t1.c1)";
        PlanChecker.from(connectContext)
                .parse(sql)
                .analyze()
                .applyBottomUp(new UnCorrelatedApplyProjectFilter())
                .matchesFromRoot(logicalResultSink(logicalProject(logicalFilter(
                        logicalProject(logicalApply(logicalOlapScan(), logicalProject(logicalOlapScan())))))));
    }

    @Test
    void testCorrelatedInProjectFilter2() {
        String sql = "select * from t1 where t1.c2 in (select t2.c2 from t2 where t2.c1 > t1.c1 and t2.c2 > 0)";
        PlanChecker.from(connectContext)
                .parse(sql)
                .analyze()
                .applyBottomUp(new UnCorrelatedApplyProjectFilter())
                .matchesFromRoot(logicalResultSink(logicalProject(logicalFilter(
                        logicalProject(logicalApply(logicalOlapScan(), logicalProject(logicalFilter(logicalOlapScan()))))))));
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }
}
