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

class EliminateSemiJoinTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        connectContext.setDatabase("test");

        createTable("CREATE TABLE t ("
                + "id int not null"
                + ")\n"
                + "DISTRIBUTED BY HASH(id)\n"
                + "BUCKETS 1\n"
                + "PROPERTIES(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ");");
    }

    @Test
    void leftSemiTrue() {
        String sql = "select * from t t1 left semi join t t2 on true";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new EliminateSemiJoin())
                .matches(
                        logicalResultSink(
                                logicalProject(logicalJoin())
                        )
                );
    }

    @Test
    void leftSemiFalse() {
        String sql = "select * from t t1 left semi join t t2 on false";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new EliminateSemiJoin())
                .matches(
                        logicalEmptyRelation()
                );
    }

    @Test
    void leftAntiTrue() {
        String sql = "select * from t t1 left anti join t t2 on true";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new EliminateSemiJoin())
                .matches(
                        logicalJoin(logicalSubQueryAlias(), logicalSubQueryAlias())
                );
    }

    @Test
    void leftAntiFalse() {
        String sql = "select * from t t1 left anti join t t2 on false";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new EliminateSemiJoin())
                .matches(
                        logicalResultSink(
                                logicalProject(logicalSubQueryAlias(logicalOlapScan()))
                        )
                );
    }

    @Test
    void rightSemiTrue() {
        String sql = "select * from t t1 right semi join t t2 on true";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new EliminateSemiJoin())
                .matches(
                        logicalResultSink(
                                logicalProject(logicalJoin())
                        )
                );
    }

    @Test
    void rightSemiFalse() {
        String sql = "select * from t t1 right semi join t t2 on false";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new EliminateSemiJoin())
                .matches(
                        logicalEmptyRelation()
                );
    }

    @Test
    void rightAntiTrue() {
        String sql = "select * from t t1 right anti join t t2 on true";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new EliminateSemiJoin())
                .matches(
                        logicalJoin(logicalSubQueryAlias(), logicalSubQueryAlias())
                );
    }

    @Test
    void rightAntiFalse() {
        String sql = "select * from t t1 right anti join t t2 on false";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new EliminateSemiJoin())
                .matches(
                        logicalResultSink(
                                logicalProject(logicalSubQueryAlias(logicalOlapScan()))
                        )
                );
    }
}
