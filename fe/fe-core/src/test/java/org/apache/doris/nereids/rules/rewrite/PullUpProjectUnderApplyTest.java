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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class PullUpProjectUnderApplyTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T (\n"
                        + "    id bigint,\n"
                        + "    score bigint,\n"
                        + "    score_int int\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    @Test
    void testPullUpProjectUnderApply() {
        List<String> testSql = ImmutableList.of(
                "select * from T as T1 where id = (select max(id) from T as T2 where T1.score = T2.score)",
                "select * from T as T1 where id = (select max(id) + 1 from T as T2 where T1.score = T2.score)"
        );

        testSql.forEach(sql ->
                PlanChecker.from(connectContext)
                        .analyze(sql)
                        .applyTopDown(new PullUpProjectUnderApply())
                        .printlnTree()
                        .matchesNotCheck(
                                logicalApply(
                                        logicalSubQueryAlias(),
                                        logicalAggregate()
                                )
                        )
        );
    }

    @Test
    void testScalarTwoColumn() {
        String sql = "select * from T as T1 where id = (select max(id), score from T as T2 where T1.score = T2.score)";
        Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
        );
    }
}
