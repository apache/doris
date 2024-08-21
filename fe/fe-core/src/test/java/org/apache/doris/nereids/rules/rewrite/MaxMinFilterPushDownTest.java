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

public class MaxMinFilterPushDownTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("CREATE TABLE IF NOT EXISTS max_t(\n"
                + "`id` int(32),\n"
                + "`score` int(64) NULL,\n"
                + "`name` varchar(64) NULL\n"
                + ") properties('replication_num'='1');");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    public void testMaxRewrite() {
        String sql = "select id, max(score) from max_t group by id having max(score)>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .matches(logicalFilter(logicalOlapScan()).when(filter -> filter.getConjuncts().size() == 1));
    }

    @Test
    public void testMinRewrite() {
        String sql = "select id, min(score) from max_t group by id having min(score)<10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .matches(logicalFilter(logicalOlapScan()).when(filter -> filter.getConjuncts().size() == 1));
    }

    @Test
    public void testNotRewriteBecauseFuncIsMoreThanOne1() {
        String sql = "select id, min(score), max(name) from max_t group by id having min(score)<10 and max(name)>'abc'";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }

    @Test
    public void testNotRewriteBecauseFuncIsMoreThanOne2() {
        String sql = "select id, min(score), min(name) from max_t group by id having min(score)<10 and min(name)<'abc'";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }

    @Test
    public void testMaxNotRewriteBecauseLessThan() {
        String sql = "select id, max(score) from max_t group by id having max(score)<10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }

    @Test
    public void testMinNotRewriteBecauseGreaterThan() {
        String sql = "select id, min(score) from max_t group by id having min(score)>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }

    @Test
    public void testMinNotRewriteBecauseHasMaxFunc() {
        String sql = "select id, min(score), max(score) from max_t group by id having min(score)<10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }

    @Test
    public void testMinNotRewriteBecauseHasCountFunc() {
        String sql = "select id, min(score), count(score) from max_t group by id having min(score)<10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }

    @Test
    public void testNotRewriteBecauseConjunctLeftNotSlot() {
        String sql = "select id, max(score) from max_t group by id having abs(max(score))>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }

    @Test
    public void testRewriteAggFuncHasExpr() {
        String sql = "select id, max(score+1) from max_t group by id having max(score+1)>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .matches(logicalFilter(logicalOlapScan()).when(filter -> filter.getConjuncts().size() == 1));
    }

    @Test
    public void testNotRewriteScalarAgg() {
        String sql = "select max(score+1) from max_t having max(score+1)>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }
}
