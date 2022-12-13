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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class AnalyzeFunctionTest extends TestWithFeService {

    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables("CREATE TABLE t1 (col1 date, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                + "    \"replication_num\"=\"1\"\n" + ");");
    }

    @Test
    public void testTimeArithmExpr() {
        String sql = "SELECT * FROM t1 WHERE col1 < date '1994-01-01' + interval '1' year";
        LogicalPlan logicalPlan = (LogicalPlan) PlanChecker.from(connectContext)
                .analyze(sql).getCascadesContext().getMemo().copyOut();
        Assertions.assertTrue(logicalPlan
                .<Set<LogicalFilter>>collect(LogicalFilter.class::isInstance)
                .stream().map(f -> f.getPredicates()).noneMatch(VarcharLiteral.class::isInstance));
    }
}
