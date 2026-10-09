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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTVFCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFTableSink;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class BindTVFTableSinkTest {

    @Test
    void duplicateOutputNamesKeepDistinctPositions() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        InsertIntoTVFCommand command = (InsertIntoTVFCommand) new NereidsParser().parseSingle(
                "INSERT INTO local("
                        + "'file_path'='/tmp/tvf_duplicate_columns_',"
                        + "'backend_id'='1',"
                        + "'format'='csv') "
                        + "SELECT 1 AS x, 2 AS x");

        Plan analyzed = PlanChecker.from(context)
                .analyze(command.getExplainPlan(context))
                .getPlan();
        LogicalTVFTableSink<?> sink = Assertions.assertInstanceOf(LogicalTVFTableSink.class, analyzed);
        LogicalProject<?> sinkProject = Assertions.assertInstanceOf(LogicalProject.class, sink.child());
        List<NamedExpression> projects = sinkProject.getProjects();

        Assertions.assertEquals(2, projects.size());
        Assertions.assertNotEquals(projects.get(0).getExprId(), projects.get(1).getExprId(),
                "TVF sink output positions must not be collapsed by duplicate display names");
    }
}
