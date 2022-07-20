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

import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.BindFunction;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.BindSlotReference;
import org.apache.doris.nereids.rules.analysis.ProjectToGlobalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

/**
 * Analyzer for unit test.
 * // TODO: unify the logic with ones in production files.
 */
public class TestAnalyzer {
    private final ConnectContext connectContext;

    public TestAnalyzer(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    /**
     * Try to analyze a SQL into analyzed LogicalPlan.
     */
    public LogicalPlan analyze(String sql) {
        NereidsParser parser = new NereidsParser();
        LogicalPlan parsed = parser.parseSingle(sql);
        return analyze(parsed);
    }

    private LogicalPlan analyze(LogicalPlan inputPlan) {
        return (LogicalPlan) new Memo(inputPlan)
                .newPlannerContext(connectContext)
                .setDefaultJobContext()
                .bottomUpRewrite(new BindFunction())
                .bottomUpRewrite(new BindRelation())
                .bottomUpRewrite(new BindSlotReference())
                .bottomUpRewrite(new ProjectToGlobalAggregate())
                .getMemo()
                .copyOut();
    }
}
