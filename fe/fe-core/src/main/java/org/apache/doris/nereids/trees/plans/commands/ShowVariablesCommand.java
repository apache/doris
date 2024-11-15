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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.SetType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;

import java.util.List;

/**
 * ShowVariablesCommand
 */
public class ShowVariablesCommand extends ShowCommand {
    private static final String NAME_COL = "Variable_name";
    private static final String VALUE_COL = "Value";
    private static final String DEFAULT_VALUE_COL = "Default_Value";
    private static final String CHANGED_COL = "Changed";
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column(NAME_COL, ScalarType.createVarchar(20)))
            .addColumn(new Column(VALUE_COL, ScalarType.createVarchar(20)))
            .addColumn(new Column(DEFAULT_VALUE_COL, ScalarType.createVarchar(20)))
            .addColumn(new Column(CHANGED_COL, ScalarType.createVarchar(20)))
            .build();

    private final SetType type;
    private final String pattern;

    public ShowVariablesCommand(SetType type, String pattern) {
        super(PlanType.SHOW_VARIABLES_COMMAND);
        this.type = type == null ? SetType.DEFAULT : type;
        this.pattern = pattern;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowVariablesCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        PatternMatcher matcher = null;
        if (pattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(pattern, CaseSensibility.VARIABLES.getCaseSensibility());
        }
        List<List<String>> rows = VariableMgr.dump(type, ctx.getSessionVariable(), matcher);
        return new ShowResultSet(META_DATA, rows);
    }
}
