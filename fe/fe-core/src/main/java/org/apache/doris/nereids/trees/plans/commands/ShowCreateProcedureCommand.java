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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.FuncNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * show create procedure command
 */
public class ShowCreateProcedureCommand extends Command implements NoForward {

    public static final Logger LOG = LogManager.getLogger(ShowCreateProcedureCommand.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("Procedure")
                                                                .add("Create Procedure").build();
    private final FuncNameInfo procedureName;
    /**
     * constructor
     */

    public ShowCreateProcedureCommand(FuncNameInfo procedureName) {
        super(PlanType.SHOW_PROCEDURE_COMMAND);
        this.procedureName = procedureName;

    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createStringType()));
        }
        return builder.build();
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> results = new ArrayList<>();
        ctx.getPlSqlOperation().getExec().functions.showCreateProcedure(this.procedureName, results);
        ShowResultSet commonResultSet = new ShowResultSet(getMetaData(), results);
        executor.sendResultSet(commonResultSet);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateProcedureCommand(this, context);
    }
}
