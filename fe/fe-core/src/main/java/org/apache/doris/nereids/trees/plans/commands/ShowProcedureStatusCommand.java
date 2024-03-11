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
 * show procedure status command
 */
public class ShowProcedureStatusCommand extends Command implements NoForward {

    public static final Logger LOG = LogManager.getLogger(ShowProcedureStatusCommand.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("ProcedureName")
                                                                .add("CatalogId").add("DbId").add("PackageName")
                                                                .add("OwnerName").add("CreateTime").add("ModifyTime")
                                                                .build();

    /**
     * constructor
     */
    public ShowProcedureStatusCommand() {
        super(PlanType.SHOW_PROCEDURE_COMMAND);
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
        ctx.getPlSqlOperation().getExec().functions.showProcedure(results);
        if (!results.isEmpty()) {
            ShowResultSet commonResultSet = new ShowResultSet(getMetaData(), results);
            executor.sendResultSet(commonResultSet);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowProcedureStatusCommand(this, context);
    }
}
