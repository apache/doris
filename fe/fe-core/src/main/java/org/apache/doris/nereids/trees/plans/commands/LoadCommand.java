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

import org.apache.doris.analysis.BulkLoadDataDesc;
import org.apache.doris.analysis.BulkStorageDesc;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * export table
 */
public class LoadCommand extends Command {
    private String labelName;
    private BulkStorageDesc brokerDesc;
    private List<BulkLoadDataDesc> sourceInfos;
    private Map<String, String> properties;
    private String comment;

    /**
     * constructor of ExportCommand
     */
    public LoadCommand(String labelName, List<BulkLoadDataDesc> sourceInfos, BulkStorageDesc brokerDesc,
                       Map<String, String> properties, String comment) {
        super(PlanType.LOAD_COMMAND);
        this.labelName = labelName.trim();
        this.sourceInfos = sourceInfos;
        this.properties = properties;
        this.brokerDesc = brokerDesc;
        this.comment = comment;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<LogicalPlan> plans = new ArrayList<>();
        // 1. to insert
        //      1.1 build table sink info
        //      1.2 build select sql, and parse sql to query context
        //      1.3 build sink, and put to insert context
        for (BulkLoadDataDesc dataDesc : sourceInfos) {
            dataDesc.toSql();
            Map<String, String> props = getTvfProperties(brokerDesc, properties);
            String dataTvfSql = dataDesc.toInsertSql(props);
            List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(dataTvfSql);
            // TODO: check if tvf plan legal
            plans.add(statements.get(0).first);
        }
        // 2. execute insert stmt
        executeInsertStmtPlan(plans);
    }

    private Map<String, String> getTvfProperties(BulkStorageDesc brokerDesc, Map<String, String> properties) {
        return new HashMap<>();
    }

    private void executeInsertStmtPlan(List<LogicalPlan> plans) {}

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLoadCommand(this, context);
    }
}
