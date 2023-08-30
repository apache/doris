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

import org.apache.doris.analysis.BulkDesc;
import org.apache.doris.analysis.BulkLoadDataDesc;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;
import java.util.Map;

/**
 * export table
 */
public class LoadCommand extends Command {
    private String labelName;
    private BulkDesc brokerDesc;
    private List<BulkLoadDataDesc> sinkInfos;
    private InsertIntoTableCommand loadByInsert;
    private Map<String, String> properties;
    private String comment;

    /**
     * constructor of ExportCommand
     */
    public LoadCommand(String labelName, List<BulkLoadDataDesc> sinkInfos, BulkDesc brokerDesc,
                       Map<String, String> properties, String comment) {
        super(PlanType.LOAD_COMMAND);
        this.labelName = labelName.trim();
        this.sinkInfos = sinkInfos;
        this.properties = properties;
        this.brokerDesc = brokerDesc;
        this.comment = comment;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        //        NereidsLoadStmt loadStmt = generateTvfStmt();
        //        Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
        //        InsertIntoTableCommand loadCommand = loadBatchByInsert(ctx);
        //        loadCommand.run(ctx, executor);
        // 1. to insert
        //      1.1 build table sink inf o
        //      1.2 build select sql, and parse sql to query context
        //      1.3 build sink, and put to insert context
        // 2. execute insert stmt

    }

    private InsertIntoTableCommand loadBatchByInsert(ConnectContext ctx) {
        //        for (BulkLoadDataDesc desc : sinkInfos) {
        //            List<Pair<LogicalPlan, StatementContext>> statements;
        //            String insertTvfSql = buildInsertIntoFromRemote(
        //                    new LabelName(desc.getDbName(), desc.getTableName()),
        //                    ImmutableList.of(desc), brokerDesc, properties, comment);
        //            try {
        //                statements = new NereidsParser().parseMultiple(originStmt.originStmt);
        //            } catch (Exception e) {
        //                throw new ParseException("Nereids parse failed. " + e.getMessage());
        //            }
        //        }
        return null;
    }

    private void executeInsertStmt() {

    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLoadCommand(this, context);
    }
}
