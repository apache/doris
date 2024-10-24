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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;
import java.util.Optional;

/**
 * delete from unique key table.
 */
public class DeleteFromUsingCommand extends DeleteFromCommand {
    private final Optional<LogicalPlan> cte;

    /**
     * constructor
     */
    public DeleteFromUsingCommand(List<String> nameParts, String tableAlias,
            boolean isTempPart, List<String> partitions, LogicalPlan logicalQuery, Optional<LogicalPlan> cte) {
        super(nameParts, tableAlias, isTempPart, partitions, logicalQuery);
        this.cte = cte;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (ctx.getSessionVariable().isInDebugMode()) {
            throw new AnalysisException("Delete is forbidden since current session is in debug mode."
                    + " Please check the following session variables: "
                    + ctx.getSessionVariable().printDebugModeVariables());
        }
        // NOTE: delete from using command is executed as insert command, so txn insert can support it
        new InsertIntoTableCommand(completeQueryPlan(ctx, logicalQuery), Optional.empty(), Optional.empty(),
                Optional.empty()).run(ctx, executor);
    }

    @Override
    protected LogicalPlan handleCte(LogicalPlan logicalPlan) {
        if (cte.isPresent()) {
            logicalPlan = ((LogicalPlan) cte.get().withChildren(logicalPlan));
        }
        return logicalPlan;
    }

    /**
     * for test
     */
    public LogicalPlan getLogicalQuery() {
        return logicalQuery;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDeleteFromUsingCommand(this, context);
    }

    @Override
    protected void checkTargetTable(OlapTable targetTable) {
        if (targetTable.getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("delete command on with using clause only supports unique key model");
        }
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DELETE;
    }
}
