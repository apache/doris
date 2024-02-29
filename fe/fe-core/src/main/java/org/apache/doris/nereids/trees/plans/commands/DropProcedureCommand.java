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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.FuncNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.plsql.metastore.PlsqlMetaClient;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * Drop table procedure
 */
@Developing
public class DropProcedureCommand extends Command implements ForwardWithSync {
    public static final Logger LOG = LogManager.getLogger(DropProcedureCommand.class);
    private final FuncNameInfo procedureName;
    private final String source; // Original SQL, from LogicalPlanBuilder.getOriginSql()
    private final PlsqlMetaClient client;

    /**
     * constructor
     */
    public DropProcedureCommand(FuncNameInfo procedureName, String source) {
        super(PlanType.DROP_PROCEDURE_COMMAND);
        this.client = new PlsqlMetaClient();
        this.procedureName = Objects.requireNonNull(procedureName, "procedureName is null");
        this.source = Objects.requireNonNull(source, "source is null");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        ctx.getPlSqlOperation().getExec().functions.remove(procedureName);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropProcedureCommand(this, context);
    }
}
