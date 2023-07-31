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

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;
import java.util.Optional;

/**
 * create table command
 */
public class CreateTableCommand extends Command implements ForwardWithSync {
    private final Optional<LogicalPlan> ctasQuery;
    private final CreateTableInfo createTableInfo;

    public CreateTableCommand(Optional<LogicalPlan> ctasQuery, CreateTableInfo createTableInfo) {
        super(PlanType.CREATE_TABLE_COMMAND);
        this.ctasQuery = ctasQuery;
        this.createTableInfo = Objects.requireNonNull(createTableInfo);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        createTableInfo.validate(ctx);
        CreateTableStmt createTableStmt = createTableInfo.translateToCatalogStyle();
        try {
            Env.getCurrentEnv().createTable(createTableStmt);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        if (ctasQuery.isPresent()) {
            return;
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableCommand(this, context);
    }
}
