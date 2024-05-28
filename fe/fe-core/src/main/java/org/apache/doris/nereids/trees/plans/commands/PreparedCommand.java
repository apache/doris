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

import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Prepared Statement
 */
public class PreparedCommand extends Command {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);

    protected List<Placeholder> params = new ArrayList<>();
    private final LogicalPlan inner;

    private final String stmtName;

    private final OriginStatement originalStmt;

    /**
     * constructor
     * @param name the statement name which represents statement id for prepared statement
     * @param plan the inner statement
     * @param placeholders the parameters for this prepared statement
     * @param originalStmt original statement from StmtExecutor
     */
    public PreparedCommand(String name, LogicalPlan plan, List<Placeholder> placeholders,
                OriginStatement originalStmt) {
        super(PlanType.UNKNOWN);
        this.inner = plan;
        if (placeholders != null) {
            this.params = placeholders;
        }
        this.stmtName = name;
        this.originalStmt = originalStmt;
    }

    public String getName() {
        return stmtName;
    }

    public List<Placeholder> params() {
        return params;
    }

    public int getParamLen() {
        if (params == null) {
            return 0;
        }
        return params.size();
    }

    public LogicalPlan getInnerPlan() {
        return inner;
    }

    public OriginStatement getOriginalStmt() {
        return originalStmt;
    }

    /**
     * return the labels of paramters
     */
    public List<String> getLabels() {
        List<String> labels = new ArrayList<>();
        if (params == null) {
            return labels;
        }
        for (Placeholder parameter : params) {
            labels.add("$" + parameter.getExprId().asInt());
        }
        return labels;
    }

    // register prepared statement with attached statement id
    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<String> labels = getLabels();
        // register prepareStmt
        if (LOG.isDebugEnabled()) {
            LOG.debug("add prepared statement {}, isBinaryProtocol {}",
                    stmtName, ctx.getCommand() == MysqlCommand.COM_STMT_PREPARE);
        }
        ctx.addPreparedStatementContext(stmtName,
                new PreparedStatementContext(this, ctx, ctx.getStatementContext(), stmtName));
        if (ctx.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            executor.sendStmtPrepareOK((int) ctx.getStmtId(), labels);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    public PreparedCommand withNewPreparedCommand(List<Placeholder> params) {
        return new PreparedCommand(this.stmtName, this.inner, params, this.originalStmt);
    }
}
