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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.Map;

/**
 * cancel export command
 */
public class CancelExportCommand extends CancelCommand implements ForwardWithSync {
    private Map<String, String> supportedColumns = new HashMap<>();

    private String dbName;

    private String label;

    private String state;

    private Expression whereClause;

    public CancelExportCommand(String dbName, Expression whereClause) {
        super(PlanType.CANCEL_EXPORT_COMMAND);
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().getExportMgr().cancelExportJob(label, state, whereClause, dbName);
    }

    private void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException("No database selected");
            }
        }

        supportedColumns.put("label", "");
        supportedColumns.put("state", "");
        checkWhereFilter(whereClause, supportedColumns);
        if (!Strings.isNullOrEmpty(supportedColumns.get("label"))) {
            label = supportedColumns.get("label");
        }
        if (!Strings.isNullOrEmpty(supportedColumns.get("state"))) {
            state = supportedColumns.get("state");
            ExportJobState jobState = ExportJobState.valueOf(state);
            if (jobState != ExportJobState.PENDING
                    && jobState != ExportJobState.EXPORTING) {
                throw new AnalysisException("Only support PENDING/EXPORTING, invalid state: " + state);
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCancelExportCommand(this, context);
    }
}
