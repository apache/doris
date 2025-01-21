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
import org.apache.doris.nereids.trees.plans.commands.info.CopyIntoInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

/**
 * copy into command
 */
public class CopyIntoCommand extends Command implements ForwardWithSync {
    private static final ShowResultSetMetaData COPY_INTO_META_DATA =
            ShowResultSetMetaData.builder()
            .addColumn(new Column("id", ScalarType.createVarchar(64)))
            .addColumn(new Column("state", ScalarType.createVarchar(64)))
            .addColumn(new Column("type", ScalarType.createVarchar(64)))
            .addColumn(new Column("msg", ScalarType.createVarchar(128)))
            .addColumn(new Column("loadedRows", ScalarType.createVarchar(64)))
            .addColumn(new Column("filterRows", ScalarType.createVarchar(64)))
            .addColumn(new Column("unselectRows", ScalarType.createVarchar(64)))
            .addColumn(new Column("url", ScalarType.createVarchar(128)))
            .build();

    CopyIntoInfo copyIntoInfo;

    /**
     * Use for copy into command.
     */
    public CopyIntoCommand(CopyIntoInfo info) {
        super(PlanType.COPY_INTO_COMMAND);
        this.copyIntoInfo = info;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        copyIntoInfo.validate(ctx);
        copyIntoInfo.executeCopyInfoCommand(ctx, this.COPY_INTO_META_DATA, executor.getOriginStmt());
        // copy into used
        if (executor.getContext().getState().getResultSet() != null) {
            if (executor.isProxy()) {
                executor.setProxyShowResultSet(executor.getContext().getState().getResultSet());
                return;
            }
            executor.sendResultSet(executor.getContext().getState().getResultSet());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCopyIntoCommand(this, context);
    }
}
