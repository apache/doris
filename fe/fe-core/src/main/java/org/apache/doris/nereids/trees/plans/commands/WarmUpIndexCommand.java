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

import org.apache.doris.datasource.lance.LanceIndexPrewarm;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

/** Synchronously fills the query sessions on the selected backends. */
public class WarmUpIndexCommand extends Command {
    private final TableNameInfo table;
    private final String indexName;
    private final String computeGroup;

    public WarmUpIndexCommand(TableNameInfo table, String indexName, String computeGroup) {
        super(PlanType.WARM_UP_INDEX_COMMAND);
        this.table = table;
        this.indexName = indexName;
        this.computeGroup = computeGroup;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        LanceIndexPrewarm.run(ctx, executor, table, indexName, computeGroup,
                () -> executor.isCancelled() || ctx.isKilled());
    }

    @Override
    public ResultSetMetaData getResultSetMetaData() {
        // Binary PREPARE must advertise exactly the columns later sent by EXECUTE.
        return LanceIndexPrewarm.getResultSetMetaData();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }
}
