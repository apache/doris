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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.utils.KillUtils;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 * kill query command
 */

public class KillQueryCommand extends KillCommand {
    private static final Logger LOG = LogManager.getLogger(KillQueryCommand.class);
    private final String queryId;
    private final int connectionId;

    public KillQueryCommand(String queryId, int connectionId) {
        super(PlanType.KILL_QUERY_COMMAND);
        this.queryId = queryId;
        this.connectionId = connectionId;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (Strings.isNullOrEmpty(queryId) && connectionId < 0) {
            throw new AnalysisException(
                    "Please specify a non empty query id or connection id which >= 0 to kill");
        }
        KillUtils.kill(ctx, false, queryId, connectionId, executor.getOriginStmt());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitKillQueryCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
