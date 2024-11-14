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
import org.apache.doris.analysis.StmtType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;

/**
 * base class for all show commands
 */
public abstract class ShowCommand extends Command implements Redirect {
    public ShowCommand(PlanType type) {
        super(type);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SHOW;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        ShowResultSet resultSet = doRun(ctx, executor);
        if (resultSet != null) {
            if (executor.isProxy()) {
                executor.setProxyShowResultSet(resultSet);
            } else {
                executor.sendResultSet(resultSet);
            }
        }
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public abstract ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception;

}
