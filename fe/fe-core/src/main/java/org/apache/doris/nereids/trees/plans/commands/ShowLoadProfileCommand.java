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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;

/**
 * show load profile command
 */
public class ShowLoadProfileCommand extends ShowCommand {
    private String loadIdPath;

    /**
     * constructor
     */
    public ShowLoadProfileCommand(String path) {
        super(PlanType.SHOW_LOAD_PROFILE_COMMAND);
        this.loadIdPath = path;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        String selfHost = Env.getCurrentEnv().getSelfNode().getHost();
        int httpPort = Config.http_port;
        String terminalMsg = String.format(
                "try visit http://%s:%d/QueryProfile/%s, show query/load profile syntax is a deprecated feature",
                selfHost, httpPort, this.loadIdPath);
        throw new UserException(terminalMsg);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLoadProfileCommand(this, context);
    }
}
