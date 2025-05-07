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

import lombok.Getter;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;

import java.util.List;
import java.util.Objects;

public class CancelDecommissionBackendCommand extends CancelCommand {
    private final List<SystemInfoService.HostInfo> hostInfos;
    private final List<String> ids;

    public CancelDecommissionBackendCommand(List<SystemInfoService.HostInfo> hostInfos,
                                            List<String> ids) {
        super(PlanType.CANCEL_DECOMMISSION_BACKEND_COMMAND);
        Objects.requireNonNull(hostInfos, "hostInfos is null!");
        Objects.requireNonNull(ids, "ids is null!");
        this.hostInfos = hostInfos;
        this.ids = ids;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {

    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCancelDecommissionCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CANCEL;
    }
}
