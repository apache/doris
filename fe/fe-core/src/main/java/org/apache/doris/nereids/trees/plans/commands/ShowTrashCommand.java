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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.TrashProcDir;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * show trash command
 */
public class ShowTrashCommand extends ShowCommand {
    private List<Backend> backends = Lists.newArrayList();
    private List<String> backendsQuery;

    public ShowTrashCommand() {
        super(PlanType.SHOW_TRASH_COMMAND);
    }

    public ShowTrashCommand(List<String> backendsQuery) {
        super(PlanType.SHOW_TRASH_COMMAND);
        this.backendsQuery = backendsQuery;
    }

    public List<Backend> getBackends() {
        return backends;
    }

    public List<String> getBackendsQuery() {
        return backendsQuery;
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TrashProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    private ShowResultSet handleShowTrash(List<String> backendsQuery) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(),
                PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
        }
        ImmutableMap<Long, Backend> backendsInfo = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        if (backendsQuery == null) {
            for (Backend backend : backendsInfo.values()) {
                this.backends.add(backend);
            }
        } else {
            Map<String, Long> backendsID = new HashMap<>();
            for (Backend backend : backendsInfo.values()) {
                backendsID.put(
                        NetUtils.getHostPortInAccessibleFormat(backend.getHost(), backend.getHeartbeatPort()),
                        backend.getId());
            }
            for (String backendQuery : backendsQuery) {
                if (backendsID.containsKey(backendQuery)) {
                    this.backends.add(backendsInfo.get(backendsID.get(backendQuery)));
                    backendsID.remove(backendQuery);
                }
            }
        }
        List<List<String>> infos = Lists.newArrayList();
        TrashProcDir.getTrashInfo(backends, infos);
        return new ShowResultSet(getMetaData(), infos);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTrashCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowTrash(backendsQuery);
    }
}

