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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPythonEnvInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SHOW PYTHON VERSIONS command.
 * Shows Python versions available on all alive backends (intersection).
 */
public class ShowPythonVersionsCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowPythonVersionsCommand.class);

    private static final String[] TITLE_NAMES = {
        "Version", "EnvName", "EnvType", "BasePath", "ExecutablePath"
    };

    public ShowPythonVersionsCommand() {
        super(PlanType.SHOW_PYTHON_VERSIONS_COMMAND);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(256)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowPythonVersionsCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(),
                PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        ImmutableMap<Long, Backend> backendsInfo = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        List<List<TPythonEnvInfo>> allBeEnvs = new ArrayList<>();
        Set<String> commonVersions = null;

        for (Backend backend : backendsInfo.values()) {
            if (!backend.isAlive()) {
                continue;
            }
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                List<TPythonEnvInfo> envs = client.getPythonEnvs();
                ok = true;

                allBeEnvs.add(envs);
                Set<String> versions = new HashSet<>();
                for (TPythonEnvInfo env : envs) {
                    versions.add(env.getFullVersion());
                }
                if (commonVersions == null) {
                    commonVersions = versions;
                } else {
                    commonVersions.retainAll(versions);
                }
            } catch (Exception e) {
                LOG.warn("Failed to get python envs from backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }

        List<List<String>> rows = Lists.newArrayList();
        if (commonVersions != null && !allBeEnvs.isEmpty()) {
            // Use envs from the first BE as reference, filtered to common versions
            for (TPythonEnvInfo env : allBeEnvs.get(0)) {
                if (commonVersions.contains(env.getFullVersion())) {
                    List<String> row = new ArrayList<>();
                    row.add(env.getFullVersion());
                    row.add(env.getEnvName());
                    row.add(env.getEnvType());
                    row.add(env.getBasePath());
                    row.add(env.getExecutablePath());
                    rows.add(row);
                }
            }
        }

        return new ShowResultSet(getMetaData(), rows);
    }
}
