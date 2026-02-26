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
import org.apache.doris.thrift.TPythonPackageInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SHOW PYTHON PACKAGES IN '&lt;version&gt;' command.
 * Shows pip packages installed for the given Python version, collected from all alive BEs.
 */
public class ShowPythonPackagesCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(ShowPythonPackagesCommand.class);

    private static final String[] TITLE_NAMES = {"Package", "Version"};
    private static final String[] TITLE_NAMES_INCONSISTENT = {"Package", "Version", "Consistent", "Backends"};

    private final String pythonVersion;

    public ShowPythonPackagesCommand(String pythonVersion) {
        super(PlanType.SHOW_PYTHON_PACKAGES_COMMAND);
        this.pythonVersion = pythonVersion;
    }

    public String getPythonVersion() {
        return pythonVersion;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return getMetaData(true);
    }

    private ShowResultSetMetaData getMetaData(boolean consistent) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        String[] titles = consistent ? TITLE_NAMES : TITLE_NAMES_INCONSISTENT;
        for (String title : titles) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(256)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowPythonPackagesCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(),
                PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        ImmutableMap<Long, Backend> backendsInfo = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();

        // Collect packages from each alive BE, tracking which BE each result came from
        List<Map<String, String>> allBePackages = new ArrayList<>();
        List<String> beIdentifiers = new ArrayList<>();
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
                List<TPythonPackageInfo> packages = client.getPythonPackages(pythonVersion);
                ok = true;

                Map<String, String> pkgMap = new HashMap<>();
                for (TPythonPackageInfo pkg : packages) {
                    pkgMap.put(pkg.getPackageName(), pkg.getVersion());
                }
                allBePackages.add(pkgMap);
                beIdentifiers.add(backend.getHost() + ":" + backend.getBePort());
            } catch (Exception e) {
                LOG.warn("Failed to get python packages from backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }

        if (allBePackages.isEmpty()) {
            return new ShowResultSet(getMetaData(), Lists.newArrayList());
        }

        // Check consistency across BEs
        boolean consistent = true;
        Map<String, String> referencePackages = allBePackages.get(0);
        for (int i = 1; i < allBePackages.size(); i++) {
            if (!referencePackages.equals(allBePackages.get(i))) {
                consistent = false;
                break;
            }
        }

        List<List<String>> rows = Lists.newArrayList();
        if (consistent) {
            for (Map.Entry<String, String> entry : referencePackages.entrySet()) {
                List<String> row = new ArrayList<>();
                row.add(entry.getKey());
                row.add(entry.getValue());
                rows.add(row);
            }
        } else {
            // For each package+version, collect which BEs have it
            // key: pkgName -> (version -> list of BE identifiers)
            Map<String, Map<String, List<String>>> pkgVersionBes = new HashMap<>();
            for (int i = 0; i < allBePackages.size(); i++) {
                String beId = beIdentifiers.get(i);
                for (Map.Entry<String, String> entry : allBePackages.get(i).entrySet()) {
                    pkgVersionBes.computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                            .computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                            .add(beId);
                }
            }
            int totalBes = allBePackages.size();
            for (Map.Entry<String, Map<String, List<String>>> entry : pkgVersionBes.entrySet()) {
                String pkgName = entry.getKey();
                Map<String, List<String>> versionBes = entry.getValue();
                boolean pkgConsistent = versionBes.size() == 1
                        && versionBes.values().iterator().next().size() == totalBes;
                for (Map.Entry<String, List<String>> vb : versionBes.entrySet()) {
                    List<String> row = new ArrayList<>();
                    row.add(pkgName);
                    row.add(vb.getKey());
                    row.add(pkgConsistent ? "Yes" : "No");
                    row.add(pkgConsistent ? "" : String.join(", ", vb.getValue()));
                    rows.add(row);
                }
            }
        }

        return new ShowResultSet(getMetaData(consistent), rows);
    }
}
