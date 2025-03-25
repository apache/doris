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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.system.NodeType;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * show frontend/backend config
*/
public class ShowConfigCommand extends Command implements NoForward {

    public static final ImmutableList<String> FE_TITLE_NAMES = new ImmutableList.Builder<String>().add("Key").add(
            "Value").add("Type").add("IsMutable").add("MasterOnly").add("Comment").build();
    public static final ImmutableList<String> BE_TITLE_NAMES = new ImmutableList.Builder<String>().add("BackendId")
            .add("Host").add("Key").add("Value").add("Type").add("IsMutable").build();

    private final NodeType nodeType;
    private String pattern;
    private long backendId;
    private boolean isShowSingleBackend;

    public ShowConfigCommand(NodeType nodeType) {
        super(PlanType.SHOW_CONFIG_COMMAND);
        this.nodeType = nodeType;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
        this.isShowSingleBackend = true;
    }

    private ShowResultSetMetaData getMetaData(ImmutableList<String> metaNames) {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : metaNames) {
            builder.addColumn(new Column(title, ScalarType.createStringType()));
        }
        return builder.build();
    }

    private ShowResultSet handShowFrontendConfig() throws AnalysisException {
        List<List<String>> results;
        PatternMatcher matcher = null;
        if (pattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(pattern, CaseSensibility.CONFIG.getCaseSensibility());
        }
        results = ConfigBase.getConfigInfo(matcher);
        // Sort all configs by config key.
        results.sort(Comparator.comparing(o -> o.get(0)));
        return new ShowResultSet(getMetaData(FE_TITLE_NAMES), results);
    }

    private ShowResultSet handShowBackendConfig() throws AnalysisException {
        List<List<String>> results = new ArrayList<>();
        List<Long> backendIds;
        final SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        if (isShowSingleBackend) {
            if (systemInfoService.getBackend(backendId) == null) {
                throw new AnalysisException("Backend " + backendId + " not exists");
            }
            Backend backend = systemInfoService.getBackend(backendId);
            if (!backend.isAlive()) {
                throw new AnalysisException("Backend " + backendId + " is not alive");
            }
            backendIds = Lists.newArrayList(backendId);
        } else {
            backendIds = systemInfoService.getAllBackendIds(true);
        }

        PatternMatcher matcher = null;
        if (pattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(pattern, CaseSensibility.CONFIG.getCaseSensibility());
        }
        for (long beId : backendIds) {
            Backend backend = systemInfoService.getBackend(beId);
            String host = backend.getHost();
            int httpPort = backend.getHttpPort();
            String urlString = String.format("http://%s:%d/api/show_config", host, httpPort);
            try {
                URL url = new URL(urlString);
                URLConnection urlConnection = url.openConnection();
                InputStream inputStream = urlConnection.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                while (reader.ready()) {
                    // line's format like [["k1","v1"], ["k2","v2"]]
                    String line = reader.readLine();
                    JSONArray outer = new JSONArray(line);
                    for (int i = 0; i < outer.length(); ++i) {
                        // [key, type, value, isMutable]
                        JSONArray inner = outer.getJSONArray(i);
                        if (matcher == null || matcher.match(inner.getString(0))) {
                            List<String> rows = Lists.newArrayList();
                            rows.add(String.valueOf(beId));
                            rows.add(host);
                            rows.add(inner.getString(0));  // key
                            rows.add(inner.getString(2));  // value
                            rows.add(inner.getString(1));  // Type
                            rows.add(inner.getString(3));  // isMutable
                            results.add(rows);
                        }
                    }
                }
            } catch (Exception e) {
                throw new AnalysisException(
                    String.format("Can’t get backend config, backendId: %d, host: %s", beId, host));
            }
        }
        return new ShowResultSet(getMetaData(BE_TITLE_NAMES), results);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        if (nodeType == NodeType.FRONTEND) {
            executor.sendResultSet(handShowFrontendConfig());
        } else {
            executor.sendResultSet(handShowBackendConfig());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowConfigCommand(this, context);
    }
}
