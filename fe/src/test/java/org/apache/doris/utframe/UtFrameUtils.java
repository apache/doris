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

package org.apache.doris.utframe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.MockedBackendFactory.DefaultBeThriftServiceImpl;
import org.apache.doris.utframe.MockedBackendFactory.DefaultHeartbeatServiceImpl;
import org.apache.doris.utframe.MockedBackendFactory.DefaultPBackendServiceImpl;
import org.apache.doris.utframe.MockedFrontend.EnvVarNotSetException;
import org.apache.doris.utframe.MockedFrontend.FeStartException;
import org.apache.doris.utframe.MockedFrontend.NotInitException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.StringReader;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class UtFrameUtils {

    // Help to create a mocked ConnectContext.
    public static ConnectContext createDefaultCtx() throws IOException {
        SocketChannel channel = SocketChannel.open();
        ConnectContext ctx = new ConnectContext(channel);
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(PaloAuth.ROOT_USER);
        ctx.setCatalog(Catalog.getCurrentCatalog());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    // Parse an origin stmt and analyze it. Return a StatementBase instance.
    public static StatementBase parseAndAnalyzeStmt(String originStmt, ConnectContext ctx)
            throws Exception {
        System.out.println("begin to parse stmt: " + originStmt);
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);

        StatementBase statementBase = (StatementBase) parser.parse().value;
        Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
        statementBase.analyze(analyzer);
        return statementBase;
    }

    public static void createMinDorisCluster(String runningDir) throws EnvVarNotSetException, IOException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        // get DORIS_HOME
        final String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            throw new EnvVarNotSetException("env DORIS_HOME is not set");
        }

        Random r = new Random(System.currentTimeMillis());
        int basePort = 20000 + r.nextInt(9000);
        int fe_http_port = basePort + 1;
        int fe_rpc_port = basePort + 2;
        int fe_query_port = basePort + 3;
        int fe_edit_log_port = basePort + 4;

        int be_heartbeat_port = basePort + 5;
        int be_thrift_port = basePort + 6;
        int be_brpc_port = basePort + 7;
        int be_http_port = basePort + 8;

        // start fe in "DORIS_HOME/fe/mocked/"
        MockedFrontend frontend = MockedFrontend.getInstance();
        Map<String, String> feConfMap = Maps.newHashMap();
        // set additional fe config
        feConfMap.put("http_port", String.valueOf(fe_http_port));
        feConfMap.put("rpc_port", String.valueOf(fe_rpc_port));
        feConfMap.put("query_port", String.valueOf(fe_query_port));
        feConfMap.put("edit_log_port", String.valueOf(fe_edit_log_port));
        feConfMap.put("tablet_create_timeout_second", "10");
        frontend.init(dorisHome + "/" + runningDir, feConfMap);
        frontend.start(new String[0]);

        // start be
        MockedBackend backend = MockedBackendFactory.createBackend("127.0.0.1",
                be_heartbeat_port, be_thrift_port, be_brpc_port, be_http_port,
                new DefaultHeartbeatServiceImpl(be_thrift_port, be_http_port, be_brpc_port),
                new DefaultBeThriftServiceImpl(), new DefaultPBackendServiceImpl());
        backend.setFeAddress(new TNetworkAddress("127.0.0.1", frontend.getRpcPort()));
        backend.start();

        // add be
        List<Pair<String, Integer>> bes = Lists.newArrayList();
        bes.add(Pair.create(backend.getHost(), backend.getHeartbeatPort()));
        Catalog.getCurrentSystemInfo().addBackends(bes, false, "default_cluster");

        // sleep to wait first heartbeat
        Thread.sleep(6000);
    }
}
