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
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.MockedFrontend.EnvVarNotSetException;
import org.apache.doris.utframe.MockedFrontend.FeStartException;
import org.apache.doris.utframe.MockedFrontend.NotInitException;
import org.apache.doris.utframe.ThreadManager.ThreadAlreadyExistException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class Demo {

    @BeforeClass
    public static void beforeClass() throws EnvVarNotSetException, IOException, ThreadAlreadyExistException,
            FeStartException, NotInitException, DdlException, InterruptedException {
        // start fe
        MockedFrontend frontend = MockedFrontend.getInstance();
        Map<String, String> feConfMap = Maps.newHashMap();
        feConfMap.put("tablet_create_timeout_second", "10");
        frontend.init(feConfMap);
        frontend.start(new String[0]);

        // start be
        MockedBackend backend = MockedBackendFactory.createDefaultBackend();
        backend.setFeAddress(new TNetworkAddress("127.0.0.1", frontend.getRpcPort()));
        backend.start();

        // add be
        List<Pair<String, Integer>> bes = Lists.newArrayList();
        bes.add(Pair.create(backend.getHost(), backend.getHeartbeatPort()));
        Catalog.getCurrentSystemInfo().addBackends(bes, false, "default_cluster");

        // sleep to wait first heartbeat
        Thread.sleep(5000);
    }

    @Test
    public void test() {
        System.out.println(Catalog.getCurrentCatalog().getDbNames());

        ConnectContext ctx = new ConnectContext();
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(PaloAuth.ROOT_USER);
        ctx.setThreadLocalInfo();
        ctx.setCatalog(Catalog.getCurrentCatalog());

        String createDbStmtStr = "create database db1;";
        SqlScanner input = new SqlScanner(new StringReader(createDbStmtStr), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        try {
            CreateDbStmt createDbStmt = (CreateDbStmt) parser.parse().value;
            Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
            createDbStmt.analyze(analyzer);

            Catalog.getCurrentCatalog().createDb(createDbStmt);

            System.out.println(Catalog.getCurrentCatalog().getDbNames());

        } catch (Exception e) {
            e.printStackTrace();
        }

        String createTblStmtStr = "create table db1.tbl1(k1 int) distributed by hash(k1) buckets 1 properties('replication_num' = '1');";
        
        input = new SqlScanner(new StringReader(createTblStmtStr), ctx.getSessionVariable().getSqlMode());
        parser = new SqlParser(input);
        try {
            CreateTableStmt createTableStmt = (CreateTableStmt) parser.parse().value;
            Analyzer analyzer = new Analyzer(ctx.getCatalog(), ctx);
            createTableStmt.analyze(analyzer);

            Catalog.getCurrentCatalog().createTable(createTableStmt);

            Database db = Catalog.getCurrentCatalog().getDb("default_cluster:db1");
            db.readLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable("tbl1");
                System.out.println(tbl.getName());
            } finally {
                db.readUnlock();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
