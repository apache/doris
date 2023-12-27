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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.sync.DataSyncJobType;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CreateDataSyncJobStmtTest {
    private static final Logger LOG = LogManager.getLogger(CreateDataSyncJobStmtTest.class);

    private String jobName = "testJob";
    private String dbName = "testDb";
    private String tblName = "testTbl";
    private Map<String, String> properties;

    @Mocked
    Env env;
    @Mocked
    InternalCatalog catalog;
    @Mocked
    Analyzer analyzer;
    @Mocked
    AccessControllerManager accessManager;
    @Injectable
    Database database;
    @Injectable
    OlapTable table;

    @Before
    public void setUp() {
        properties = Maps.newHashMap();
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable("testDb");
                minTimes = 0;
                result = database;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                database.getTableNullable("testTbl");
                minTimes = 0;
                result = table;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
            }
        };
    }

    @Test
    public void testNoDb() {
        CreateDataSyncJobStmt stmt = new CreateDataSyncJobStmt(
                null, null, null, null, null);
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testNoType() {
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        CreateDataSyncJobStmt stmt = new CreateDataSyncJobStmt(
                jobName, dbName, null, binlogDesc, null);
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testDuplicateColNames() {
        properties.put("type", "canal");
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        List<String> colNames = Lists.newArrayList();
        colNames.add("a");
        colNames.add("a");
        ChannelDescription channelDescription = new ChannelDescription(
                "mysql_db", "mysql_tbl", tblName, null, colNames);
        CreateDataSyncJobStmt stmt = new CreateDataSyncJobStmt(
                jobName, dbName, Lists.newArrayList(channelDescription), binlogDesc, null);
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testNoUniqueTable() {
        properties.put("type", "canal");
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        ChannelDescription channelDescription = new ChannelDescription(
                "mysql_db", "mysql_tbl", tblName, null, null);
        CreateDataSyncJobStmt stmt = new CreateDataSyncJobStmt(
                jobName, dbName, Lists.newArrayList(channelDescription), binlogDesc, null);
        try {
            stmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testNormal() {
        new Expectations() {
            {
                table.getKeysType();
                result = KeysType.UNIQUE_KEYS;
                table.hasDeleteSign();
                result = true;
            }
        };
        properties.put("type", "canal");
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        ChannelDescription channelDescription = new ChannelDescription(
                "mysql_db", "mysql_tbl", tblName, null, null);
        CreateDataSyncJobStmt stmt = new CreateDataSyncJobStmt(
                jobName, dbName, Lists.newArrayList(channelDescription), binlogDesc, null);
        try {
            stmt.analyze(analyzer);
            Assert.assertEquals(jobName, stmt.getJobName());
            Assert.assertEquals("testDb", stmt.getDbName());
            Assert.assertEquals(DataSyncJobType.CANAL, stmt.getDataSyncJobType());
        } catch (UserException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
    }
}
