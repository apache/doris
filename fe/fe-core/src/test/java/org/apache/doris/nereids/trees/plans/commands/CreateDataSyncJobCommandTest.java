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

import org.apache.doris.analysis.BinlogDesc;
import org.apache.doris.analysis.ChannelDescription;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.sync.DataSyncJobType;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.load.CreateDataSyncJobCommand;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateDataSyncJobCommandTest {
    private static final Logger LOG = LogManager.getLogger(CreateDataSyncJobCommandTest.class);

    @Injectable
    Database database;
    @Injectable
    OlapTable table;
    @Mocked
    AccessControllerManager accessManager;
    @Mocked
    Env env;
    @Mocked
    InternalCatalog catalog;
    @Mocked
    private ConnectContext connectContext;

    private String jobName = "testJob";
    private String dbName = "testDb";
    private String tblName = "testTbl";
    private Map<String, String> properties;

    public void runBefore() {
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

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString, (PrivPredicate) any);
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
    public void testNoDb() throws Exception {
        runBefore();
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        CreateDataSyncJobCommand command = new CreateDataSyncJobCommand(
                "", "test_job", new ArrayList<>(), binlogDesc, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext), "No database selected");
    }

    @Test
    public void testNoType() {
        runBefore();
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        CreateDataSyncJobCommand command = new CreateDataSyncJobCommand(
                dbName, jobName, new ArrayList<>(), binlogDesc, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext), "Binlog properties must contain property `type`");
    }

    @Test
    public void testDuplicateColNames() {
        runBefore();
        properties.put("type", "canal");
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        List<String> colNames = Lists.newArrayList();
        colNames.add("a");
        colNames.add("a");
        ChannelDescription channelDescription = new ChannelDescription(
                "mysql_db", "mysql_tbl", tblName, null, colNames);
        CreateDataSyncJobCommand command = new CreateDataSyncJobCommand(
                dbName, jobName, Lists.newArrayList(channelDescription), binlogDesc, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext), "Duplicate column: a");
    }

    @Test
    public void testTypeIsNotCanal() {
        runBefore();
        properties.put("type", "test");
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        List<String> colNames = Lists.newArrayList();
        colNames.add("a");
        ChannelDescription channelDescription = new ChannelDescription(
                "mysql_db", "mysql_tbl", tblName, null, colNames);
        CreateDataSyncJobCommand command = new CreateDataSyncJobCommand(
                dbName, jobName, Lists.newArrayList(channelDescription), binlogDesc, null);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext), "Data sync job now only support CANAL type");
    }

    @Test
    public void testEmptyChannelDescription() {
        runBefore();
        properties.put("type", "canal");
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        List<String> colNames = Lists.newArrayList();
        colNames.add("a");
        CreateDataSyncJobCommand command = new CreateDataSyncJobCommand(
                dbName, jobName, Lists.newArrayList(), binlogDesc, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext), "No channel is assign in data sync job statement.");
    }

    @Test
    public void testNoUniqueTable() {
        runBefore();
        new Expectations() {
            {
                table.getKeysType();
                result = KeysType.DUP_KEYS;
                table.hasDeleteSign();
                result = true;
            }
        };
        properties.put("type", "canal");
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        ChannelDescription channelDescription = new ChannelDescription(
                "mysql_db", "mysql_tbl", tblName, null, null);
        CreateDataSyncJobCommand command = new CreateDataSyncJobCommand(
                    dbName, jobName, Lists.newArrayList(channelDescription), binlogDesc, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext), "Table: testTbl"
                + " is not a unique table, key type: DUP_KEYS");
    }

    @Test
    public void testHasNotDeleteSign() {
        runBefore();
        new Expectations() {
            {
                table.getKeysType();
                result = KeysType.UNIQUE_KEYS;
                table.hasDeleteSign();
                result = false;
            }
        };
        properties.put("type", "canal");
        BinlogDesc binlogDesc = new BinlogDesc(properties);
        ChannelDescription channelDescription = new ChannelDescription(
                "mysql_db", "mysql_tbl", tblName, null, null);
        CreateDataSyncJobCommand command = new CreateDataSyncJobCommand(
                dbName, jobName, Lists.newArrayList(channelDescription), binlogDesc, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext), "Table: testTbl"
                + " don't support batch delete. Please upgrade it to support, see `help alter table`.");
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
        CreateDataSyncJobCommand command = new CreateDataSyncJobCommand(
                dbName, jobName, Lists.newArrayList(channelDescription), binlogDesc, properties);
        try {
            command.validate(connectContext);
            Assertions.assertEquals(jobName, command.getJobName());
            Assertions.assertEquals("testDb", command.getDbName());
            Assertions.assertEquals(DataSyncJobType.CANAL, command.getDataSyncJobType());
        } catch (UserException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
    }
}
