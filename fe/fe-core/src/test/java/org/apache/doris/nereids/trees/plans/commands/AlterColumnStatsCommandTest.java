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

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AlterColumnStatsCommandTest extends TestWithFeService {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;
    private Database db;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        new Expectations() {
            {
                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME,
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };

        //test normal
        connectContext.getSessionVariable().enableStats = true;
        createDatabase("test_db");
        createTable("create table test_db.test_tbl\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ");

        TableNameInfo tableNameInfo =
                new TableNameInfo("test_db", "test_tbl");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of("p1"));
        String indexName = null;
        String columnName = "k1";
        Map<String, String> properties = new HashMap<>();
        properties.put("row_count", "5");
        properties.put("avg_size", "100000");
        AlterColumnStatsCommand command = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test not a partitioned table
        createTable("create table test_db.test_tbl2(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
        TableNameInfo tableNameInfo2 =
                new TableNameInfo("test_db", "test_tbl2");
        AlterColumnStatsCommand command2 = new AlterColumnStatsCommand(tableNameInfo2, partitionNamesInfo, indexName, columnName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "Not a partitioned table: test_tbl2");

        //test partition does not exist
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false,
                ImmutableList.of("k3"));
        AlterColumnStatsCommand command3 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo2, indexName, columnName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Partition does not exist: k3");

        //test indexId not exist in OlapTable
        String indexName3 = "invalid_index";
        AlterColumnStatsCommand command4 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName3, columnName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext),
                "Index invalid_index not exist in table test_tbl");

        //test invalid statistics
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("histogram", "invalide_value");
        AlterColumnStatsCommand command5 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties2);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext),
                "histogram is invalid statistics");

        //row_count is not exist
        Map<String, String> properties3 = new HashMap<>();
        properties2.put("avg_size", "100000");
        properties2.put("max_size", "100000000");
        AlterColumnStatsCommand command6 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties3);
        Assertions.assertThrows(AnalysisException.class, () -> command6.validate(connectContext),
                 "Set column stats must set row_count. e.g. 'row_count'='5'");

        //test enable stats
        connectContext.getSessionVariable().enableStats = false;
        AlterColumnStatsCommand command7 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties);
        Assertions.assertThrows(UserException.class, () -> command7.validate(connectContext),
                "Analyze function is forbidden, you should add `enable_stats=true` in your FE conf file");
    }

    @Test
    void testValidateNoPrivilege() throws IOException {
        runBefore();
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME,
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = false;
            }
        };

        TableNameInfo tableNameInfo =
                    new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                    ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));

        String indexName = "index1";
        String columnName = "k1";
        Map<String, String> properties = new HashMap<>();

        AlterColumnStatsCommand command = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties);
        connectContext.getSessionVariable().enableStats = true;
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                    "ALTER TABLE STATS command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}
