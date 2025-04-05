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

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AlterColumnStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String partitionNotExist = "partition_not_exist";
    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;
    private Database db;

    private void runBefore() throws Exception {
        db = CatalogMocker.mockDb();
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getCatalogMgr().getCatalog(anyString);
                minTimes = 0;
                result = catalog;

                catalog.getDb(anyString);
                minTimes = 0;
                result = db;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME,
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME,
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.MYSQL_DB, CatalogMocker.MYSQL_TBL,
                        PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        //test normal
        connectContext.getSessionVariable().enableStats = true;
        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        String indexName = null;
        String columnName = "v1";
        Map<String, String> properties = new HashMap<>();
        properties.put("row_count", "5");
        properties.put("avg_size", "100000");
        AlterColumnStatsCommand command = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test OlapTable
        String indexName2 = "test_index_name";
        TableNameInfo tableNameInfo2 = new TableNameInfo(CatalogMocker.MYSQL_DB, CatalogMocker.MYSQL_TBL);
        AlterColumnStatsCommand command2 = new AlterColumnStatsCommand(tableNameInfo2, partitionNamesInfo, indexName2, columnName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "Only OlapTable support alter index stats. Table mysql-tbl is not OlapTable.");

        //test indexId in OlapTable
        String indexName3 = "invalid_index";
        AlterColumnStatsCommand command3 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName3, columnName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Index invalid_index not exist in table test_tbl2");

        //test invalid statistics
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("histogram", "invalide_value");
        AlterColumnStatsCommand command7 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties2);
        Assertions.assertThrows(AnalysisException.class, () -> command7.validate(connectContext),
                "histogram is invalid statistics");

        //row_count is not exist
        Map<String, String> properties3 = new HashMap<>();
        properties2.put("avg_size", "100000");
        properties2.put("max_size", "100000000");
        AlterColumnStatsCommand command8 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties3);
        Assertions.assertThrows(AnalysisException.class, () -> command8.validate(connectContext),
                 "Set column stats must set row_count. e.g. 'row_count'='5'");

        //test enable stats
        connectContext.getSessionVariable().enableStats = false;
        AlterColumnStatsCommand command9 = new AlterColumnStatsCommand(tableNameInfo, partitionNamesInfo, indexName, columnName, properties);
        Assertions.assertThrows(UserException.class, () -> command9.validate(connectContext),
                "Analyze function is forbidden, you should add `enable_stats=true` in your FE conf file");
    }

    @Test
    void testValidateNoPrivilege() {
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

        TableNameInfo tableNameInfo2 =
                    new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false,
                    ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));

        String indexName = "index1";
        String columnName = "k1";
        Map<String, String> properties = new HashMap<>();

        AlterColumnStatsCommand command = new AlterColumnStatsCommand(tableNameInfo2, partitionNamesInfo2, indexName, columnName, properties);
        connectContext.getSessionVariable().enableStats = true;
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                    "ALTER TABLE STATS command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}
