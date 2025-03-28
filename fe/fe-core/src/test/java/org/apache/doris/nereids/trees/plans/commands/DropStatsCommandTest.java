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
import org.apache.doris.catalog.Column;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DropStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String catalogNotExist = "catalog_not_exist";
    private static final String dbNotExist = "db_not_exist";
    private static final String tblNotExist = "tbl_not_exist";
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private InternalCatalog catalog;
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
                        PrivPredicate.DROP);
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME,
                        PrivPredicate.DROP);
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
        Set<String> columns = new HashSet<>();
        for (Column column : CatalogMocker.TEST_TBL_BASE_SCHEMA) {
            columns.add(column.getName());
        }
        DropStatsCommand command = new DropStatsCommand(tableNameInfo, columns, partitionNamesInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test table is empty
        TableNameInfo tableNameInfo2 =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, "");
        DropStatsCommand command2 = new DropStatsCommand(tableNameInfo2, columns, partitionNamesInfo);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () -> command2.validate(connectContext),
                "Table name is null");

        //test unkown catalog
        List<String> parts = new ArrayList<>();
        parts.add(catalogNotExist);
        parts.add(dbNotExist);
        parts.add(tblNotExist);
        TableNameInfo tableNameInfo3 = new TableNameInfo(parts);
        DropStatsCommand command3 = new DropStatsCommand(tableNameInfo3, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Unknown catalog 'catalog_not_exist'");

        //test unkown db
        TableNameInfo tableNameInfo4 = new TableNameInfo(dbNotExist, CatalogMocker.TEST_TBL_NAME);
        DropStatsCommand command4 = new DropStatsCommand(tableNameInfo4, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext),
                "Unknown database 'db_not_exist'");

        //test unkown tbl
        TableNameInfo tableNameInfo5 = new TableNameInfo(CatalogMocker.TEST_DB_NAME, tblNotExist);
        DropStatsCommand command5 = new DropStatsCommand(tableNameInfo5, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext),
                "Unknown table 'tbl_not_exist'");

        //test columns' size > 100
        Set<String> columns2 = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            columns2.add(String.valueOf(i));
        }
        DropStatsCommand command6 = new DropStatsCommand(tableNameInfo, columns2, partitionNamesInfo);
        Assertions.assertThrows(UserException.class, () -> command6.validate(connectContext),
                "Can't delete more that 100 columns at one time.");

        //test partitions' size > 100
        List<String> partitionNames = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            partitionNames.add(String.valueOf(i));
        }
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false, partitionNames);
        DropStatsCommand command7 = new DropStatsCommand(tableNameInfo, columns, partitionNamesInfo2);
        Assertions.assertThrows(UserException.class, () -> command7.validate(connectContext),
                "Can't delete more that 100 partitions at one time");

        //test enable stats is false
        connectContext.getSessionVariable().enableStats = false;
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext),
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
                        PrivPredicate.DROP);
                minTimes = 0;
                result = false;
            }
        };

        connectContext.getSessionVariable().enableStats = true;
        TableNameInfo tableNameInfo =
                    new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        Set<String> columns = new HashSet<>();
        for (Column column : CatalogMocker.TEST_TBL_BASE_SCHEMA) {
            columns.add(column.getName());
        }
        DropStatsCommand command = new DropStatsCommand(tableNameInfo, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "DROP command denied to user 'null'@'null' for table 'test_db.test_tbl2'");
    }
}
