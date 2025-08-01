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

public class ShowTableStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String tableNotExist = "table_not_exist";
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

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.SHOW);
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME,
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = true;
            }
        };
    }

    private Set<String> getColumns() {
        Set<String> columnNames = new HashSet<>();
        List<Column> columns = CatalogMocker.TEST_TBL_BASE_SCHEMA;
        for (Column column : columns) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        //test useTableId is true
        ShowTableStatsCommand command = new ShowTableStatsCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test useTableId is false
        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_SINGLE_PARTITION_NAME));
        List<String> columns = new ArrayList<>(getColumns());
        ShowTableStatsCommand command2 = new ShowTableStatsCommand(tableNameInfo, columns, partitionNamesInfo);
        Assertions.assertDoesNotThrow(() -> command2.validate(connectContext));

        //test tableName is not exist
        TableNameInfo tableNameInfo2 =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, tableNotExist);
        ShowTableStatsCommand command3 = new ShowTableStatsCommand(tableNameInfo2, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Table: " + tableNotExist + " not exists");

        //test partitionName is not exist
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false,
                ImmutableList.of(partitionNotExist));
        ShowTableStatsCommand command4 = new ShowTableStatsCommand(tableNameInfo, columns, partitionNamesInfo2);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext),
                "Partition: " + partitionNotExist + " not exists");
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

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.SHOW);
                minTimes = 0;
                result = false;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME,
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = false;
            }
        };

        //test useTableId is true
        ShowTableStatsCommand command = new ShowTableStatsCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Permission denied command denied to user 'null'@'null' for table '30000'");

        //test useTableId is false
        TableNameInfo tableNameInfo2 =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        List<String> columns = new ArrayList<>(getColumns());
        ShowTableStatsCommand command2 = new ShowTableStatsCommand(tableNameInfo2, columns, partitionNamesInfo2);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "Permission denied command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}
