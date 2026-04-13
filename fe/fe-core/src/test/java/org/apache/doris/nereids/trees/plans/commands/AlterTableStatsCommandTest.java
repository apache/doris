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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AlterTableStatsCommandTest extends TestWithFeService {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(CatalogMocker.TEST_DB_NAME);
        createTable("create table " + CatalogMocker.TEST_DB_NAME + "." + CatalogMocker.TEST_TBL2_NAME + "\n"
                + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n"
                + "partition by range(k2)\n"
                + "(partition " + CatalogMocker.TEST_PARTITION1_NAME + " values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');");
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.ALTER));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        //test normal
        connectContext.getSessionVariable().enableStats = true;
        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        Map<String, String> properties = new HashMap<>();
        properties.put("row_count", "5");
        AlterTableStatsCommand command = new AlterTableStatsCommand(tableNameInfo, partitionNamesInfo, properties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test invalid statistics
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("histogram", "invalide_value");
        AlterTableStatsCommand command2 = new AlterTableStatsCommand(tableNameInfo, partitionNamesInfo, properties2);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "histogram is invalid statistics");

        //test enable stats
        connectContext.getSessionVariable().enableStats = false;
        AlterTableStatsCommand command5 = new AlterTableStatsCommand(tableNameInfo, partitionNamesInfo, properties);
        Assertions.assertThrows(UserException.class, () -> command5.validate(connectContext),
                "Analyze function is forbidden, you should add `enable_stats=true` in your FE conf file");
    }

    @Test
    void testValidateNoPrivilege() throws Exception {
        runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.ALTER));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo2 =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        Map<String, String> properties = new HashMap<>();
        AlterTableStatsCommand command = new AlterTableStatsCommand(tableNameInfo2, partitionNamesInfo2, properties);
        connectContext.getSessionVariable().enableStats = true;
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "ALTER TABLE STATS command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}
