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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class AdminCancelRepairTableCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;

    private void runBefore() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, "test_db", "test_tbl");
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("p1");
        partitionNames.add("p2");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitionNames);
        TableRefInfo tableRefInfo = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo, null, null, null, null);
        AdminCancelRepairTableCommand command = new AdminCancelRepairTableCommand(tableRefInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test external catalog
        TableNameInfo tableNameInfo2 = new TableNameInfo("hive", "test_db", "test_tbl");
        TableRefInfo tableRefInfo2 = new TableRefInfo(tableNameInfo2, null, null, partitionNamesInfo, null, null, null, null);
        AdminCancelRepairTableCommand command2 = new AdminCancelRepairTableCommand(tableRefInfo2);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "External catalog 'hive' is not allowed in 'AdminCancelRepairTableCommand.class'");

        //test partitionNameInfo isTemp
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(true, partitionNames);
        TableRefInfo tableRefInfo3 = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo2, null, null, null, null);
        AdminCancelRepairTableCommand command3 = new AdminCancelRepairTableCommand(tableRefInfo3);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Do not support (cancel)repair temporary partitions");
    }

    @Test
    public void testValidateNoPriviledge() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };

        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, "test_db", "test_tbl");
        List<String> partitionNames = new ArrayList<>();
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitionNames);
        TableRefInfo tableRefInfo = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo, null, null, null, null);
        AdminCancelRepairTableCommand command = new AdminCancelRepairTableCommand(tableRefInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Access denied; you need (at least one of) the (ADMIN) privilege(s) for this operation");
    }
}
