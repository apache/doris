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
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.collections.map.HashedMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BackupCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;

    private String dbName = "test_db";

    private void runBefore() {
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

                accessControllerManager.checkDbPriv(connectContext, internalCtl, dbName, PrivPredicate.LOAD);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() {
        runBefore();
        LabelNameInfo labelNameInfo = new LabelNameInfo(dbName, "label0");
        String repoName = "testRepo";

        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, null, "test_tbl");
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("p1");
        partitionNames.add("p2");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitionNames);
        TableRefInfo tableRefInfo1 = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo, null, null, null, null);

        TableNameInfo tableNameInfo2 = new TableNameInfo(internalCtl, null, "test_tbl2");
        TableRefInfo tableRefInfo2 = new TableRefInfo(tableNameInfo2, null, null, partitionNamesInfo, null, null, null, null);

        List<TableRefInfo> tableRefInfos = new ArrayList<>();
        tableRefInfos.add(tableRefInfo1);
        tableRefInfos.add(tableRefInfo2);

        Map<String, String> properties = new HashedMap();
        properties.put("timeout", "86400");
        properties.put("type", BackupCommand.BackupType.FULL.name());
        properties.put("content", BackupCommand.BackupContent.ALL.name());

        boolean isExclude = false;

        BackupCommand command = new BackupCommand(labelNameInfo, repoName, tableRefInfos, properties, isExclude);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        List<TableRefInfo> tableRefInfos2 = new ArrayList<>();
        BackupCommand command2 = new BackupCommand(labelNameInfo, repoName, tableRefInfos2, properties, isExclude);
        Assertions.assertDoesNotThrow(() -> command2.validate(connectContext));

        TableNameInfo tableNameInfo3 = new TableNameInfo(internalCtl, null, "test_tbl");
        TableRefInfo tableRefInfo3 = new TableRefInfo(tableNameInfo3, null, null, partitionNamesInfo, null, null, null, null);
        List<TableRefInfo> tableRefInfos3 = new ArrayList<>();
        tableRefInfos3.add(tableRefInfo1);
        tableRefInfos.add(tableRefInfo3);
        BackupCommand command3 = new BackupCommand(labelNameInfo, repoName, tableRefInfos3, properties, isExclude);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext));

        Map<String, String> properties2 = new HashedMap();
        properties.put("key1", "value1");
        BackupCommand command4 = new BackupCommand(labelNameInfo, repoName, tableRefInfos, properties2, isExclude);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext));
    }
}
