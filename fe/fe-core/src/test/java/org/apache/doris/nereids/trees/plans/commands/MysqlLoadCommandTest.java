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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.MysqlDataDescription;
import org.apache.doris.nereids.trees.plans.commands.load.MysqlLoadCommand;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MysqlLoadCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private final String dbName = "test_db";
    private final String tblName = "test_tbl";
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;

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

                accessControllerManager.checkTblPriv(connectContext, internalCtl, dbName, tblName, PrivPredicate.LOAD);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() {
        runBefore();
        TableNameInfo tableNameInfo = new TableNameInfo(dbName, tblName);
        List<String> filePaths = new ArrayList<>();
        filePaths.add("/mnt/d/test.csv");

        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("p1");
        partitionNames.add("p2");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitionNames);

        List<String> columns = new ArrayList<>();
        columns.add("k1");
        columns.add("k2");
        columns.add("k3");

        Map<String, String> properties = new HashMap<>();
        properties.put("exec_mem_limit", "100000000");
        properties.put("timeout", "10");
        properties.put("max_filter_ratio", "0.55");
        properties.put("strict_mode", "false");
        properties.put("timezone", "Asia/Shanghai");
        properties.put("trim_double_quotes", "true");

        MysqlDataDescription mysqlDataDescription = new MysqlDataDescription(filePaths,
                  tableNameInfo,
                  true,
                  partitionNamesInfo,
                  Optional.empty(),
                  Optional.empty(),
                  100,
                  columns,
                  new ArrayList<>(),
                  new HashMap<>());

        MysqlLoadCommand command = new MysqlLoadCommand(mysqlDataDescription, properties, "test");
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }
}
