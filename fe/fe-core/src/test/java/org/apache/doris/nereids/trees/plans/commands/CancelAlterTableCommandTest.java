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
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class CancelAlterTableCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private AccessControllerManager accessControllerManager;

    private final String dbName = "test_db";
    private final String tblName = "test_tbl";

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

                accessControllerManager.checkTblPriv(connectContext, internalCtl, dbName, tblName, PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        TableNameInfo tableNameInfo = new TableNameInfo(dbName, tblName);
        CancelAlterTableCommand.AlterType alterType = CancelAlterTableCommand.AlterType.COLUMN;
        List<Long> alterJobIdList = new ArrayList<>();
        CancelAlterTableCommand command = new CancelAlterTableCommand(tableNameInfo, alterType, alterJobIdList);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        TableNameInfo tableNameInfo02 = new TableNameInfo("hive", dbName, tblName);
        CancelAlterTableCommand command02 = new CancelAlterTableCommand(tableNameInfo02, alterType, alterJobIdList);
        Assertions.assertThrows(AnalysisException.class, () -> command02.validate(connectContext),
                "External catalog 'hive' is not allowed in 'CancelAlterTableCommand'");
    }
}
