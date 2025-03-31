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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ShowTabletsFromTableCommandTest {
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

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME,
                        CatalogMocker.TEST_TBL_NAME, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateWithPrivilege() throws Exception {
        runBefore();

        Expression version = new UnboundSlot("version");

        Expression whereClauseNormal = new EqualTo(version, new IntegerLiteral(2));

        List<OrderKey> orderKeysNormal = new ArrayList<>();
        orderKeysNormal.add(new OrderKey(version, true, true));

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_SINGLE_PARTITION_NAME));

        // normal
        ShowTabletsFromTableCommand command = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseNormal, orderKeysNormal, 5, 0);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        // where clause error
        Expression error = new UnboundSlot("error");
        Expression whereClauseError = new EqualTo(error, new IntegerLiteral(2));

        ShowTabletsFromTableCommand command2 = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseError, orderKeysNormal, 5, 0);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext));

        // where clause contains or
        Expression backendId = new UnboundSlot("BackendId");
        Expression whereClauseOr = new Or(
                new EqualTo(version, new IntegerLiteral(2)),
                new EqualTo(backendId, new IntegerLiteral(2)));

        ShowTabletsFromTableCommand command3 = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseOr, orderKeysNormal, 5, 0);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext));

        // order by error
        List<OrderKey> orderKeysError = new ArrayList<>();
        orderKeysError.add(new OrderKey(error, true, true));

        ShowTabletsFromTableCommand command4 = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseNormal, orderKeysError, 5, 0);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext));
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

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME,
                        CatalogMocker.TEST_TBL2_NAME, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };

        Expression version = new UnboundSlot("version");

        Expression whereClauseNormal = new EqualTo(version, new IntegerLiteral(2));

        List<OrderKey> orderKeysNormal = new ArrayList<>();
        orderKeysNormal.add(new OrderKey(version, true, true));

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_SINGLE_PARTITION_NAME));

        ShowTabletsFromTableCommand command = new ShowTabletsFromTableCommand(tableNameInfo, partitionNamesInfo,
                whereClauseNormal, orderKeysNormal, 5, 0);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));
    }
}
