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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShowTabletsFromTableCommandTest extends TestWithFeService {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void testValidateWithPrivilege() throws Exception {
        runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

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
    void testValidateNoPrivilege() throws Exception {
        runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Mockito.doReturn(false).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

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
