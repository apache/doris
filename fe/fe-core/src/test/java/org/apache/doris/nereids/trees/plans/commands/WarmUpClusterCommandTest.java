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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.WarmUpItem;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class WarmUpClusterCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private Env env;
    private AccessControllerManager accessControllerManager;
    private InternalCatalog catalog;
    private ConnectContext connectContext;
    private Database db;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() throws Exception {
        db = CatalogMocker.mockDb();
        env = Mockito.mock(Env.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);
        catalog = Mockito.mock(InternalCatalog.class);
        connectContext = Mockito.mock(ConnectContext.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    @Test
    public void testValidate() throws Exception {
        if (Config.isCloudMode()) {
            List<String> cloudClusterNames = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames();
            if (cloudClusterNames.isEmpty()) {
                WarmUpClusterCommand command = new WarmUpClusterCommand(null, "test_clusterxxx", null, false, false);
                Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));
            } else {
                String srcClusterName = cloudClusterNames.get(0);
                String dstClusterName = "test_clusterxxx";
                TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
                String partitionName = "";
                WarmUpItem warmUpItem = new WarmUpItem(tableNameInfo, partitionName);
                List<WarmUpItem> warmUpItems = new ArrayList<>();
                warmUpItems.add(warmUpItem);
                WarmUpClusterCommand command = new WarmUpClusterCommand(warmUpItems, srcClusterName, dstClusterName, false, true);
                Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
            }
        } else {
            WarmUpClusterCommand command = new WarmUpClusterCommand(null, null, null, false, false);
            Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
        }
    }
}
