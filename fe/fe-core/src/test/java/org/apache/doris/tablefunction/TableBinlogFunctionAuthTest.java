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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TableBinlogFunctionAuthTest {

    @Test
    public void testRequiresSelectPrivilegeBeforeLoadingTableMetadata() {
        Env env = Mockito.mock(Env.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(context.getState()).thenReturn(new QueryState());
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkTblPriv(context, InternalCatalog.INTERNAL_CATALOG_NAME,
                "test_db", "test_table", PrivPredicate.SELECT)).thenReturn(false);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> mockedContext = Mockito.mockStatic(ConnectContext.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedContext.when(ConnectContext::get).thenReturn(context);

            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> new TableBinlogFunction(ImmutableMap.of(
                            "db", "test_db", "table", "test_table")));

            Assertions.assertTrue(exception.getMessage().contains("Access denied"));
            Mockito.verify(accessManager).checkTblPriv(context, InternalCatalog.INTERNAL_CATALOG_NAME,
                    "test_db", "test_table", PrivPredicate.SELECT);
            Mockito.verify(env, Mockito.never()).getInternalCatalog();
        }
    }
}
