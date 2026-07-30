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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.functions.table.Brokers;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.thrift.TMetaScanRange;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BrokersTest {

    private Env env = Mockito.mock(Env.class);
    private AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
    private ConnectContext ctx = Mockito.mock(ConnectContext.class);
    private QueryState queryState = Mockito.mock(QueryState.class);

    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<ConnectContext> mockedCtxStatic;

    @Before
    public void setUp() {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);

        mockedCtxStatic = Mockito.mockStatic(ConnectContext.class);
        mockedCtxStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(ctx.getState()).thenReturn(queryState);
        // Mock ADMIN privilege to true to ensure permission check passes
        Mockito.when(accessControllerManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN)).thenReturn(true);
    }

    @After
    public void tearDown() {
        if (mockedCtxStatic != null) {
            mockedCtxStatic.close();
        }
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    private BrokersTableValuedFunction invokeToCatalogFunction(Brokers brokers) throws Exception {
        Method method = Brokers.class.getDeclaredMethod("toCatalogFunction");
        method.setAccessible(true);
        return (BrokersTableValuedFunction) method.invoke(brokers);
    }

    @Test
    public void testToCatalogFunction() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("cluster_name", "test_cluster");
        Properties properties = new Properties(params);

        Brokers brokers = new Brokers(properties);
        BrokersTableValuedFunction tvf = invokeToCatalogFunction(brokers);

        Assert.assertNotNull(tvf);
        TMetaScanRange tMetaScanRange = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertEquals("test_cluster", tMetaScanRange.getBrokersParams().getClusterName());
    }

    @Test(expected = AnalysisException.class)
    public void testToCatalogFunctionWithInvalidParams() throws Throwable {
        Map<String, String> params = new HashMap<>();
        params.put("invalid_param", "value");
        Properties properties = new Properties(params);

        Brokers brokers = new Brokers(properties);
        try {
            invokeToCatalogFunction(brokers);
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw e.getCause();
        }
    }

}
