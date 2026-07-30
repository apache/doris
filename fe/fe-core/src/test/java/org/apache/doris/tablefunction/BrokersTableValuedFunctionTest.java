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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;

public class BrokersTableValuedFunctionTest {

    private Env env = Mockito.mock(Env.class);
    private AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
    private ConnectContext ctx = Mockito.mock(ConnectContext.class);

    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<ConnectContext> mockedCtxStatic;

    @After
    public void tearDown() {
        if (mockedCtxStatic != null) {
            mockedCtxStatic.close();
            mockedCtxStatic = null;
        }
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
            mockedEnvStatic = null;
        }
    }

    private void mockContext() {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);

        mockedCtxStatic = Mockito.mockStatic(ConnectContext.class);
        mockedCtxStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        // Mock ADMIN privilege to true to ensure permission check passes
        Mockito.when(accessControllerManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN)).thenReturn(true);
    }

    @Test
    public void testConstructorWithEmptyParams() throws Exception {
        mockContext();
        BrokersTableValuedFunction tvf = new BrokersTableValuedFunction(new HashMap<>());
        Assert.assertEquals(TMetadataType.BROKERS, tvf.getMetadataType());
        Assert.assertNull(tvf.getMetaScanRange(Collections.emptyList()).getBrokersParams().getClusterName());
    }

    @Test
    public void testConstructorWithClusterName() throws Exception {
        mockContext();
        HashMap<String, String> params = new HashMap<>();
        params.put("cluster_name", "test_cluster");
        BrokersTableValuedFunction tvf = new BrokersTableValuedFunction(params);

        TMetaScanRange metaScanRange = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertNotNull(metaScanRange);
        Assert.assertTrue(metaScanRange.isSetBrokersParams());
        Assert.assertEquals("test_cluster", metaScanRange.getBrokersParams().getClusterName());
    }

    @Test
    public void testConstructorWithUpperCaseClusterNameKey() throws Exception {
        mockContext();
        HashMap<String, String> params = new HashMap<>();
        // Test uppercase key
        params.put("CLUSTER_NAME", "test_cluster");
        BrokersTableValuedFunction tvf = new BrokersTableValuedFunction(params);

        TMetaScanRange metaScanRange = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertNotNull(metaScanRange);
        Assert.assertTrue(metaScanRange.isSetBrokersParams());
        Assert.assertEquals("test_cluster", metaScanRange.getBrokersParams().getClusterName());
    }

    @Test
    public void testConstructorWithMixedCaseClusterNameKey() throws Exception {
        mockContext();
        HashMap<String, String> params = new HashMap<>();
        // Test mixed case key
        params.put("Cluster_Name", "test_cluster");
        BrokersTableValuedFunction tvf = new BrokersTableValuedFunction(params);

        TMetaScanRange metaScanRange = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertNotNull(metaScanRange);
        Assert.assertTrue(metaScanRange.isSetBrokersParams());
        Assert.assertEquals("test_cluster", metaScanRange.getBrokersParams().getClusterName());
    }

    @Test
    public void testConstructorWithUpperCaseClusterNameKeyNotExist() throws Exception {
        mockContext();
        HashMap<String, String> params = new HashMap<>();
        // Test uppercase key with a non-existent cluster name
        params.put("CLUSTER_NAME", "non_existent_cluster");
        BrokersTableValuedFunction tvf = new BrokersTableValuedFunction(params);

        TMetaScanRange metaScanRange = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertNotNull(metaScanRange);
        Assert.assertTrue(metaScanRange.isSetBrokersParams());
        // Ensure the cluster name is correctly passed down and not null
        Assert.assertEquals("non_existent_cluster", metaScanRange.getBrokersParams().getClusterName());
    }

    @Test
    public void testConstructorWithMixedCaseClusterNameKeyNotExist() throws Exception {
        mockContext();
        HashMap<String, String> params = new HashMap<>();
        // Test mixed case key with a non-existent cluster name
        params.put("Cluster_Name", "non_existent_cluster");
        BrokersTableValuedFunction tvf = new BrokersTableValuedFunction(params);

        TMetaScanRange metaScanRange = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertNotNull(metaScanRange);
        Assert.assertTrue(metaScanRange.isSetBrokersParams());
        // Ensure the cluster name is correctly passed down and not null
        Assert.assertEquals("non_existent_cluster", metaScanRange.getBrokersParams().getClusterName());
    }

    @Test(expected = AnalysisException.class)
    public void testConstructorWithInvalidParamKey() throws Exception {
        mockContext();
        HashMap<String, String> params = new HashMap<>();
        params.put("invalid_param", "value");
        new BrokersTableValuedFunction(params);
    }

    @Test(expected = AnalysisException.class)
    public void testConstructorWithEmptyClusterName() throws Exception {
        mockContext();
        HashMap<String, String> params = new HashMap<>();
        params.put("cluster_name", "");
        new BrokersTableValuedFunction(params);
    }

    @Test(expected = AnalysisException.class)
    public void testConstructorWithBlankClusterName() throws Exception {
        mockContext();
        HashMap<String, String> params = new HashMap<>();
        params.put("cluster_name", "   ");
        new BrokersTableValuedFunction(params);
    }

    @Test(expected = AnalysisException.class)
    public void testConstructorWithNullParams() throws Exception {
        mockContext();
        new BrokersTableValuedFunction(null);
    }

    @Test(expected = AnalysisException.class)
    public void testConstructorWithoutPrivilege() throws Exception {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);

        mockedCtxStatic = Mockito.mockStatic(ConnectContext.class);
        mockedCtxStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        // Mock both ADMIN and OPERATOR privileges to false to test insufficient permission
        Mockito.when(accessControllerManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN)).thenReturn(false);
        Mockito.when(accessControllerManager.checkGlobalPriv(ctx, PrivPredicate.OPERATOR)).thenReturn(false);
        QueryState mockState = Mockito.mock(QueryState.class);
        Mockito.when(ctx.getState()).thenReturn(mockState);
        new BrokersTableValuedFunction(new HashMap<>());
    }

    @Test
    public void testGetTableName() throws Exception {
        mockContext();
        BrokersTableValuedFunction tvf = new BrokersTableValuedFunction(new HashMap<>());
        Assert.assertEquals("BrokersTableValuedFunction", tvf.getTableName());
    }

    @Test
    public void testGetTableColumns() throws Exception {
        mockContext();
        BrokersTableValuedFunction tvf = new BrokersTableValuedFunction(new HashMap<>());
        Assert.assertEquals(7, tvf.getTableColumns().size());
    }

    @Test
    public void testGetColumnIndexFromColumnName() {
        Assert.assertEquals(0, BrokersTableValuedFunction.getColumnIndexFromColumnName("Name").intValue());
        Assert.assertEquals(1, BrokersTableValuedFunction.getColumnIndexFromColumnName("Host").intValue());
        Assert.assertEquals(2, BrokersTableValuedFunction.getColumnIndexFromColumnName("Port").intValue());
        Assert.assertEquals(3, BrokersTableValuedFunction.getColumnIndexFromColumnName("Alive").intValue());
        Assert.assertEquals(4,
                BrokersTableValuedFunction.getColumnIndexFromColumnName("LastStartTime").intValue());
        Assert.assertEquals(5,
                BrokersTableValuedFunction.getColumnIndexFromColumnName("LastUpdateTime").intValue());
        Assert.assertEquals(6, BrokersTableValuedFunction.getColumnIndexFromColumnName("ErrMsg").intValue());
    }
}
