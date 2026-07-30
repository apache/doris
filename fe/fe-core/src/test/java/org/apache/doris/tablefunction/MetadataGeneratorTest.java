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
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokersMetadataParams;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataGeneratorTest {

    @Test
    public void testBrokersMetadataResultWithClusterName() throws Exception {
        // Mock Env、BrokerMgr 和 权限相关组件
        Env env = Mockito.mock(Env.class);
        BrokerMgr brokerMgr = Mockito.mock(BrokerMgr.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);

        Mockito.when(env.getBrokerMgr()).thenReturn(brokerMgr);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        // Mock ADMIN 权限为 true，绕过校验
        Mockito.when(accessManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN)).thenReturn(true);

        // Mock broker信息
        List<List<String>> brokersInfo = Arrays.asList(
            Arrays.asList("cluster1", "host1", "8080", "true", "2024-01-01", "2024-01-01", ""),
            Arrays.asList("cluster2", "host2", "8080", "true", "2024-01-01", "2024-01-01", ""),
            Arrays.asList("cluster1", "host3", "8080", "true", "2024-01-01", "2024-01-01", "")
        );
        Mockito.when(brokerMgr.getBrokersInfo()).thenReturn(brokersInfo);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
             MockedStatic<ConnectContext> mockedCtx = Mockito.mockStatic(ConnectContext.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedCtx.when(ConnectContext::get).thenReturn(ctx);

            // 构建请求参数
            TMetadataTableRequestParams params = new TMetadataTableRequestParams();
            TBrokersMetadataParams brokersParams = new TBrokersMetadataParams();
            brokersParams.setClusterName("cluster1");
            params.setBrokersMetadataParams(brokersParams);
            params.setMetadataType(TMetadataType.BROKERS); // 设置正确的枚举类型

            // 必须设置 columnsName，否则内部处理会报空指针
            List<String> columns = new BrokersTableValuedFunction(new HashMap<>()).getTableColumns()
                .stream().map(c -> c.getName()).collect(Collectors.toList());
            params.setColumnsName(columns);

            // 构建外层 Request
            TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
            request.setMetadaTableParams(params);

            // 执行测试：调用 public 方法触发内部 private 方法
            TFetchSchemaTableDataResult result = MetadataGenerator.getMetadataTable(request);

            // 验证结果
            Assert.assertNotNull(result);
            Assert.assertEquals(2, result.getDataBatch().size()); // 只有cluster1的2个broker
        }
    }

    @Test
    public void testBrokersMetadataResultWithoutClusterName() throws Exception {
        Env env = Mockito.mock(Env.class);
        BrokerMgr brokerMgr = Mockito.mock(BrokerMgr.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);

        Mockito.when(env.getBrokerMgr()).thenReturn(brokerMgr);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN)).thenReturn(true);

        List<List<String>> brokersInfo = Arrays.asList(
            Arrays.asList("cluster1", "host1", "8080", "true", "2024-01-01", "2024-01-01", ""),
            Arrays.asList("cluster2", "host2", "8080", "true", "2024-01-01", "2024-01-01", "")
        );
        Mockito.when(brokerMgr.getBrokersInfo()).thenReturn(brokersInfo);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
             MockedStatic<ConnectContext> mockedCtx = Mockito.mockStatic(ConnectContext.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedCtx.when(ConnectContext::get).thenReturn(ctx);

            // 构建请求参数
            TMetadataTableRequestParams params = new TMetadataTableRequestParams();
            TBrokersMetadataParams brokersParams = new TBrokersMetadataParams();
            params.setBrokersMetadataParams(brokersParams);
            params.setMetadataType(TMetadataType.BROKERS); // 设置正确的枚举类型

            // 必须设置 columnsName，否则内部处理会报空指针
            List<String> columns = new BrokersTableValuedFunction(new HashMap<>()).getTableColumns()
                .stream().map(c -> c.getName()).collect(Collectors.toList());
            params.setColumnsName(columns);

            // 构建外层 Request
            TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
            request.setMetadaTableParams(params);

            // 执行测试：调用 public 方法触发内部 private 方法
            TFetchSchemaTableDataResult result = MetadataGenerator.getMetadataTable(request);

            Assert.assertNotNull(result);
            Assert.assertEquals(2, result.getDataBatch().size()); // 返回所有broker
        }
    }

}
