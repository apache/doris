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

package org.apache.doris.qe;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnBE;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TFoldConstantParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

class SessionContextRequestTest {
    @AfterEach
    void clearContext() {
        ConnectContext.remove();
    }

    @Test
    void testFoldRequestKeepsChineseLocale() throws Exception {
        assertFoldLocale("zh_CN");
    }

    @Test
    void testFoldRequestKeepsEnglishLocale() throws Exception {
        assertFoldLocale("en_US");
    }

    private void assertFoldLocale(String locale) throws Exception {
        ConnectContext context = newContext(locale);
        Assertions.assertEquals(locale, CoordinatorContext.createQueryGlobals(context).getLcTimeNames());
        TFoldConstantParams request = captureFoldRequest(context);
        Assertions.assertTrue(request.getQueryGlobals().isSetLcTimeNames());
        Assertions.assertEquals(locale, request.getQueryGlobals().getLcTimeNames());
    }

    @Test
    void testFoldRequestKeepsHyperscanFallback() throws Exception {
        ConnectContext context = newContext("zh_CN");
        for (boolean fallback : new boolean[] {false, true}) {
            context.getSessionVariable().enableHyperscanFallback = fallback;
            Assertions.assertEquals(fallback, context.getSessionVariable().toThrift().isEnableHyperscanFallback());
            TFoldConstantParams request = captureFoldRequest(context);
            Assertions.assertTrue(request.getQueryOptions().isSetEnableHyperscanFallback());
            Assertions.assertEquals(fallback, request.getQueryOptions().isEnableHyperscanFallback());
        }
    }

    @Test
    void testPointLookupRequestChangesWithLocale() throws Exception {
        ConnectContext context = newContext("zh_CN");
        OlapTable table = Mockito.mock(OlapTable.class);
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("session_context_case");
        Mockito.when(scanNode.getPointQueryProjectList()).thenReturn(List.of());
        Planner planner = Mockito.mock(Planner.class);
        DescriptorTable descriptorTable = new DescriptorTable();
        descriptorTable.createTupleDescriptor();
        Mockito.when(planner.getDescTable()).thenReturn(descriptorTable);
        Mockito.when(planner.getScanNodes()).thenReturn(List.of(scanNode));
        Mockito.when(planner.getQueryOptions()).thenReturn(context.getSessionVariable().toThrift());
        ShortCircuitQueryContext shortCircuit =
                new ShortCircuitQueryContext(planner, null, context.getSessionVariable().getLcTimeNames());
        PointQueryExecutor executor = new PointQueryExecutor(shortCircuit, 1024);
        for (MysqlCommand command : new MysqlCommand[] {MysqlCommand.COM_QUERY, MysqlCommand.COM_STMT_EXECUTE}) {
            context.command = command;
            for (boolean full : new boolean[] {true, false, true}) {
                for (String locale : new String[] {"zh_CN", "en_US", "zh_CN"}) {
                    context.getSessionVariable().setLcTimeNames(locale);
                    InternalService.PTabletKeyLookupRequest request = Deencapsulation.invoke(
                            executor, "buildLookupRequest", full);
                    request = InternalService.PTabletKeyLookupRequest.parseFrom(request.toByteArray());
                    Assertions.assertTrue(request.hasLcTimeNames());
                    Assertions.assertEquals(locale, request.getLcTimeNames());
                    Assertions.assertEquals(full, request.hasDescTbl());
                    Assertions.assertEquals(full, request.hasOutputExpr());
                    Assertions.assertEquals(full, request.hasQueryOptions());
                    Assertions.assertEquals(command == MysqlCommand.COM_STMT_EXECUTE, request.hasUuid());
                    if (request.hasUuid()) {
                        Assertions.assertEquals(shortCircuit.cacheID.getMostSignificantBits(),
                                request.getUuid().getUuidHigh());
                        Assertions.assertEquals(shortCircuit.cacheID.getLeastSignificantBits(),
                                request.getUuid().getUuidLow());
                    }
                }
            }
        }
    }

    private ConnectContext newContext(String locale) {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.setQueryId(new TUniqueId(1, 2));
        context.getSessionVariable().setLcTimeNames(locale);
        return context;
    }

    private TFoldConstantParams captureFoldRequest(ConnectContext context) throws Exception {
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Backend backend = Mockito.mock(Backend.class);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(systemInfo.getAllBackendByCurrentCluster(true)).thenReturn(new ArrayList<>(List.of(1L)));
        Mockito.when(systemInfo.getBackend(1L)).thenReturn(backend);
        Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
        Mockito.when(backend.getBrpcPort()).thenReturn(8060);
        AtomicReference<TFoldConstantParams> captured = new AtomicReference<>();
        Mockito.when(proxy.foldConstantExpr(Mockito.any(TNetworkAddress.class), Mockito.any(TFoldConstantParams.class)))
                .thenAnswer(invocation -> {
                    captured.set(invocation.getArgument(1));
                    return CompletableFuture.completedFuture(InternalService.PConstantExprResult.getDefaultInstance());
                });
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> proxies = Mockito.mockStatic(BackendServiceProxy.class)) {
            env.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            proxies.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Deencapsulation.invoke(FoldConstantRuleOnBE.class, "evalOnBE", Map.of(), Map.of(), context);
        }
        Assertions.assertNotNull(captured.get(), "real evalOnBE must reach the RPC boundary");
        TFoldConstantParams wireCopy = new TFoldConstantParams();
        new TDeserializer().deserialize(wireCopy, new TSerializer().serialize(captured.get()));
        return wireCopy;
    }
}
