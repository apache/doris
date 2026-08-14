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

package org.apache.doris.system;

import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.resource.Tag;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class RowTtlFeatureGateTest {
    @Test
    public void testMissingBackendCapabilityDoesNotActivateBarrier() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        CatalogRecycleBin recycleBin = Mockito.mock(CatalogRecycleBin.class);
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setAlive(true);
        backend.setLastUpdateMs(System.currentTimeMillis());

        Mockito.when(env.getFrontends(null)).thenReturn(Collections.emptyList());
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getDbs()).thenReturn(Collections.emptyList());
        Mockito.when(env.getClusterInfo()).thenReturn(systemInfoService);
        Mockito.when(systemInfoService.getAllClusterBackends(false))
                .thenReturn(Collections.singletonList(backend));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);
            DdlException exception = Assert.assertThrows(
                    DdlException.class, RowTtlFeatureGate::activateForMutation);
            Assert.assertTrue(exception.getMessage().contains("does not support Row TTL"));
            Mockito.verify(env, Mockito.never()).activateRowTtlMetaVersion();

            backend.setNodeFeatureFlags(NodeFeature.ROW_TTL);
            RowTtlFeatureGate.activateForMutation();
            Mockito.verify(env).activateRowTtlMetaVersion();
        }
    }

    @Test
    public void testPreActivationTableIsNotSilentlyAdopted() throws DdlException {
        Env env = Mockito.mock(Env.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        CatalogRecycleBin recycleBin = Mockito.mock(CatalogRecycleBin.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getDbs()).thenReturn(Collections.singletonList(database));
        Mockito.when(database.getTables()).thenReturn(Collections.singletonList(table));
        Mockito.when(table.hasRowTtl()).thenReturn(true);
        Mockito.when(table.getQualifiedName()).thenReturn("db.tbl");

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);
            DdlException exception = Assert.assertThrows(
                    DdlException.class, RowTtlFeatureGate::activateForMutation);
            Assert.assertTrue(exception.getMessage().contains("cannot be silently adopted"));
            Mockito.verify(env, Mockito.never()).activateRowTtlMetaVersion();
        }
    }

    @Test
    public void testPreActivationTableUseFailsClosed() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isRowTtlActivated()).thenReturn(false);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            DdlException exception = Assert.assertThrows(
                    DdlException.class, RowTtlFeatureGate::ensureReadyForUse);
            Assert.assertTrue(exception.getMessage().contains("activation barrier"));
        }
    }

    @Test
    public void testActivatedClusterMarksOldHeartbeatIncompatibleImmediately() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isRowTtlActivated()).thenReturn(true);
        long now = System.currentTimeMillis();

        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        BackendHbResponse capableBackendHeartbeat = new BackendHbResponse(
                1L, 9060, 8040, 8060, now, now, "version", Tag.VALUE_MIX,
                0, 0, false, 8070);
        capableBackendHeartbeat.setNodeFeatureFlags(NodeFeature.ROW_TTL);
        BackendHbResponse oldBackendHeartbeat = new BackendHbResponse(
                1L, 9060, 8040, 8060, now + 1, now, "version", Tag.VALUE_MIX,
                0, 0, false, 8070);

        Frontend frontend = new Frontend(
                FrontendNodeType.FOLLOWER, "fe", "127.0.0.2", 9010);
        FrontendHbResponse capableFrontendHeartbeat = new FrontendHbResponse(
                "fe", 9030, 9020, 9040, 1, now, "version", now,
                Collections.emptyList(), 1, "");
        capableFrontendHeartbeat.setNodeFeatureFlags(NodeFeature.ROW_TTL);
        FrontendHbResponse oldFrontendHeartbeat = new FrontendHbResponse(
                "fe", 9030, 9020, 9040, 2, now + 1, "version", now,
                Collections.emptyList(), 1, "");

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            backend.setAlive(true);
            Assert.assertTrue(backend.isNodeFeatureIncompatible());
            Assert.assertFalse(backend.isQueryAvailable());
            Assert.assertTrue(frontend.isNodeFeatureIncompatible());

            backend.handleHbResponse(capableBackendHeartbeat, true);
            Assert.assertFalse(backend.isNodeFeatureIncompatible());
            Assert.assertTrue(backend.isQueryAvailable());
            Assert.assertTrue(backend.isScheduleAvailable());
            Assert.assertTrue(backend.isLoadAvailable());
            backend.handleHbResponse(oldBackendHeartbeat, true);
            Assert.assertTrue(backend.isNodeFeatureIncompatible());
            Assert.assertTrue(backend.isAlive());
            Assert.assertEquals(0, backend.getHeartbeatFailureCounter());
            Assert.assertFalse(backend.isQueryAvailable());
            Assert.assertFalse(backend.isScheduleAvailable());
            Assert.assertFalse(backend.isLoadAvailable());

            frontend.handleHbResponse(capableFrontendHeartbeat, true);
            Assert.assertFalse(frontend.isNodeFeatureIncompatible());
            frontend.handleHbResponse(oldFrontendHeartbeat, true);
            Assert.assertTrue(frontend.isNodeFeatureIncompatible());
            Assert.assertTrue(frontend.isAlive());
        }
    }

    @Test
    public void testCloudMetaServiceCapabilityIsRequired() throws Exception {
        Env env = Mockito.mock(Env.class);
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(env.getFrontends(null)).thenReturn(Collections.emptyList());
        Mockito.when(env.getClusterInfo()).thenReturn(systemInfoService);
        Mockito.when(systemInfoService.getAllClusterBackends(false)).thenReturn(Collections.emptyList());

        Cloud.MetaServiceResponseStatus ok = Cloud.MetaServiceResponseStatus.newBuilder()
                .setCode(Cloud.MetaServiceCode.OK).build();
        Cloud.GetMetaServiceCapabilityResponse missingCapability =
                Cloud.GetMetaServiceCapabilityResponse.newBuilder().setStatus(ok).build();
        Cloud.GetMetaServiceCapabilityResponse capable = Cloud.GetMetaServiceCapabilityResponse.newBuilder()
                .setStatus(ok)
                .setFeatureFlags(Cloud.MetaServiceFeature.META_SERVICE_FEATURE_ROW_TTL_VALUE)
                .build();

        String oldEndpoint = Config.meta_service_endpoint;
        Config.meta_service_endpoint = "127.0.0.1:20121";
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<Config> configStatic = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<MetaServiceProxy> proxyStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            configStatic.when(Config::isCloudMode).thenReturn(true);
            MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
            proxyStatic.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            Mockito.when(proxy.getMetaServiceCapability(Mockito.any())).thenReturn(missingCapability, capable);

            DdlException exception = Assert.assertThrows(
                    DdlException.class, RowTtlFeatureGate::ensureClusterSupportsRowTtl);
            Assert.assertTrue(exception.getMessage().contains("Every active Meta Service"));
            RowTtlFeatureGate.ensureClusterSupportsRowTtl();
        } finally {
            Config.meta_service_endpoint = oldEndpoint;
        }
    }
}
