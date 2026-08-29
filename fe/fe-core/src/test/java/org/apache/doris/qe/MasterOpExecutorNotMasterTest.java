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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MasterOpExecutorNotMasterTest {

    // The static Env mock must stay open for the whole test method, because
    // validateHintForTest() reads Env.getCurrentEnv() outside the constructor.
    private static MasterOpExecutor newExecutor(String masterHost, int masterPort, MockedStatic<Env> mockedEnv) {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSelfNode()).thenReturn(new SystemInfoService.HostInfo("127.0.0.1", 9010));
        Mockito.when(env.getMasterHost()).thenReturn(masterHost);
        Mockito.when(env.getMasterRpcPort()).thenReturn(masterPort);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.setSessionVariable(VariableMgr.newSessionVariable());
        ctx.getSessionVariable().setQueryTimeoutS(10);
        return new MasterOpExecutor(null, ctx, RedirectStatus.FORWARD_WITH_SYNC, true);
    }

    // A hint equal to the failed target must be rejected (a degraded old master whose
    // masterInfo = itself would otherwise create a retry loop).
    @Test
    public void testHintPointingToFailedTargetRejected() {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            MasterOpExecutor executor = newExecutor("10.0.0.1", 9020, mockedEnv);
            Assert.assertNull(executor.validateHintForTest(new TNetworkAddress("10.0.0.1", 9020)));
        }
    }

    // A hint pointing to this node must be rejected as well.
    @Test
    public void testHintPointingToSelfRejected() {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            MasterOpExecutor executor = newExecutor("10.0.0.1", 9020, mockedEnv);
            Assert.assertNull(executor.validateHintForTest(new TNetworkAddress("127.0.0.1", Config.rpc_port)));
        }
    }

    // A valid hint pointing elsewhere is accepted.
    @Test
    public void testValidHintAccepted() {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            MasterOpExecutor executor = newExecutor("10.0.0.1", 9020, mockedEnv);
            TNetworkAddress hint = new TNetworkAddress("10.0.0.2", 9020);
            Assert.assertEquals(hint, executor.validateHintForTest(hint));
        }
    }

    // Empty/invalid hints are rejected.
    @Test
    public void testInvalidHintRejected() {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            MasterOpExecutor executor = newExecutor("10.0.0.1", 9020, mockedEnv);
            Assert.assertNull(executor.validateHintForTest(null));
            Assert.assertNull(executor.validateHintForTest(new TNetworkAddress("", 9020)));
            Assert.assertNull(executor.validateHintForTest(new TNetworkAddress("10.0.0.2", 0)));
        }
    }

    // NOT_MASTER detection.
    @Test
    public void testIsNotMasterResult() {
        Assert.assertFalse(MasterOpExecutor.isNotMasterResultForTest(null));
        TMasterOpResult normal = new TMasterOpResult();
        Assert.assertFalse(MasterOpExecutor.isNotMasterResultForTest(normal));
        TMasterOpResult notMaster = new TMasterOpResult();
        notMaster.setNotMaster(true);
        Assert.assertTrue(MasterOpExecutor.isNotMasterResultForTest(notMaster));
    }
}
