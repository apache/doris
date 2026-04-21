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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.GenericPool;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.HeartbeatMgr.BrokerHeartbeatHandler;
import org.apache.doris.system.HeartbeatMgr.FrontendHeartbeatHandler;
import org.apache.doris.system.HeartbeatResponse.HbStatus;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPingBrokerRequest;
import org.apache.doris.thrift.TFrontendPingFrontendRequest;
import org.apache.doris.thrift.TFrontendPingFrontendResult;
import org.apache.doris.thrift.TFrontendPingFrontendStatusCode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class HeartbeatMgrTest {

    private Env env = Mockito.mock(Env.class);
    private MockedStatic<Env> mockedEnvStatic;

    @Before
    public void setUp() {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("192.168.1.3", 9010)); // not self
        Mockito.when(env.isReady()).thenReturn(true);
    }

    @After
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFrontendHbHandler() throws Exception {
        FrontendService.Client client = Mockito.mock(FrontendService.Client.class);

        GenericPool<FrontendService.Client> mockPool = Mockito.mock(GenericPool.class);
        Mockito.when(mockPool.borrowObject(Mockito.any(TNetworkAddress.class))).thenReturn(client);

        GenericPool<FrontendService.Client> originalPool = ClientPool.frontendHeartbeatPool;
        ClientPool.frontendHeartbeatPool = mockPool;
        try {
            TFrontendPingFrontendRequest normalRequest = new TFrontendPingFrontendRequest(12345, "abcd");
            TFrontendPingFrontendResult normalResult = new TFrontendPingFrontendResult();
            normalResult.setStatus(TFrontendPingFrontendStatusCode.OK);
            normalResult.setMsg("success");
            normalResult.setReplayedJournalId(191224);
            normalResult.setQueryPort(9131);
            normalResult.setRpcPort(9121);
            normalResult.setArrowFlightSqlPort(9141);
            normalResult.setVersion("test");

            TFrontendPingFrontendRequest badRequest = new TFrontendPingFrontendRequest(12345, "abcde");
            TFrontendPingFrontendResult badResult = new TFrontendPingFrontendResult();
            badResult.setStatus(TFrontendPingFrontendStatusCode.FAILED);
            badResult.setMsg("not ready");

            Mockito.when(client.ping(normalRequest)).thenReturn(normalResult);
            Mockito.when(client.ping(badRequest)).thenReturn(badResult);

            Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "test", "192.168.1.1", 9010);
            FrontendHeartbeatHandler handler = new FrontendHeartbeatHandler(fe, 12345, "abcd");
            HeartbeatResponse response = handler.call();

            Assert.assertTrue(response instanceof FrontendHbResponse);
            FrontendHbResponse hbResponse = (FrontendHbResponse) response;
            Assert.assertEquals(191224, hbResponse.getReplayedJournalId());
            Assert.assertEquals(9131, hbResponse.getQueryPort());
            Assert.assertEquals(9121, hbResponse.getRpcPort());
            Assert.assertEquals(9141, hbResponse.getArrowFlightSqlPort());
            Assert.assertEquals(HbStatus.OK, hbResponse.getStatus());
            Assert.assertEquals("test", hbResponse.getVersion());

            Frontend fe2 = new Frontend(FrontendNodeType.FOLLOWER, "test2", "192.168.1.2", 9010);
            handler = new FrontendHeartbeatHandler(fe2, 12345, "abcde");
            response = handler.call();

            Assert.assertTrue(response instanceof FrontendHbResponse);
            hbResponse = (FrontendHbResponse) response;
            Assert.assertEquals(0, hbResponse.getReplayedJournalId());
            Assert.assertEquals(0, hbResponse.getQueryPort());
            Assert.assertEquals(0, hbResponse.getRpcPort());
            Assert.assertEquals(0, hbResponse.getArrowFlightSqlPort());
            Assert.assertEquals(HbStatus.BAD, hbResponse.getStatus());
            Assert.assertEquals("not ready", hbResponse.getMsg());
        } finally {
            ClientPool.frontendHeartbeatPool = originalPool;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBrokerHbHandler() throws Exception {
        TPaloBrokerService.Client client = Mockito.mock(TPaloBrokerService.Client.class);

        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.setStatusCode(TBrokerOperationStatusCode.OK);

        GenericPool<TPaloBrokerService.Client> mockPool = Mockito.mock(GenericPool.class);
        Mockito.when(mockPool.borrowObject(Mockito.any(TNetworkAddress.class))).thenReturn(client);

        GenericPool<TPaloBrokerService.Client> originalPool = ClientPool.brokerPool;
        ClientPool.brokerPool = mockPool;
        try {
            Mockito.when(client.ping(Mockito.any(TBrokerPingBrokerRequest.class))).thenReturn(status);

            FsBroker broker = new FsBroker("192.168.1.1", 8111);
            BrokerHeartbeatHandler handler = new BrokerHeartbeatHandler("hdfs", broker, "abc");
            HeartbeatResponse response = handler.call();

            Assert.assertTrue(response instanceof BrokerHbResponse);
            BrokerHbResponse hbResponse = (BrokerHbResponse) response;
            Assert.assertEquals(HbStatus.OK, hbResponse.getStatus());
        } finally {
            ClientPool.brokerPool = originalPool;
        }
    }

}
