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

import mockit.Expectations;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.HeartbeatMgr.BrokerHeartbeatHandler;
import org.apache.doris.system.HeartbeatMgr.FrontendHeartbeatHandler;
import org.apache.doris.system.HeartbeatResponse.HbStatus;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPingBrokerRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class HeartbeatMgrTest {

    @Mocked
    private Catalog catalog;

    @Before
    public void setUp() {
        new Expectations() {
            {
                catalog.getSelfNode();
                minTimes = 0;
                result = Pair.create("192.168.1.3", 9010); // not self

                catalog.isReady();
                minTimes = 0;
                result = true;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
            }
        };

    }

    @Test
    public void testFrontendHbHandler() {
        new MockUp<Util>() {
            @Mock
            public String getResultForUrl(String urlStr, String encodedAuthInfo, int connectTimeoutMs,
                    int readTimeoutMs) {
                if (urlStr.contains("192.168.1.1")) {
                    return "{\"replayedJournalId\":191224,\"queryPort\":9131,\"rpcPort\":9121,\"status\":\"OK\",\"msg\":\"Success\"}";
                } else {
                    return "{\"replayedJournalId\":0,\"queryPort\":0,\"rpcPort\":0,\"status\":\"FAILED\",\"msg\":\"not ready\"}";
                }
            }
        };

        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "test", "192.168.1.1", 9010);
        FrontendHeartbeatHandler handler = new FrontendHeartbeatHandler(fe, 12345, "abcd");
        HeartbeatResponse response = handler.call();
        
        Assert.assertTrue(response instanceof FrontendHbResponse);
        FrontendHbResponse hbResponse = (FrontendHbResponse) response;
        Assert.assertEquals(191224, hbResponse.getReplayedJournalId());
        Assert.assertEquals(9121, hbResponse.getRpcPort());
        Assert.assertEquals(9131, hbResponse.getQueryPort());
        Assert.assertEquals(HbStatus.OK, hbResponse.getStatus());

        Frontend fe2 = new Frontend(FrontendNodeType.FOLLOWER, "test2", "192.168.1.2", 9010);
        handler = new FrontendHeartbeatHandler(fe2, 12345, "abcd");
        response = handler.call();

        Assert.assertTrue(response instanceof FrontendHbResponse);
        hbResponse = (FrontendHbResponse) response;
        Assert.assertEquals(0, hbResponse.getReplayedJournalId());
        Assert.assertEquals(0, hbResponse.getRpcPort());
        Assert.assertEquals(0, hbResponse.getQueryPort());
        Assert.assertEquals(HbStatus.BAD, hbResponse.getStatus());

    }

    @Test
    public void testBrokerHbHandler(@Mocked TPaloBrokerService.Client client) throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus();
        status.setStatusCode(TBrokerOperationStatusCode.OK);

        new MockUp<GenericPool<TPaloBrokerService.Client>>() {
            @Mock
            public TPaloBrokerService.Client borrowObject(TNetworkAddress address) throws Exception {
                return client;
            }

            @Mock
            public void returnObject(TNetworkAddress address, TPaloBrokerService.Client object) {
                return;
            }

            @Mock
            public void invalidateObject(TNetworkAddress address, TPaloBrokerService.Client object) {
                return;
            }
        };

        new Expectations() {
            {
                client.ping((TBrokerPingBrokerRequest) any);
                minTimes = 0;
                result = status;
            }
        };

        FsBroker broker = new FsBroker("192.168.1.1", 8111);
        BrokerHeartbeatHandler handler = new BrokerHeartbeatHandler("hdfs", broker, "abc");
        HeartbeatResponse response = handler.call();

        Assert.assertTrue(response instanceof BrokerHbResponse);
        BrokerHbResponse hbResponse = (BrokerHbResponse) response;
        System.out.println(hbResponse.toString());
        Assert.assertEquals(HbStatus.OK, hbResponse.getStatus());
    }

}
