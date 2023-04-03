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
import org.apache.doris.common.Config;
import org.apache.doris.ha.BDBHA;
import org.apache.doris.ha.FrontendNodeType;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class FQDNManagerTest {
    @Mocked
    private InetAddress inetAddress;

    @Mocked
    private Env env;

    private FQDNManager fdqnManager;

    private SystemInfoService systemInfoService;

    List<Frontend> frontends = new ArrayList<>();

    @Mocked
    private BDBHA bdbha;

    @Before
    public void setUp() throws UnknownHostException {
        new MockUp<InetAddress>(InetAddress.class) {
            @Mock
            public InetAddress getByName(String hostName) {
                return inetAddress;
            }
        };

        new MockUp<Env>(Env.class) {
            @Mock
            public Env getServingEnv() {
                return env;
            }

            @Mock
            public Env getCurrentEnv() {
                return env;
            }

            @Mock
            public boolean tryLock() {
                return true;
            }

            @Mock
            public void modifyFrontendIp(String nodeName, String ip) {
                for (Frontend fe : frontends) {
                    if (fe.getNodeName().equals(nodeName)) {
                        fe.setIp(ip);
                    }
                }
            }
        };

        new MockUp<BDBHA>(BDBHA.class) {
            @Mock
            public boolean updateNodeAddress(String nodeName, String ip, int port) {
                return true;
            }
        };

        Config.enable_fqdn_mode = true;
        systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(new Backend(1, "193.88.67.98", "doris.test.domain", 9090));
        frontends.add(new Frontend(FrontendNodeType.FOLLOWER, "doris.test.domain_9010_1675168383846",
                "193.88.67.90", "doris.test.domain", 9010));
        fdqnManager = new FQDNManager(systemInfoService);

        new Expectations() {
            {
                env.isReady();
                minTimes = 0;
                result = true;

                inetAddress.getHostAddress();
                minTimes = 0;
                result = "193.88.67.99";

                env.getFrontends(null);
                minTimes = 0;
                result = frontends;

                env.getFeByName("doris.test.domain_9010_1675168383846");
                minTimes = 0;
                result = frontends.get(0);

                env.getHaProtocol();
                minTimes = 0;
                result = bdbha;
            }
        };
    }

    @Test
    public void testBackendIpChanged() throws InterruptedException {
        Assert.assertEquals("193.88.67.98", systemInfoService.getBackend(1).getIp());
        fdqnManager.start();
        Thread.sleep(1000);
        Assert.assertEquals("193.88.67.99", systemInfoService.getBackend(1).getIp());
        fdqnManager.exit();
    }

    @Test
    public void testFrontendChanged() throws InterruptedException {
        // frontend ip changed
        Assert.assertEquals("193.88.67.90", env.getFrontends(null).get(0).getIp());
        fdqnManager.start();
        Thread.sleep(1000);
        Assert.assertEquals("193.88.67.99", env.getFrontends(null).get(0).getIp());
        fdqnManager.exit();
    }
}
