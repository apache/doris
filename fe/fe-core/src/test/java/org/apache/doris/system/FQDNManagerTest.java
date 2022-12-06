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

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class FQDNManagerTest {
    @Mocked
    private InetAddress inetAddress;

    @Mocked
    private Env env;

    private FQDNManager fdqnManager;

    private SystemInfoService systemInfoService;

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
        };

        new Expectations() {
            {
                env.isReady();
                minTimes = 0;
                result = true;

                inetAddress.getHostAddress();
                minTimes = 0;
                result = "193.88.67.99";
            }
        };

        Config.enable_fqdn_mode = true;
        systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(new Backend(1, "193.88.67.98", "doris.test.domain", 9090));
        fdqnManager = new FQDNManager(systemInfoService);
    }

    @Test
    public void testBackendIpChanged() throws InterruptedException {
        Assert.assertEquals("193.88.67.98", systemInfoService.getBackend(1).getHost());
        fdqnManager.start();
        Thread.sleep(1000);
        Assert.assertEquals("193.88.67.99", systemInfoService.getBackend(1).getHost());
        fdqnManager.exit();
    }
}
