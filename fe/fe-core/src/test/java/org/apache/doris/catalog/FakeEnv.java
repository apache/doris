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

package org.apache.doris.catalog;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.SystemInfoService;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class FakeEnv implements AutoCloseable {

    private static Env env;
    private static int metaVersion;
    private static SystemInfoService systemInfo = new SystemInfoService();
    private MockedStatic<Env> mockedStatic;

    public FakeEnv() {
        mockedStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
        mockedStatic.when(Env::getCurrentEnv).thenAnswer(inv -> env);
        mockedStatic.when(Env::getServingEnv).thenAnswer(inv -> env);
        mockedStatic.when(Env::getCurrentEnvJournalVersion).thenAnswer(inv -> metaVersion);
        mockedStatic.when(Env::getCurrentSystemInfo).thenAnswer(inv -> systemInfo);
    }

    public static void setEnv(Env env) {
        // Set feType to MASTER to replicate original isMaster() mock behavior
        Deencapsulation.setField(env, "feType", FrontendNodeType.MASTER);
        FakeEnv.env = env;
    }

    public static void setMetaVersion(int metaVersion) {
        FakeEnv.metaVersion = metaVersion;
    }

    public static void setSystemInfo(SystemInfoService systemInfo) {
        FakeEnv.systemInfo = systemInfo;
    }

    @Override
    public void close() {
        if (mockedStatic != null) {
            mockedStatic.close();
            mockedStatic = null;
        }
    }

}
