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

import org.apache.doris.system.SystemInfoService;

import mockit.Mock;
import mockit.MockUp;

public class FakeEnv extends MockUp<Env> {

    private static Env env;
    private static int metaVersion;
    private static SystemInfoService systemInfo = new SystemInfoService();

    public static void setEnv(Env env) {
        FakeEnv.env = env;
    }

    public static void setMetaVersion(int metaVersion) {
        FakeEnv.metaVersion = metaVersion;
    }

    public static void setSystemInfo(SystemInfoService systemInfo) {
        FakeEnv.systemInfo = systemInfo;
    }

    @Mock
    public static Env getCurrentEnv() {
        return env;
    }

    @Mock
    public static Env getInstance() {
        return env;
    }

    @Mock
    public static int getCurrentEnvJournalVersion() {
        return metaVersion;
    }

    @Mock
    public static SystemInfoService getCurrentSystemInfo() {
        return systemInfo;
    }

}
