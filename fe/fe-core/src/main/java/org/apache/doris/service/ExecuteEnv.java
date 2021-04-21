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

package org.apache.doris.service;

import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.MultiLoadMgr;

// Execute environment, used to save other module, need to singleton
public class ExecuteEnv {
    private static volatile ExecuteEnv INSTANCE;
    private MultiLoadMgr multiLoadMgr;
    private ConnectScheduler scheduler;

    private ExecuteEnv() {
        multiLoadMgr = new MultiLoadMgr();
        scheduler = new ConnectScheduler(Config.qe_max_connection);
    }

    public static ExecuteEnv getInstance() {
        if (INSTANCE == null) {
            synchronized (ExecuteEnv.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ExecuteEnv();
                }
            }
        }
        return INSTANCE;
    }

    public ConnectScheduler getScheduler() {
        return scheduler;
    }

    public MultiLoadMgr getMultiLoadMgr() {
        return multiLoadMgr;
    }
}
