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

package org.apache.doris.common;

import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;


public class ClientPool {
    static GenericKeyedObjectPoolConfig heartbeatConfig = new GenericKeyedObjectPoolConfig();
    static int heartbeatTimeoutMs = Config.heartbeat_interval_second * 1000;

    static GenericKeyedObjectPoolConfig backendConfig = new GenericKeyedObjectPoolConfig();

    static {
        heartbeatConfig.setLifo(true);            // set Last In First Out strategy
        heartbeatConfig.setMaxIdlePerKey(2);      // (default 8)
        heartbeatConfig.setMinIdlePerKey(1);      // (default 0)
        heartbeatConfig.setMaxTotalPerKey(-1);    // (default 8)
        heartbeatConfig.setMaxTotal(-1);          // (default -1)
        heartbeatConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    static {
        backendConfig.setLifo(true);            // set Last In First Out strategy
        backendConfig.setMaxIdlePerKey(128);      // (default 8)
        backendConfig.setMinIdlePerKey(2);      // (default 0)
        backendConfig.setMaxTotalPerKey(-1);    // (default 8)
        backendConfig.setMaxTotal(-1);          // (default -1)
        backendConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    static GenericKeyedObjectPoolConfig brokerPoolConfig = new GenericKeyedObjectPoolConfig();
    static int brokerTimeoutMs = Config.broker_timeout_ms;

    static {
        brokerPoolConfig.setLifo(true);            // set Last In First Out strategy
        brokerPoolConfig.setMaxIdlePerKey(128);      // (default 8)
        brokerPoolConfig.setMinIdlePerKey(2);      // (default 0)
        brokerPoolConfig.setMaxTotalPerKey(-1);    // (default 8)
        brokerPoolConfig.setMaxTotal(-1);          // (default -1)
        brokerPoolConfig.setMaxWaitMillis(500);    //  wait for the connection
    }

    public static GenericPool<HeartbeatService.Client> backendHeartbeatPool =
            new GenericPool("HeartbeatService", heartbeatConfig, heartbeatTimeoutMs);
    public static GenericPool<FrontendService.Client> frontendHeartbeatPool =
            new GenericPool<>("FrontendService", heartbeatConfig, heartbeatTimeoutMs,
                    Config.thrift_server_type.equalsIgnoreCase(ThriftServer.THREADED_SELECTOR));
    public static GenericPool<FrontendService.Client> frontendPool =
            new GenericPool("FrontendService", backendConfig, Config.backend_rpc_timeout_ms,
                    Config.thrift_server_type.equalsIgnoreCase(ThriftServer.THREADED_SELECTOR));
    public static GenericPool<BackendService.Client> backendPool =
            new GenericPool("BackendService", backendConfig, Config.backend_rpc_timeout_ms);
    public static GenericPool<TPaloBrokerService.Client> brokerPool =
            new GenericPool("TPaloBrokerService", brokerPoolConfig, brokerTimeoutMs);
}
