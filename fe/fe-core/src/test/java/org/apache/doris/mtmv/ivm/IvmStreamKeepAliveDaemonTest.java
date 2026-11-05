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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.tso.TSOService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;

public class IvmStreamKeepAliveDaemonTest {

    @Test
    public void testRoundWaitsForTheTsoServiceWithoutWaitingAnInterval() {
        boolean enableTableStream = Config.enable_table_stream;
        boolean enableKeepAlive = Config.enable_ivm_stream_keepalive;
        int interval = Config.ivm_stream_keepalive_interval_second;
        AtomicInteger rounds = new AtomicInteger();
        IvmStreamKeepAliveDaemon daemon = new IvmStreamKeepAliveDaemon() {
            @Override
            public void keepAliveIdlePartitions() {
                rounds.incrementAndGet();
            }
        };
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Config.enable_table_stream = true;
            Config.enable_ivm_stream_keepalive = true;
            Config.ivm_stream_keepalive_interval_second = 600;
            Env env = Mockito.mock(Env.class);
            TSOService tsoService = Mockito.mock(TSOService.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getTSOService()).thenReturn(tsoService);

            // A new master checks before its TSO service has calibrated itself: no round is started.
            Mockito.when(tsoService.getStatusSnapshot()).thenReturn(new TSOService.TSOStatusSnapshot(false, 0, 0));
            daemon.runAfterCatalogReady();
            daemon.runAfterCatalogReady();
            Assertions.assertEquals(0, rounds.get());

            // The first check after the TSO service is ready starts the round, not one an interval later.
            Mockito.when(tsoService.getStatusSnapshot()).thenReturn(new TSOService.TSOStatusSnapshot(true, 1, 0));
            daemon.runAfterCatalogReady();
            Assertions.assertEquals(1, rounds.get());

            // The next round waits for the interval.
            daemon.runAfterCatalogReady();
            Assertions.assertEquals(1, rounds.get());
        } finally {
            Config.enable_table_stream = enableTableStream;
            Config.enable_ivm_stream_keepalive = enableKeepAlive;
            Config.ivm_stream_keepalive_interval_second = interval;
        }
    }
}
