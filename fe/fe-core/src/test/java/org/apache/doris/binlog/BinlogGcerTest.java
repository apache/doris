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

package org.apache.doris.binlog;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.system.HeartbeatMgr;
import org.apache.doris.system.RowTtlFeatureGate;
import org.apache.doris.thrift.TMasterInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.tso.TSOService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

public class BinlogGcerTest {
    @Test
    @SuppressWarnings("unchecked")
    public void referenceCachePreservesFailuresAndFencesMasterChanges() throws Exception {
        Field field = HeartbeatMgr.class.getDeclaredField("masterInfo");
        field.setAccessible(true);
        AtomicReference<TMasterInfo> cache = (AtomicReference<TMasterInfo>) field.get(null);
        TMasterInfo previous = cache.get();
        boolean enabled = Config.enable_feature_binlog;
        TSOService tso = Mockito.mock(TSOService.class);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<RowTtlFeatureGate> gate = Mockito.mockStatic(RowTtlFeatureGate.class)) {
            Config.enable_feature_binlog = true;
            cache.set(new TMasterInfo(new TNetworkAddress("master", 9020), 1, 1));
            env.when(Env::getCurrentTSOService).thenReturn(tso);
            Mockito.when(tso.getTSO()).thenReturn(100L, 50L, 0L)
                    .thenThrow(new IllegalStateException("unavailable"));
            for (int i = 0; i < 4; i++) {
                HeartbeatMgr.refreshRowBinlogTtlReferenceTso();
                Assertions.assertEquals(100L, cache.get().getRowBinlogTtlReferenceTso());
            }
            Mockito.doAnswer(invocation -> {
                cache.set(new TMasterInfo(new TNetworkAddress("new-master", 9020), 1, 2));
                return 200L;
            }).when(tso).getTSO();
            HeartbeatMgr.refreshRowBinlogTtlReferenceTso();
            Assertions.assertFalse(cache.get().isSetRowBinlogTtlReferenceTso());
            Config.enable_feature_binlog = false;
            Mockito.clearInvocations(tso);
            HeartbeatMgr.refreshRowBinlogTtlReferenceTso();
            Mockito.verifyNoInteractions(tso);
        } finally {
            cache.set(previous);
            Config.enable_feature_binlog = enabled;
        }
    }
}
