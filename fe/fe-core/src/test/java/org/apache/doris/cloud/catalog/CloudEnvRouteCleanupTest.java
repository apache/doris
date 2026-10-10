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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicLong;

public class CloudEnvRouteCleanupTest {
    private boolean savedClean;
    private CloudEnv env;
    private CloudSystemInfoService systemInfo;
    private CloudInternalCatalog catalog;
    private MockedStatic<Env> envStatic;

    @BeforeEach
    public void setUp() {
        savedClean = Config.enable_cloud_replica_stale_route_clean;
        Config.enable_cloud_replica_stale_route_clean = true;
        env = Mockito.mock(CloudEnv.class, Mockito.CALLS_REAL_METHODS);
        systemInfo = new CloudSystemInfoService();
        catalog = Mockito.mock(CloudInternalCatalog.class);
        Mockito.doReturn(systemInfo).when(env).getClusterInfo();
        Mockito.doReturn(catalog).when(env).getInternalCatalog();
        Deencapsulation.setField(env, "replayedJournalId", new AtomicLong());
        Deencapsulation.setField(env, "feType", FrontendNodeType.MASTER);
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
    }

    @AfterEach
    public void tearDown() {
        envStatic.close();
        Config.enable_cloud_replica_stale_route_clean = savedClean;
    }

    @Test
    public void testCoalescesDropsAtBatchEndEvenWhenFinalTopologyIsUnchanged() {
        // Startup and idle batches: exactly one sweep, even without route update journals.
        Assertions.assertFalse(env.replayJournal(0));
        Assertions.assertFalse(env.replayJournal(0));
        Mockito.verify(catalog, Mockito.times(1)).removeInvalidCloudReplicaRoutes(systemInfo);
        Mockito.clearInvocations(catalog);

        Backend first = backend(101L);
        Backend second = backend(102L);
        EditLog editLog = Mockito.mock(EditLog.class);
        JournalCursor cursor = Mockito.mock(JournalCursor.class);
        Deencapsulation.setField(env, "editLog", editLog);
        Mockito.when(editLog.read(1L, 4L)).thenReturn(cursor);
        Mockito.when(cursor.next()).thenReturn(
                journal(1L, OperationType.OP_ADD_BACKEND, first),
                journal(2L, OperationType.OP_ADD_BACKEND, second),
                journal(3L, OperationType.OP_DROP_BACKEND, first),
                journal(4L, OperationType.OP_DROP_BACKEND, second), null);
        Mockito.when(catalog.removeInvalidCloudReplicaRoutes(systemInfo)).thenAnswer(invocation -> {
            Assertions.assertNull(systemInfo.getBackend(101L));
            Assertions.assertNull(systemInfo.getBackend(102L));
            Assertions.assertEquals(4L, env.getReplayedJournalId());
            return 0L;
        });

        Assertions.assertTrue(env.replayJournal(4));
        Assertions.assertEquals(2L, systemInfo.getReplayBackendRemovalVersion());
        Assertions.assertFalse(env.replayJournal(4));
        Mockito.verify(catalog, Mockito.times(1)).removeInvalidCloudReplicaRoutes(systemInfo);

        // Backend additions alone do not trigger cleanup.
        systemInfo.replayAddBackend(backend(103L));
        Assertions.assertFalse(env.replayJournal(4));
        Mockito.verify(catalog, Mockito.times(1)).removeInvalidCloudReplicaRoutes(systemInfo);
    }

    @Test
    public void testRetriesFailedSweepAndSweepsAfterReenable() {
        Mockito.when(catalog.removeInvalidCloudReplicaRoutes(systemInfo))
                .thenThrow(new IllegalStateException("test sweep failure")).thenReturn(0L);
        Assertions.assertThrows(IllegalStateException.class, () -> env.replayJournal(0));
        Assertions.assertFalse(env.replayJournal(0));
        Assertions.assertFalse(env.replayJournal(0));
        Mockito.verify(catalog, Mockito.times(2)).removeInvalidCloudReplicaRoutes(systemInfo);
        Config.enable_cloud_replica_stale_route_clean = false;
        Assertions.assertFalse(env.replayJournal(0));
        Config.enable_cloud_replica_stale_route_clean = true;
        Assertions.assertFalse(env.replayJournal(0));
        Mockito.verify(catalog, Mockito.times(3)).removeInvalidCloudReplicaRoutes(systemInfo);
    }

    @Test
    public void testCheckpointDefersSweepUntilAfterMetadataPostProcessing() {
        envStatic.when(Env::isCheckpointThread).thenReturn(true);
        Assertions.assertFalse(env.replayJournal(0));
        Mockito.verifyNoInteractions(catalog);
    }

    private static Backend backend(long id) {
        Backend backend = new Backend(id, "127.0.0.1", (int) (9050 + id));
        backend.setTagMap(ImmutableMap.of(Tag.TYPE_LOCATION, "default",
                Tag.CLOUD_CLUSTER_ID, "cg", Tag.CLOUD_CLUSTER_NAME, "cg"));
        return backend;
    }

    private static Pair<Long, JournalEntity> journal(long id, short opCode, Backend backend) {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(opCode);
        entity.setData(backend);
        return Pair.of(id, entity);
    }
}
