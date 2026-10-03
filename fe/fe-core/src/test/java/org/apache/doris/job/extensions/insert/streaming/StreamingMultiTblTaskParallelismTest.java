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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.common.Config;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.jdbc.JdbcOffset;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

class StreamingMultiTblTaskParallelismTest {
    @Test
    void taskUsesHalfSelectedBackendCpuWithoutChangingJobProperties() throws Exception {
        int oldMax = Config.streaming_cdc_max_snapshot_parallelism;
        try {
            Config.streaming_cdc_max_snapshot_parallelism = 16;
            assertParallelism(0, null, 1);
            assertParallelism(1, null, 1);
            assertParallelism(4, null, 2);
            assertParallelism(8, null, 4);
            assertParallelism(16, null, 8);
            assertParallelism(32, null, 16);
            assertParallelism(8, "1", 1);
            assertParallelism(8, "4", 4);
            assertParallelism(8, "10000", 4);
            assertParallelism(32, "10000", 16);
            assertParallelism(8, null, 4, DataSourceType.MYSQL, null);
            assertParallelism(8, null, 1, DataSourceType.MYSQL, "5400");
            assertParallelism(8, null, 4, DataSourceType.MYSQL, "5400-5403");
            assertParallelism(8, null, 1, DataSourceType.OCEANBASE, "5400");
            Config.streaming_cdc_max_snapshot_parallelism = 4;
            assertParallelism(8, null, 4);
            Config.streaming_cdc_max_snapshot_parallelism = 1;
            assertParallelism(8, null, 1);
            assertParallelism(8, "10000", 1);
            Config.streaming_cdc_max_snapshot_parallelism = 0;
            assertParallelism(8, null, 1);
        } finally {
            Config.streaming_cdc_max_snapshot_parallelism = oldMax;
        }
    }

    private void assertParallelism(int cpuCores, String requested, int expected) throws Exception {
        assertParallelism(cpuCores, requested, expected, DataSourceType.MYSQL, "5400-5415");
    }

    private void assertParallelism(int cpuCores, String requested, int expected,
            DataSourceType sourceType, String serverId) throws Exception {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(7L);
        Mockito.when(backend.getCputCores()).thenReturn(cpuCores);
        SourceOffsetProvider provider = Mockito.mock(SourceOffsetProvider.class);
        Mockito.when(provider.isSnapshotPhase()).thenReturn(true);
        Mockito.when(provider.getNextOffset(Mockito.isNull(), Mockito.anyMap()))
                .thenReturn(Mockito.mock(JdbcOffset.class));
        Map<String, String> properties = new HashMap<>();
        if (serverId != null) {
            properties.put(DataSourceConfigKeys.SERVER_ID, serverId);
        }
        if (requested != null) {
            properties.put(DataSourceConfigKeys.SNAPSHOT_PARALLELISM, requested);
        }
        Map<String, String> original = new HashMap<>(properties);
        StreamingMultiTblTask task = new StreamingMultiTblTask(
                1L, 2L, sourceType, provider, properties, "db", null, null, null, null);
        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class)) {
            utils.when(() -> StreamingJobUtils.selectBackend(null)).thenReturn(backend);
            task.before();
            utils.verify(() -> StreamingJobUtils.selectBackend(null));
        }
        Assertions.assertEquals(7L, task.getRunningBackendId());
        Assertions.assertEquals(String.valueOf(expected),
                task.getSourceProperties().get(DataSourceConfigKeys.SNAPSHOT_PARALLELISM));
        Assertions.assertEquals(original, properties);
        Mockito.verify(provider).getNextOffset(Mockito.isNull(), Mockito.same(task.getSourceProperties()));
    }
}
