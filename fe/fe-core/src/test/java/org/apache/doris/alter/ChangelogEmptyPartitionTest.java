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

package org.apache.doris.alter;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.CreateReplicaTask;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TCreatePartitionRequest;
import org.apache.doris.thrift.TCreatePartitionResult;
import org.apache.doris.thrift.TCreateTabletReq;
import org.apache.doris.thrift.TNullableStringLiteral;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ChangelogEmptyPartitionTest extends TestWithFeService {
    private boolean previousChangelogEnabled;

    @Override
    protected void runBeforeAll() throws Exception {
        previousChangelogEnabled = Config.enable_feature_changelog;
        Config.enable_feature_changelog = true;
        createDatabase("empty_changelog_schema");
        connectContext.setDatabase("empty_changelog_schema");
    }

    @Override
    protected void runAfterAll() {
        Config.enable_feature_changelog = previousChangelogEnabled;
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void firstPartitionPreservesCreationTimeIdentities(boolean automatic, boolean lightSchemaChange) throws Exception {
        String name = (automatic ? "automatic_" : "manual_") + lightSchemaChange;
        createTable("CREATE TABLE empty_changelog_schema." + name
                + " (k DATETIME NOT NULL, v INT) DUPLICATE KEY(k) "
                + "AUTO PARTITION BY RANGE(date_trunc(k, 'day')) () "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("
                + "\"replication_num\"=\"1\", \"light_schema_change\"=\"" + lightSchemaChange + "\","
                + "\"changelog.enable\"=\"true\", \"changelog.need_historical_value\"=\"false\")");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("empty_changelog_schema");
        OlapTable table = db.getOlapTableOrDdlException(name);
        Assertions.assertTrue(table.getAllPartitions().isEmpty());
        Map<Long, Map<String, Integer>> originalIds = new HashMap<>();
        Map<Long, Integer> originalVersions = new HashMap<>();
        Map<Long, Integer> originalMaximums = new HashMap<>();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : table.getIndexIdToMeta(true).entrySet()) {
            MaterializedIndexMeta meta = entry.getValue();
            Map<String, Integer> ids = columnIds(meta.getSchema(true));
            Assertions.assertEquals(meta.getSchema(true).size(), ids.size());
            Assertions.assertTrue(ids.values().stream().allMatch(id -> id >= 0));
            Assertions.assertEquals(ids.size(), ids.values().stream().distinct().count());
            originalIds.put(entry.getKey(), ids);
            originalVersions.put(entry.getKey(), meta.getSchemaVersion());
            originalMaximums.put(entry.getKey(), meta.getMaxColUniqueId());
        }
        Map<Long, Map<String, Integer>> sent = new HashMap<>();
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(
                AgentTaskExecutor.class, Mockito.CALLS_REAL_METHODS)) {
            executor.when(() -> AgentTaskExecutor.submit(Mockito.any(AgentBatchTask.class)))
                    .thenAnswer(invocation -> {
                        AgentBatchTask batch = invocation.getArgument(0);
                        for (AgentTask task : batch.getAllTasks()) {
                            if (task instanceof CreateReplicaTask) {
                                TCreateTabletReq wire = ((CreateReplicaTask) task).toThrift();
                                Map<String, Integer> ids = wire.getTabletSchema().getColumns().stream()
                                        .collect(Collectors.toMap(TColumn::getColumnName, TColumn::getColUniqueId));
                                Assertions.assertTrue(ids.values().stream().allMatch(id -> id >= 0));
                                Assertions.assertEquals(ids.size(), ids.values().stream().distinct().count());
                                sent.put(task.getIndexId(), ids);
                            }
                        }
                        return invocation.callRealMethod();
                    });
            if (automatic) {
                TCreatePartitionRequest request = new TCreatePartitionRequest()
                        .setDbId(db.getId()).setTableId(table.getId())
                        .setPartitionValues(Collections.singletonList(Collections.singletonList(
                                new TNullableStringLiteral().setValue("2026-10-26 00:00:00"))));
                TCreatePartitionResult result = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class))
                        .createPartition(request);
                Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode(), result.toString());
            } else {
                executeSql("ALTER TABLE empty_changelog_schema." + name
                        + " ADD PARTITION p20261026 VALUES [('2026-10-26'), ('2026-10-27'))");
            }
        }
        Assertions.assertEquals(1, table.getAllPartitions().size());
        Assertions.assertEquals(lightSchemaChange, table.getEnableLightSchemaChange());
        Assertions.assertEquals(originalIds, sent);
        for (Map.Entry<Long, MaterializedIndexMeta> entry : table.getIndexIdToMeta(true).entrySet()) {
            MaterializedIndexMeta meta = entry.getValue();
            Assertions.assertEquals(originalIds.get(entry.getKey()), columnIds(meta.getSchema(true)));
            Assertions.assertEquals(originalVersions.get(entry.getKey()).intValue(), meta.getSchemaVersion());
            Assertions.assertEquals(originalMaximums.get(entry.getKey()).intValue(), meta.getMaxColUniqueId());
        }
    }

    private static Map<String, Integer> columnIds(List<Column> schema) {
        return schema.stream().collect(Collectors.toMap(Column::getName, Column::getUniqueId));
    }
}
