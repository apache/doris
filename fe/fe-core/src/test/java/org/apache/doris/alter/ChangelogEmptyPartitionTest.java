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
import org.apache.doris.persist.gson.GsonUtils;
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
import org.junit.jupiter.params.provider.ValueSource;
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
        createDatabase("empty_changelog_bootstrap");
        connectContext.setDatabase("empty_changelog_bootstrap");
    }

    @Override
    protected void runAfterAll() {
        Config.enable_feature_changelog = previousChangelogEnabled;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void firstPartitionUsesInitializedIdentities(boolean automatic) throws Exception {
        String name = automatic ? "automatic_bootstrap" : "manual_bootstrap";
        createTable("CREATE TABLE empty_changelog_bootstrap." + name
                + " (k DATETIME NOT NULL, v INT) DUPLICATE KEY(k) "
                + "AUTO PARTITION BY RANGE(date_trunc(k, 'day')) () "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("
                + "\"replication_num\"=\"1\", \"light_schema_change\"=\"false\","
                + "\"changelog.enable\"=\"true\", \"changelog.need_historical_value\"=\"false\")");
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("empty_changelog_bootstrap");
        OlapTable created = db.getOlapTableOrDdlException(name);
        Assertions.assertTrue(created.getAllPartitions().isEmpty());
        // Reproduce the old catalog: source IDs were never initialized, companion IDs were.
        created.getBaseSchema(true).forEach(column -> column.setUniqueId(-1));
        created.getBaseIndexMeta().setMaxColUniqueId(-1);
        OlapTable restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(created), OlapTable.class);
        db.unregisterTable(name);
        db.registerTable(restored);
        MaterializedIndexMeta companion = restored.getChangelogIndexMeta();
        Map<String, Integer> companionIds = columnIds(companion.getSchema(true));
        int sourceVersion = restored.getBaseSchemaVersion();
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
                        .setDbId(db.getId()).setTableId(restored.getId())
                        .setPartitionValues(Collections.singletonList(Collections.singletonList(
                                new TNullableStringLiteral().setValue("2026-10-26 00:00:00"))));
                TCreatePartitionResult result = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class))
                        .createPartition(request);
                Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode(), result.toString());
            } else {
                executeSql("ALTER TABLE empty_changelog_bootstrap." + name
                        + " ADD PARTITION p20261026 VALUES [('2026-10-26'), ('2026-10-27'))");
            }
        }
        Assertions.assertEquals(1, restored.getAllPartitions().size());
        Assertions.assertFalse(restored.getEnableLightSchemaChange());
        Assertions.assertEquals(sourceVersion + 1, restored.getBaseSchemaVersion());
        Assertions.assertEquals(columnIds(restored.getBaseSchema(true)), sent.get(restored.getBaseIndexId()));
        Assertions.assertEquals(companionIds, sent.get(companion.getIndexId()));
        Assertions.assertEquals(companionIds, columnIds(companion.getSchema(true)));
    }

    private static Map<String, Integer> columnIds(List<Column> schema) {
        return schema.stream().collect(Collectors.toMap(Column::getName, Column::getUniqueId));
    }
}
