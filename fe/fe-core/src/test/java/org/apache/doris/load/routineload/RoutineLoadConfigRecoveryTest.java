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

package org.apache.doris.load.routineload;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.nereids.load.NereidsRoutineLoadTaskInfo;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RoutineLoadConfigRecoveryTest {
    private static final long NON_DEFAULT_MEMORY_LIMIT = 3_333_333_333L;

    @ParameterizedTest
    @EnumSource(value = LoadDataSourceType.class, names = {"KAFKA", "KINESIS"})
    public void testSerializedPropertiesRestoreRuntimeConfiguration(LoadDataSourceType sourceType) throws Exception {
        RoutineLoadJob original = newJob(sourceType);
        original.maxFilterRatio = 0.25;
        original.execMemLimit = NON_DEFAULT_MEMORY_LIMIT;
        original.sendBatchParallelism = 3;
        original.loadToSingleTablet = true;
        original.jobProperties.put(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.25");
        original.jobProperties.put(CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY, "3333333333");
        original.jobProperties.put(CreateRoutineLoadInfo.SEND_BATCH_PARALLELISM, "3");
        original.jobProperties.put(CreateRoutineLoadInfo.LOAD_TO_SINGLE_TABLET, "true");

        RoutineLoadJob recovered = roundTrip(original, createInfo(Map.of()));
        assertConfiguration(recovered, 0.25, NON_DEFAULT_MEMORY_LIMIT, 3, true);

        NereidsRoutineLoadTaskInfo taskInfo = recovered.toNereidsRoutineLoadTaskInfo();
        Assertions.assertEquals(NON_DEFAULT_MEMORY_LIMIT, taskInfo.getMemLimit());
        Assertions.assertEquals(0.25, taskInfo.getMaxFilterRatio());
        Assertions.assertEquals(3, taskInfo.getSendBatchParallelism());
        Assertions.assertTrue(taskInfo.isLoadToSingleTablet());

        // A subsequent checkpoint must not change the recovered configuration.
        RoutineLoadJob recoveredAgain = roundTrip(recovered, createInfo(Map.of()));
        assertConfiguration(recoveredAgain, 0.25, NON_DEFAULT_MEMORY_LIMIT, 3, true);
    }

    @Test
    public void testLegacyMetadataRecoversMemoryLimitFromCreateStatement() throws Exception {
        RoutineLoadJob original = newJob(LoadDataSourceType.KAFKA);
        original.execMemLimit = NON_DEFAULT_MEMORY_LIMIT;
        original.jobProperties.put(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.0");
        original.jobProperties.put(CreateRoutineLoadInfo.SEND_BATCH_PARALLELISM, "2");
        original.jobProperties.put(CreateRoutineLoadInfo.LOAD_TO_SINGLE_TABLET, "true");
        // Old metadata has no persisted exec_mem_limit entry.

        RoutineLoadJob recovered = roundTrip(original, createInfo(Map.of(
                CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY, "3333333333")));
        assertConfiguration(recovered, 0.0, NON_DEFAULT_MEMORY_LIMIT, 2, true);
        Assertions.assertEquals("3333333333",
                recovered.jobProperties.get(CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY));

        RoutineLoadJob recoveredAgain = roundTrip(recovered, createInfo(Map.of()));
        assertConfiguration(recoveredAgain, 0.0, NON_DEFAULT_MEMORY_LIMIT, 2, true);
    }

    @Test
    public void testAlteredFilterRatioOverridesOriginalCreateValue() throws Exception {
        RoutineLoadJob original = newJob(LoadDataSourceType.KAFKA);
        original.maxFilterRatio = 0.5;
        original.jobProperties.put(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.5");
        original.modifyCommonJobProperties(new HashMap<>(Map.of(
                CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.0")));

        RoutineLoadJob recovered = roundTrip(original, createInfo(Map.of(
                CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.5")));
        Assertions.assertEquals(0.0, recovered.maxFilterRatio);
        Assertions.assertEquals("0.0",
                recovered.jobProperties.get(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY));
    }

    @Test
    public void testLegacyMetadataCanRecoverExplicitCreateProperties() throws Exception {
        RoutineLoadJob original = newJob(LoadDataSourceType.KAFKA);
        RoutineLoadJob recovered = roundTrip(original, createInfo(Map.of(
                CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.1",
                CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY, "3333333333",
                CreateRoutineLoadInfo.SEND_BATCH_PARALLELISM, "4",
                CreateRoutineLoadInfo.LOAD_TO_SINGLE_TABLET, "true")));
        assertConfiguration(recovered, 0.1, NON_DEFAULT_MEMORY_LIMIT, 4, true);
    }

    @Test
    public void testSessionResolvedParallelismDoesNotChangeDuringRecovery() throws Exception {
        RoutineLoadJob original = newJob(LoadDataSourceType.KAFKA);
        original.sendBatchParallelism = 4;
        original.jobProperties.put(CreateRoutineLoadInfo.SEND_BATCH_PARALLELISM, "4");

        RoutineLoadJob recovered = roundTrip(original, createInfo(Map.of()));
        Assertions.assertEquals(4, recovered.sendBatchParallelism);
    }

    @Test
    public void testLegacyMetadataWithoutPropertiesUsesDefaults() throws Exception {
        RoutineLoadJob recovered = roundTrip(newJob(LoadDataSourceType.KAFKA), createInfo(Map.of()));
        assertConfiguration(recovered, RoutineLoadJob.DEFAULT_MAX_FILTER_RATIO,
                RoutineLoadJob.DEFAULT_EXEC_MEM_LIMIT, RoutineLoadJob.DEFAULT_SEND_BATCH_PARALLELISM,
                RoutineLoadJob.DEFAULT_LOAD_TO_SINGLE_TABLET);
    }

    @Test
    public void testInvalidPersistedConfigurationCancelsJob() throws Exception {
        Map<String, String> invalidProperties = new HashMap<>();
        invalidProperties.put(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "NaN");
        invalidProperties.put(CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY, "-1");
        invalidProperties.put(CreateRoutineLoadInfo.SEND_BATCH_PARALLELISM, "0");
        invalidProperties.put(CreateRoutineLoadInfo.LOAD_TO_SINGLE_TABLET, "not_boolean");

        for (Map.Entry<String, String> property : invalidProperties.entrySet()) {
            RoutineLoadJob original = newJob(LoadDataSourceType.KAFKA);
            original.jobProperties.put(property.getKey(), property.getValue());
            RoutineLoadJob recovered = roundTrip(original, createInfo(Map.of()));

            Assertions.assertEquals(RoutineLoadJob.JobState.CANCELLED, recovered.getState(), property.getKey());
            Assertions.assertNotNull(recovered.cancelReason, property.getKey());
        }
    }

    @Test
    public void testAdditionalMemoryPropertyDoesNotChangeTaskConfiguration() throws Exception {
        RoutineLoadJob original = newJob(LoadDataSourceType.KAFKA);
        original.execMemLimit = NON_DEFAULT_MEMORY_LIMIT;
        original.maxFilterRatio = 0.25;
        original.sendBatchParallelism = 3;
        original.loadToSingleTablet = true;

        NereidsRoutineLoadTaskInfo withoutProperty = original.toNereidsRoutineLoadTaskInfo();
        original.jobProperties.put(CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY, "3333333333");
        NereidsRoutineLoadTaskInfo withProperty = original.toNereidsRoutineLoadTaskInfo();

        Assertions.assertEquals(withoutProperty.getMemLimit(), withProperty.getMemLimit());
        Assertions.assertEquals(withoutProperty.getMaxFilterRatio(), withProperty.getMaxFilterRatio());
        Assertions.assertEquals(withoutProperty.getSendBatchParallelism(), withProperty.getSendBatchParallelism());
        Assertions.assertEquals(withoutProperty.isLoadToSingleTablet(), withProperty.isLoadToSingleTablet());
    }

    @Test
    public void testCreationPersistsResolvedMemoryLimit() throws Exception {
        RoutineLoadJob job = newJob(LoadDataSourceType.KAFKA);
        CreateRoutineLoadInfo info = createInfo(Map.of(
                CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY, "3333333333"));
        Mockito.when(info.getRoutineLoadDesc()).thenReturn(null);
        Mockito.when(info.getUniqueKeyUpdateMode()).thenReturn(TUniqueKeyUpdateMode.UPSERT);
        Mockito.when(info.getFileFormatProperties()).thenReturn(Mockito.mock(JsonFileFormatProperties.class));
        Mockito.when(info.getDataSourceProperties()).thenReturn(Mockito.mock(KafkaDataSourceProperties.class));

        job.setOptional(info);

        Assertions.assertEquals("3333333333", job.jobProperties.get(CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY));
        JsonObject metadata = JsonParser.parseString(GsonUtils.GSON.toJson(job, RoutineLoadJob.class))
                .getAsJsonObject();
        Assertions.assertEquals("3333333333", metadata.getAsJsonObject("jp")
                .get(CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY).getAsString());
    }

    private static RoutineLoadJob roundTrip(RoutineLoadJob original, CreateRoutineLoadInfo info) {
        original.origStmt = new OriginStatement("CREATE ROUTINE LOAD job ON tbl PROPERTIES () FROM KAFKA ()", 0);
        String metadata = GsonUtils.GSON.toJson(original, RoutineLoadJob.class);

        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Database database = Mockito.mock(Database.class);
        CreateRoutineLoadCommand command = Mockito.mock(CreateRoutineLoadCommand.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getDb("test_db")).thenReturn(Optional.of(database));
        Mockito.when(catalog.getDb(1L)).thenReturn(Optional.of(database));
        Mockito.when(database.getName()).thenReturn("test_db");
        Mockito.when(command.getCreateRoutineLoadInfo()).thenReturn(info);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedConstruction<NereidsParser> parsers = Mockito.mockConstruction(NereidsParser.class,
                        (parser, context) -> Mockito.when(parser.parseSingle(Mockito.anyString()))
                                .thenReturn(command))) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            return GsonUtils.GSON.fromJson(metadata, RoutineLoadJob.class);
        }
    }

    private static CreateRoutineLoadInfo createInfo(Map<String, String> createProperties) {
        CreateRoutineLoadInfo info = Mockito.mock(CreateRoutineLoadInfo.class);
        Mockito.when(info.getJobProperties()).thenReturn(createProperties);
        Mockito.when(info.getMaxFilterRatio()).thenReturn(Double.parseDouble(createProperties.getOrDefault(
                CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "1.0")));
        Mockito.when(info.getExecMemLimit()).thenReturn(Long.parseLong(createProperties.getOrDefault(
                CreateRoutineLoadInfo.EXEC_MEM_LIMIT_PROPERTY, "2147483648")));
        Mockito.when(info.getSendBatchParallelism()).thenReturn(Integer.parseInt(createProperties.getOrDefault(
                CreateRoutineLoadInfo.SEND_BATCH_PARALLELISM, "1")));
        Mockito.when(info.isLoadToSingleTablet()).thenReturn(Boolean.parseBoolean(createProperties.getOrDefault(
                CreateRoutineLoadInfo.LOAD_TO_SINGLE_TABLET, "false")));
        return info;
    }

    private static RoutineLoadJob newJob(LoadDataSourceType sourceType) {
        if (sourceType == LoadDataSourceType.KINESIS) {
            return new KinesisRoutineLoadJob(1L, "job", 1L, 1L, "us-east-1", "stream", UserIdentity.ADMIN);
        }
        return new KafkaRoutineLoadJob(1L, "job", 1L, 1L, "127.0.0.1:9092", "topic", UserIdentity.ADMIN);
    }

    private static void assertConfiguration(RoutineLoadJob job, double ratio, long memoryLimit,
            int batchParallelism, boolean singleTablet) {
        Assertions.assertEquals(ratio, job.maxFilterRatio);
        Assertions.assertEquals(memoryLimit, job.execMemLimit);
        Assertions.assertEquals(batchParallelism, job.sendBatchParallelism);
        Assertions.assertEquals(singleTablet, job.loadToSingleTablet);
    }
}
