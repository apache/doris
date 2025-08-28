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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StageProperties;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class CreateStageCommandTest extends TestWithFeService {
    private static final String OBJ_INFO =
            "PROPERTIES (\"bucket\" = \"tmp-bucket\", "
            + "\"endpoint\" = \"cos.ap-beijing.myqcloud.com\", "
            + "\"provider\" = \"cos\", "
            + "\"prefix\" = \"tmp_prefix\", "
            + "\"sk\" = \"tmp_sk\", "
            + "\"ak\" = \"tmp_ak\", "
            + "\"access_type\" = \"aksk\", "
            + "\"region\" = \"ap-beijing\"";

    private static final String CREATE_STAGE_SQL = "create stage if not exists ex_stage_1 " + OBJ_INFO;

    @Test
    public void testAnalyzeStorageProperties() {
        // test bucket not set
        String sql = "create stage ex_stage_1 "
                + "PROPERTIES ('endpoint' = 'cos.ap-beijing.myqcloud.com', "
                + "'region' = 'ap-beijing', "
                + "'prefix' = 'tmp_prefix', "
                + "'ak'='tmp_ak', 'sk'='tmp_sk', 'access_type'='aksk');";

        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());

        // test prefix
        Map<String, String> prefixMap = new HashMap<>();
        prefixMap.put("", "");
        prefixMap.put("/", "");
        prefixMap.put("//", "");
        prefixMap.put(null, "");
        prefixMap.put("/abc/def/", "abc/def");
        prefixMap.put("/abc/def", "abc/def");
        prefixMap.put("abc/def/", "abc/def");
        prefixMap.put("abc/def", "abc/def");
        for (Map.Entry<String, String> entry : prefixMap.entrySet()) {
            sql = "create stage ex_stage_1 PROPERTIES "
                + "('endpoint' = 'cos.ap-beijing.myqcloud.com', "
                + "'bucket' = 'test-bucket', "
                + "'region' = 'ap-beijing', " + "'ak'='tmp_ak', 'sk'='tmp_sk', 'access_type'='aksk', "
                + "'prefix'='" + entry.getValue() + "', "
                + "'provider'='oss');";

            CreateStageCommand command1 = getCreateStageCommand(sql);
            StageProperties stageProperties = command1.getStageProperties();

            Assertions.assertEquals(entry.getValue(), stageProperties.getProperties().get(StageProperties.PREFIX));
            Assertions.assertEquals(entry.getValue(), stageProperties.getObjectStoreInfoPB().getPrefix());
        }

        // test invalid provider
        sql = "create stage ex_stage_1 "
            + "properties ('endpoint' = 'cos.ap-beijing.myqcloud.com', "
            + "'region' = 'ap-beijing', "
            + "'bucket' = 'tmp-bucket', "
            + "'prefix' = 'tmp_prefix', "
            + "'provider' = 'abc', "
            + "'ak'='tmp_ak', 'sk'='tmp_sk', 'access_type'='aksk');";
        // S3 Provider will be converted to upper case.
        CreateStageCommand command2 = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate());

        // test getObjectInfoPB
        sql = "create stage if not exists ex_stage_1 " + OBJ_INFO + ")";
        CreateStageCommand command3 = getCreateStageCommand(sql);
        ObjectStoreInfoPB objectStoreInfoPB = command3.getStageProperties().getObjectStoreInfoPB();

        Assertions.assertEquals("cos.ap-beijing.myqcloud.com", objectStoreInfoPB.getEndpoint());
        Assertions.assertEquals("ap-beijing", objectStoreInfoPB.getRegion());
        Assertions.assertEquals("tmp-bucket", objectStoreInfoPB.getBucket());
        Assertions.assertEquals("tmp_prefix", objectStoreInfoPB.getPrefix());
        Assertions.assertEquals("tmp_ak", objectStoreInfoPB.getAk());
        Assertions.assertEquals("tmp_sk", objectStoreInfoPB.getSk());
        Assertions.assertEquals(Provider.COS, objectStoreInfoPB.getProvider());
    }

    @Test
    public void testAnalyzeTypeAndCompression() {
        // create stage with type
        String sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'json')";
        Assertions.assertEquals("json", getCreateStageCommand(sql).getStageProperties().getFileType());

        // TODO create stage with type with invalid type
        sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'js')";
        Assertions.assertEquals("js", getCreateStageCommand(sql).getStageProperties().getFileType());

        // create stage with compression
        sql = CREATE_STAGE_SQL + ", 'default.file.compression' = 'gz')";
        Assertions.assertEquals(null, getCreateStageCommand(sql).getStageProperties().getFileType());

        // TODO create stage with invalid compression
        sql = CREATE_STAGE_SQL + ", 'default.file.compression' = 'gza')";
        Assertions.assertEquals(null, getCreateStageCommand(sql).getStageProperties().getFileType());

        // create stage with type and compression
        sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'csv', 'default.file.compression'='gz')";
        Assertions.assertEquals("csv", getCreateStageCommand(sql).getStageProperties().getFileType());

        // create stage with invalid type and compression
        sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'orc', 'default.file.compression'='gz')";
        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
    }

    @Test
    public void testAnalyzeSizeLimit() throws Exception {
        // create stage without size limit
        String sql = CREATE_STAGE_SQL + ")";
        Assertions.assertEquals(0, getCreateStageCommand(sql).getStageProperties().getSizeLimit());

        // create stage with size limit
        sql = CREATE_STAGE_SQL + ", 'default.copy.size_limit' = '100')";
        Assertions.assertEquals(100, getCreateStageCommand(sql).getStageProperties().getSizeLimit());

        // create stage with invalid size limit
        sql = CREATE_STAGE_SQL + ", 'default.copy.size_limit' = '100a')";
        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
    }

    @Test
    public void testAnalyzeOnError() throws Exception {
        // create stage without on_error
        String sql = CREATE_STAGE_SQL + ")";
        Assertions.assertEquals(0, getCreateStageCommand(sql).getStageProperties().getMaxFilterRatio(), 0.02);

        // create stage with on_error value is continue
        sql = CREATE_STAGE_SQL + ", 'default.copy.on_error' = 'continue')";
        Assertions.assertEquals(1, getCreateStageCommand(sql).getStageProperties().getMaxFilterRatio(), 0.02);

        // create stage with on_error value is abort_statement
        sql = CREATE_STAGE_SQL + ", 'default.copy.on_error' = 'abort_statement')";
        Assertions.assertEquals(0, getCreateStageCommand(sql).getStageProperties().getMaxFilterRatio(), 0.02);

        // create stage with on_error value is max filter ratio
        sql = CREATE_STAGE_SQL + ", 'default.copy.on_error' = 'max_filter_ratio_0.4')";
        Assertions.assertEquals(0.4, getCreateStageCommand(sql).getStageProperties().getMaxFilterRatio(), 0.02);

        // create stage with on_error value is invalid max filter ratio
        sql = CREATE_STAGE_SQL + ", 'default.copy.on_error' = 'max_filter_ratio_0.a')";
        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
    }

    @Test
    public void testAnalyzeAsync() {
        // create stage without async
        String sql = CREATE_STAGE_SQL + ")";
        Assertions.assertEquals(true, getCreateStageCommand(sql).getStageProperties().isAsync());

        // create stage with async
        sql = CREATE_STAGE_SQL + ", 'default.copy.async'='false')";
        Assertions.assertEquals(false, getCreateStageCommand(sql).getStageProperties().isAsync());

        // create stage with invalid async
        sql = CREATE_STAGE_SQL + ", 'default.copy.async'='abc')";
        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
    }

    @Test
    public void testAnalyzeStrictMode() {
        // create stage with strict mode
        String sql = CREATE_STAGE_SQL + ", 'default.copy.strict_mode'='true')";
        Assertions.assertEquals(true, Boolean.parseBoolean(
                getCreateStageCommand(sql).getStageProperties().getDefaultPropertiesWithoutPrefix().get("copy.strict_mode")));

        // create stage with invalid strict mode
        sql = CREATE_STAGE_SQL + ", 'default.copy.strict_mode'='def')";
        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
    }

    @Test
    public void testAnalyzeLoadParallelism() {
        // create stage with load parallelism
        String sql = CREATE_STAGE_SQL + ", 'default.copy.load_parallelism'='2')";
        Assertions.assertEquals(2, Integer.parseInt(
                getCreateStageCommand(sql).getStageProperties().getDefaultPropertiesWithoutPrefix()
                    .get("copy.load_parallelism")));

        // create stage with invalid load parallelism
        sql = CREATE_STAGE_SQL + ", 'default.copy.load_parallelism'='def')";
        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
    }

    @Test
    public void testOtherProperties() {
        String sql = CREATE_STAGE_SQL + ", 'default.file.column_separator'=\",\", "
                + "'default.file.line_delimiter'=\"\n\")";
        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertEquals(",", command.getStageProperties().getColumnSeparator());
        Assertions.assertEquals("\n", command.getStageProperties().getProperties().get("default.file.line_delimiter"));
        Assertions.assertEquals("\n", command.getStageProperties().getDefaultProperties().get("default.file.line_delimiter"));
        Assertions.assertEquals("\n",
                command.getStageProperties().getDefaultPropertiesWithoutPrefix().get("file.line_delimiter"));

        // without property
        sql = "create stage in_stage_1";
        CreateStageCommand command1 = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command1.validate());

        // with an unknown property
        sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'csv', 'test_key'='test_value')";
        CreateStageCommand command2 = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate());
    }

    @Test
    public void testGetProperties() {
        String sql = CREATE_STAGE_SQL
                + ", 'default.file.type' = 'csv', 'default.file.column_separator'=\",\" "
                + ", 'default.copy.on_error' = 'abort_statement', 'default.copy.size_limit' = '100')";
        CreateStageCommand command = getCreateStageCommand(sql);
        Assertions.assertEquals(12, command.getStageProperties().getProperties().size());
        // check default properties
        do {
            Map<String, String> properties = command.getStageProperties().getDefaultProperties();
            Assertions.assertEquals(4, properties.size());
            Map<String, String> expectedProperties = new HashMap<>();
            expectedProperties.put("default.file.type", "csv");
            expectedProperties.put("default.file.column_separator", ",");
            expectedProperties.put("default.copy.on_error", "abort_statement");
            expectedProperties.put("default.copy.size_limit", "100");
            for (Map.Entry<String, String> entry : expectedProperties.entrySet()) {
                Assertions.assertTrue(properties.containsKey(entry.getKey()));
                Assertions.assertEquals(entry.getValue(), properties.get(entry.getKey()));
            }
        } while (false);
        // check default properties without prefix
        do {
            Map<String, String> properties = command.getStageProperties().getDefaultPropertiesWithoutPrefix();
            Assertions.assertEquals(4, properties.size());
            Map<String, String> expectedProperties = new HashMap<>();
            expectedProperties.put("file.type", "csv");
            expectedProperties.put("file.column_separator", ",");
            expectedProperties.put("copy.on_error", "abort_statement");
            expectedProperties.put("copy.size_limit", "100");
            Assertions.assertEquals(4, properties.size());
            for (Map.Entry<String, String> entry : expectedProperties.entrySet()) {
                Assertions.assertTrue(properties.containsKey(entry.getKey()));
                Assertions.assertEquals(entry.getValue(), properties.get(entry.getKey()));
            }
        } while (false);
    }

    @Test
    public void testStagePB() throws Exception {
        String sql = CREATE_STAGE_SQL
                + ", 'default.file.type' = 'csv', 'default.file.column_separator'=\",\" "
                + ", 'default.copy.on_error' = 'abort_statement', 'default.copy.size_limit' = '100')";
        CreateStageCommand command = getCreateStageCommand(sql);
        StagePB stagePB = command.toStageProto();
        Assertions.assertEquals(StageType.EXTERNAL, stagePB.getType());
        Assertions.assertEquals("ex_stage_1", stagePB.getName());
        Assertions.assertTrue(StringUtils.isNotBlank(stagePB.getStageId()));
        ObjectStoreInfoPB objInfo = stagePB.getObjInfo();
        Assertions.assertEquals("cos.ap-beijing.myqcloud.com", objInfo.getEndpoint());
        Map<String, String> propertiesMap = stagePB.getPropertiesMap();
        Assertions.assertEquals(4, propertiesMap.size());
        Assertions.assertEquals("csv", propertiesMap.get("default.file.type"));
    }

    @Test
    public void testCreateStageTrimPropertyKey() throws Exception {
        String sql = CREATE_STAGE_SQL + ", ' default.file.type' = 'csv', ' default.file.compression '='gz')";
        CreateStageCommand command1 = getCreateStageCommand(sql);
        Assertions.assertEquals("csv", command1.getStageProperties().getFileType());
        Assertions.assertEquals("gz", command1.getStageProperties().getCompression());

        sql = CREATE_STAGE_SQL + ", 'default.file. type' = 'orc')";
        CreateStageCommand command2 = getCreateStageCommand(sql);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate());
    }

    private CreateStageCommand getCreateStageCommand(String sql) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateStageCommand);
        return (CreateStageCommand) plan;
    }
}
