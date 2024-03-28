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

package org.apache.doris.analysis;

import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.storage.MockRemote;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class StageTest extends TestWithFeService {

    private ConnectContext ctx;
    private static final String OBJ_INFO =  "PROPERTIES (\"bucket\" = \"tmp_bucket\", "
            + "\"endpoint\" = \"cos.ap-beijing.myqcloud.com\", "
            + "\"provider\" = \"cos\", "
            + "\"prefix\" = \"tmp_prefix\", "
            + "\"sk\" = \"tmp_sk\", "
            + "\"ak\" = \"tmp_ak\", "
            + "\"access_type\" = \"aksk\", "
            + "\"region\" = \"ap-beijing\"";

    private static final String CREATE_STAGE_SQL = "create stage if not exists ex_stage_1 " + OBJ_INFO;

    @Mocked
    RemoteBase remote = new MockRemote(new ObjectInfo(Provider.COS, "", "", "", "", "", ""));

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testAnalyzeStorageProperties() throws Exception {
        // test bucket not set
        String sql = "create stage ex_stage_1 "
                + "PROPERTIES ('endpoint' = 'cos.ap-beijing.myqcloud.com', "
                + "'region' = 'ap-beijing', "
                + "'prefix' = 'tmp_prefix', "
                + "'ak'='tmp_ak', 'sk'='tmp_sk', 'access_type'='aksk');";
        parseAndAnalyzeWithException(sql, "bucket is required for ExternalStage");

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
        for (Entry<String, String> entry : prefixMap.entrySet()) {
            sql = "create stage ex_stage_1 PROPERTIES "
                    + "('endpoint' = 'cos.ap-beijing.myqcloud.com', "
                    + "'bucket' = 'test_bucket', "
                    + "'region' = 'ap-beijing', " + "'ak'='tmp_ak', 'sk'='tmp_sk', 'access_type'='aksk', "
                    + (entry.getKey() == null ? "" : ("'prefix'='" + entry.getKey()) + "', ")
                    + "'provider'='oss');";
            CreateStageStmt stmt = parseAndAnalyze(sql);
            StageProperties stageProperties = stmt.getStageProperties();
            Assert.assertEquals(entry.getValue(), stageProperties.getProperties().get(StageProperties.PREFIX));
            Assert.assertEquals(entry.getValue(), stageProperties.getObjectStoreInfoPB().getPrefix());
        }

        // test invalid provider
        sql = "create stage ex_stage_1 "
                + "properties ('endpoint' = 'cos.ap-beijing.myqcloud.com', "
                + "'region' = 'ap-beijing', "
                + "'bucket' = 'tmp_bucket', "
                + "'prefix' = 'tmp_prefix', "
                + "'provider' = 'abc', "
                + "'ak'='tmp_ak', 'sk'='tmp_sk', 'access_type'='aksk');";
        parseAndAnalyzeWithException(sql, "Property provider with invalid value abc");

        // test getObjectInfoPB
        sql = "create stage if not exists ex_stage_1 " + OBJ_INFO + ")";
        CreateStageStmt stmt = parseAndAnalyze(sql);
        ObjectStoreInfoPB objectStoreInfoPB = stmt.getStageProperties().getObjectStoreInfoPB();
        Assert.assertEquals("cos.ap-beijing.myqcloud.com", objectStoreInfoPB.getEndpoint());
        Assert.assertEquals("ap-beijing", objectStoreInfoPB.getRegion());
        Assert.assertEquals("tmp_bucket", objectStoreInfoPB.getBucket());
        Assert.assertEquals("tmp_prefix", objectStoreInfoPB.getPrefix());
        Assert.assertEquals("tmp_ak", objectStoreInfoPB.getAk());
        Assert.assertEquals("tmp_sk", objectStoreInfoPB.getSk());
        Assert.assertEquals(Provider.COS, objectStoreInfoPB.getProvider());
    }

    @Test
    public void testAnalyzeTypeAndCompression() throws Exception {
        // create stage with type
        String sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'json')";
        Assert.assertEquals("json", parseAndAnalyze(sql).getStageProperties().getFileType());

        // TODO create stage with type with invalid type
        sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'js')";
        Assert.assertEquals("js", parseAndAnalyze(sql).getStageProperties().getFileType());

        // create stage with compression
        sql = CREATE_STAGE_SQL + ", 'default.file.compression' = 'gz')";
        Assert.assertEquals(null, parseAndAnalyze(sql).getStageProperties().getFileType());

        // TODO create stage with invalid compression
        sql = CREATE_STAGE_SQL + ", 'default.file.compression' = 'gza')";
        Assert.assertEquals(null, parseAndAnalyze(sql).getStageProperties().getFileType());

        // create stage with type and compression
        sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'csv', 'default.file.compression'='gz')";
        Assert.assertEquals("csv", parseAndAnalyze(sql).getStageProperties().getFileType());

        // create stage with invalid type and compression
        sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'orc', 'default.file.compression'='gz')";
        parseAndAnalyzeWithException(sql, "Compression only support CSV or JSON file type, but input type is orc");
    }

    @Test
    public void testAnalyzeSizeLimit() throws Exception {
        // create stage without size limit
        String sql = CREATE_STAGE_SQL + ")";
        Assert.assertEquals(0, parseAndAnalyze(sql).getStageProperties().getSizeLimit());

        // create stage with size limit
        sql = CREATE_STAGE_SQL + ", 'default.copy.size_limit' = '100')";
        Assert.assertEquals(100, parseAndAnalyze(sql).getStageProperties().getSizeLimit());

        // create stage with invalid size limit
        sql = CREATE_STAGE_SQL + ", 'default.copy.size_limit' = '100a')";
        parseAndAnalyzeWithException(sql, "Property default.copy.size_limit with invalid value 100a");
    }

    @Test
    public void testAnalyzeOnError() throws Exception {
        // create stage without on_error
        String sql = CREATE_STAGE_SQL + ")";
        Assert.assertEquals(0, parseAndAnalyze(sql).getStageProperties().getMaxFilterRatio(), 0.02);

        // create stage with on_error value is continue
        sql = CREATE_STAGE_SQL + ", 'default.copy.on_error' = 'continue')";
        Assert.assertEquals(1, parseAndAnalyze(sql).getStageProperties().getMaxFilterRatio(), 0.02);

        // create stage with on_error value is abort_statement
        sql = CREATE_STAGE_SQL + ", 'default.copy.on_error' = 'abort_statement')";
        Assert.assertEquals(0, parseAndAnalyze(sql).getStageProperties().getMaxFilterRatio(), 0.02);

        // create stage with on_error value is max filter ratio
        sql = CREATE_STAGE_SQL + ", 'default.copy.on_error' = 'max_filter_ratio_0.4')";
        Assert.assertEquals(0.4, parseAndAnalyze(sql).getStageProperties().getMaxFilterRatio(), 0.02);

        // create stage with on_error value is invalid max filter ratio
        sql = CREATE_STAGE_SQL + ", 'default.copy.on_error' = 'max_filter_ratio_0.a')";
        parseAndAnalyzeWithException(sql, "Property default.copy.on_error with invalid value max_filter_ratio_0.a");
    }

    @Test
    public void testAnalyzeAsync() throws Exception {
        // create stage without async
        String sql = CREATE_STAGE_SQL + ")";
        Assert.assertEquals(true, parseAndAnalyze(sql).getStageProperties().isAsync());

        // create stage with async
        sql = CREATE_STAGE_SQL + ", 'default.copy.async'='false')";
        Assert.assertEquals(false, parseAndAnalyze(sql).getStageProperties().isAsync());

        // create stage with invalid async
        sql = CREATE_STAGE_SQL + ", 'default.copy.async'='abc')";
        parseAndAnalyzeWithException(sql, "Property default.copy.async with invalid value abc");
    }

    @Test
    public void testAnalyzeStrictMode() throws Exception {
        // create stage with strict mode
        String sql = CREATE_STAGE_SQL + ", 'default.copy.strict_mode'='true')";
        Assert.assertEquals(true, Boolean.parseBoolean(
                parseAndAnalyze(sql).getStageProperties().getDefaultPropertiesWithoutPrefix().get("copy.strict_mode")));

        // create stage with invalid strict mode
        sql = CREATE_STAGE_SQL + ", 'default.copy.strict_mode'='def')";
        parseAndAnalyzeWithException(sql, "Property default.copy.strict_mode with invalid value def");
    }

    @Test
    public void testAnalyzeLoadParallelism() throws Exception {
        // create stage with load parallelism
        String sql = CREATE_STAGE_SQL + ", 'default.copy.load_parallelism'='2')";
        Assert.assertEquals(2, Integer.parseInt(
                parseAndAnalyze(sql).getStageProperties().getDefaultPropertiesWithoutPrefix()
                        .get("copy.load_parallelism")));

        // create stage with invalid load parallelism
        sql = CREATE_STAGE_SQL + ", 'default.copy.load_parallelism'='def')";
        parseAndAnalyzeWithException(sql, "Property default.copy.load_parallelism with invalid value def");
    }

    @Test
    public void testOtherProperties() throws Exception {
        String sql = CREATE_STAGE_SQL + ", 'default.file.column_separator'=\",\", "
                + "'default.file.line_delimiter'=\"\n\")";
        CreateStageStmt stmt = parseAndAnalyze(sql);
        Assert.assertEquals(",", stmt.getStageProperties().getColumnSeparator());
        Assert.assertEquals("\n", stmt.getStageProperties().getProperties().get("default.file.line_delimiter"));
        Assert.assertEquals("\n", stmt.getStageProperties().getDefaultProperties().get("default.file.line_delimiter"));
        Assert.assertEquals("\n",
                stmt.getStageProperties().getDefaultPropertiesWithoutPrefix().get("file.line_delimiter"));

        // without property
        sql = "create stage in_stage_1";
        parseAndAnalyzeWithException(sql, "Syntax error");

        // with an unknown property
        sql = CREATE_STAGE_SQL + ", 'default.file.type' = 'csv', 'test_key'='test_value')";
        parseAndAnalyzeWithException(sql, "'test_key' is invalid");
    }

    @Test
    public void testToSql() throws Exception {
        String sql = "create stage ex_stage_1 properties (\"bucket\" = \"tmp_bucket\", "
                + "\"default.copy.size_limit\" = \"100\", "
                + "\"endpoint\" = \"cos.ap-beijing.myqcloud.com\", \"access_type\" = \"aksk\", "
                + "\"default.file.type\" = \"csv\", "
                + "\"provider\" = \"cos\", "
                + "\"prefix\" = \"tmp_prefix\", "
                + "\"default.file.column_separator\" = \",\", "
                + "\"sk\" = \"tmp_sk\", \"ak\" = \"tmp_ak\", \"region\" = \"ap-beijing\")";
        StatementBase statementBase = parseAndAnalyze(sql);
        System.out.println("expectedSql: " + sql.toLowerCase());
        System.out.println("realSql    : " + statementBase.toSql().toLowerCase().trim());
        Assert.assertEquals(sql.toLowerCase(), statementBase.toSql().toLowerCase().trim());
    }

    @Test
    public void testGetProperties() throws Exception {
        String sql = CREATE_STAGE_SQL
                + ", 'default.file.type' = 'csv', 'default.file.column_separator'=\",\" "
                + ", 'default.copy.on_error' = 'abort_statement', 'default.copy.size_limit' = '100')";
        CreateStageStmt createStageStmt = parseAndAnalyze(sql);
        Assert.assertEquals(12, createStageStmt.getStageProperties().getProperties().size());
        // check default properties
        do {
            Map<String, String> properties = createStageStmt.getStageProperties().getDefaultProperties();
            Assert.assertEquals(4, properties.size());
            Map<String, String> expectedProperties = new HashMap<>();
            expectedProperties.put("default.file.type", "csv");
            expectedProperties.put("default.file.column_separator", ",");
            expectedProperties.put("default.copy.on_error", "abort_statement");
            expectedProperties.put("default.copy.size_limit", "100");
            for (Entry<String, String> entry : expectedProperties.entrySet()) {
                Assert.assertTrue(properties.containsKey(entry.getKey()));
                Assert.assertEquals(entry.getValue(), properties.get(entry.getKey()));
            }
        } while (false);
        // check default properties without prefix
        do {
            Map<String, String> properties = createStageStmt.getStageProperties().getDefaultPropertiesWithoutPrefix();
            Assert.assertEquals(4, properties.size());
            Map<String, String> expectedProperties = new HashMap<>();
            expectedProperties.put("file.type", "csv");
            expectedProperties.put("file.column_separator", ",");
            expectedProperties.put("copy.on_error", "abort_statement");
            expectedProperties.put("copy.size_limit", "100");
            Assert.assertEquals(4, properties.size());
            for (Entry<String, String> entry : expectedProperties.entrySet()) {
                Assert.assertTrue(properties.containsKey(entry.getKey()));
                Assert.assertEquals(entry.getValue(), properties.get(entry.getKey()));
            }
        } while (false);
    }

    @Test
    public void testStagePB() throws Exception {
        String sql = CREATE_STAGE_SQL
                + ", 'default.file.type' = 'csv', 'default.file.column_separator'=\",\" "
                + ", 'default.copy.on_error' = 'abort_statement', 'default.copy.size_limit' = '100')";
        CreateStageStmt createStageStmt = parseAndAnalyze(sql);
        StagePB stagePB = createStageStmt.toStageProto();
        Assert.assertEquals(StageType.EXTERNAL, stagePB.getType());
        Assert.assertEquals("ex_stage_1", stagePB.getName());
        Assert.assertTrue(StringUtils.isNotBlank(stagePB.getStageId()));
        ObjectStoreInfoPB objInfo = stagePB.getObjInfo();
        Assert.assertEquals("cos.ap-beijing.myqcloud.com", objInfo.getEndpoint());
        Map<String, String> propertiesMap = stagePB.getPropertiesMap();
        Assert.assertEquals(4, propertiesMap.size());
        Assert.assertEquals("csv", propertiesMap.get("default.file.type"));
    }

    private void parseAndAnalyzeWithException(String sql, String errorMsg) {
        do {
            try {
                UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains(errorMsg));
                break;
            } catch (Exception e) {
                Assert.fail("must be AnalysisException.");
            }
            Assert.fail("must be AnalysisException.");
        } while (false);
    }

    private CreateStageStmt parseAndAnalyze(String sql) throws Exception {
        try {
            CreateStageStmt stmt = (CreateStageStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
            return stmt;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("must be success.");
            throw e;
        }
    }
}
