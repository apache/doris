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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.filesystem.auth.GcpCredential;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TResultFileSinkOptions;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.Map;

public class OutFileTest extends TestWithFeService implements PlanPatternMatchSupported {
    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Test
    public void testWriteOutFile() throws Exception {
        String sql = "select * from T1 join T2 on T1.id = T2.id into outfile 'file://~/result.csv'\n"
                + " format as csv\n"
                + " properties (\n"
                + "    \"column_separator\" = \",\",\n"
                + "    \"line_delimiter\" = \"\\n\",\n"
                + "    \"broker.name\" = \"my_broker\",\n"
                + "    \"max_file_size\" = \"100MB\"\n"
                + ")";
        Assertions.assertTrue(getOutputFragment(sql).getExplainString(TExplainLevel.BRIEF)
                .contains("FILE SINK"));
    }

    @Test
    public void testHdfsOutFileCarriesDefaultFs() throws Exception {
        // The BE connects with the fs.defaultFS extracted from the outfile path; losing it makes
        // the BE-side hdfs client fail with "Expected authority at index 7: hdfs://".
        String sql = "select * from T1 into outfile 'hdfs://127.0.0.1:8020/tmp/outfile_test_'\n"
                + " format as csv\n"
                + " properties (\"hadoop.username\" = \"doris\")";
        PlanFragment fragment = getOutputFragment(sql);
        Assertions.assertTrue(fragment.getSink() instanceof ResultFileSink);
        Field field = ResultFileSink.class.getDeclaredField("fileSinkOptions");
        field.setAccessible(true);
        TResultFileSinkOptions sinkOptions = (TResultFileSinkOptions) field.get(fragment.getSink());
        Assertions.assertEquals("hdfs://127.0.0.1:8020/tmp/outfile_test_", sinkOptions.getFilePath());
        Assertions.assertEquals("hdfs://127.0.0.1:8020", sinkOptions.getBrokerProperties().get("fs.defaultFS"));
    }

    @Test
    public void testGcsOutFileNormalizesPathAndPreservesCredentials() throws Exception {
        for (String scheme : new String[] {"gs", "s3"}) {
            for (String provider : new String[] {"DEFAULT", "COMPUTE_ENGINE"}) {
                for (String serviceAccount : new String[] {"", "target@test-project.iam.gserviceaccount.com"}) {
                    String properties = "\"provider\" = \"GCP\","
                            + "\"s3.endpoint\" = \"https://storage.googleapis.com\","
                            + "\"s3.region\" = \"us-central1\","
                            + "\"" + GcpCredential.CREDENTIAL_PROVIDER_TYPE + "\" = \"" + provider + "\"";
                    if (!serviceAccount.isEmpty()) {
                        properties += ",\"" + GcpCredential.IMPERSONATION_SERVICE_ACCOUNT
                                + "\" = \"" + serviceAccount + "\"";
                    }
                    TResultFileSinkOptions options = getFileSinkOptions("select * from T1 into outfile '"
                            + scheme + "://outfile-bucket/nested/result_' format as csv properties ("
                            + properties + ")");
                    Assertions.assertEquals("s3://outfile-bucket/nested/result_", options.getFilePath());
                    Map<String, String> backendProperties = options.getBrokerProperties();
                    Assertions.assertEquals("GCP", backendProperties.get("provider"));
                    Assertions.assertEquals("https://storage.googleapis.com", backendProperties.get("AWS_ENDPOINT"));
                    // GCSProperties does not bind s3.region and retains its compatibility default.
                    Assertions.assertEquals("us-east1", backendProperties.get("AWS_REGION"));
                    Assertions.assertEquals(provider, backendProperties.get(GcpCredential.CREDENTIAL_PROVIDER_TYPE));
                    Assertions.assertEquals(serviceAccount,
                            backendProperties.getOrDefault(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT, ""));
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            "http, false", "https, false", "http, true", "https, true",
            "HTTP, false", "HTTPS, false"
    })
    public void testS3HttpOutFilePreservesPath(String scheme, boolean usePathStyle) throws Exception {
        String path = scheme + "://s3.us-east-1.amazonaws.com/outfile-bucket/nested/result_";
        TResultFileSinkOptions options = getFileSinkOptions("select * from T1 into outfile '"
                + path + "' format as csv properties ("
                + "\"s3.endpoint\" = \"https://s3.us-east-1.amazonaws.com\","
                + "\"s3.region\" = \"us-east-1\","
                + "\"s3.access_key\" = \"test-ak\",\"s3.secret_key\" = \"test-sk\","
                + "\"use_path_style\" = \"" + usePathStyle + "\")");
        Assertions.assertEquals(path, options.getFilePath());
    }

    @Test
    public void testHdfsOutFilePreservesPathWithoutAuthority() throws Exception {
        String path = "hdfs:/tmp/outfile_test_";
        TResultFileSinkOptions options = getFileSinkOptions("select * from T1 into outfile '"
                + path + "' format as csv properties (\"hadoop.username\" = \"doris\")");
        Assertions.assertEquals(path, options.getFilePath());
    }

    @Test
    public void testS3OutFilePreservesPath() throws Exception {
        TResultFileSinkOptions options = getFileSinkOptions("select * from T1 into outfile '"
                + "s3://outfile-bucket/nested/result_' format as csv properties ("
                + "\"s3.endpoint\" = \"https://s3.us-east-1.amazonaws.com\","
                + "\"s3.region\" = \"us-east-1\","
                + "\"s3.access_key\" = \"test-ak\",\"s3.secret_key\" = \"test-sk\")");
        Assertions.assertEquals("s3://outfile-bucket/nested/result_", options.getFilePath());
        Assertions.assertEquals("test-ak", options.getBrokerProperties().get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("test-sk", options.getBrokerProperties().get("AWS_SECRET_KEY"));
    }

    @ParameterizedTest
    @CsvSource({
            "oss://outfile-bucket, https://oss-cn-hangzhou.aliyuncs.com, cn-hangzhou",
            "oss://outfile-bucket.oss-cn-hangzhou.aliyuncs.com, https://oss-cn-hangzhou.aliyuncs.com, cn-hangzhou",
            "s3://outfile-bucket.oss-cn-hangzhou.aliyuncs.com, https://oss-cn-hangzhou.aliyuncs.com, cn-hangzhou",
            "cos://outfile-bucket, https://cos.ap-guangzhou.myqcloud.com, ap-guangzhou",
            "cosn://outfile-bucket, https://cos.ap-guangzhou.myqcloud.com, ap-guangzhou",
            "obs://outfile-bucket, https://obs.cn-north-4.myhuaweicloud.com, cn-north-4",
            "bos://outfile-bucket, https://s3.us-east-1.amazonaws.com, us-east-1",
            "s3a://outfile-bucket, https://s3.us-east-1.amazonaws.com, us-east-1",
            "s3n://outfile-bucket, https://s3.us-east-1.amazonaws.com, us-east-1"
    })
    public void testS3CompatibleOutFileNormalizesPath(String location, String endpoint, String region) throws Exception {
        TResultFileSinkOptions options = getFileSinkOptions("select * from T1 into outfile '"
                + location + "/nested/result_' format as csv properties ("
                + "\"s3.endpoint\" = \"" + endpoint + "\","
                + "\"s3.region\" = \"" + region + "\","
                + "\"s3.access_key\" = \"test-ak\",\"s3.secret_key\" = \"test-sk\")");
        Assertions.assertEquals("s3://outfile-bucket/nested/result_", options.getFilePath());
        Assertions.assertEquals(endpoint, options.getBrokerProperties().get("AWS_ENDPOINT"));
        Assertions.assertEquals(region, options.getBrokerProperties().get("AWS_REGION"));
        Assertions.assertEquals("test-ak", options.getBrokerProperties().get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("test-sk", options.getBrokerProperties().get("AWS_SECRET_KEY"));
    }

    private TResultFileSinkOptions getFileSinkOptions(String sql) throws Exception {
        PlanFragment fragment = getOutputFragment(sql);
        Assertions.assertTrue(fragment.getSink() instanceof ResultFileSink);
        Field field = ResultFileSink.class.getDeclaredField("fileSinkOptions");
        field.setAccessible(true);
        return (TResultFileSinkOptions) field.get(fragment.getSink());
    }

    private PlanFragment getOutputFragment(String sql) throws Exception {
        StatementScopeIdGenerator.clear();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        PhysicalPlan plan = planner.planWithLock(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        return new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext())).translatePlan(plan);
    }
}
