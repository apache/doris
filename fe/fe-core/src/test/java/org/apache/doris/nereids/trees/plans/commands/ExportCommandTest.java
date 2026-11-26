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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.property.fileformat.ParquetFileFormatProperties;
import org.apache.doris.load.ExportJob;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExportCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("export_test");
        connectContext.setDatabase("export_test");
        createTable("CREATE TABLE T1 (id bigint) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")");
    }

    @ParameterizedTest
    @CsvSource({
            "s3://export-bucket, https://s3.us-east-1.amazonaws.com, us-east-1",
            "oss://export-bucket, https://oss-cn-hangzhou.aliyuncs.com, cn-hangzhou",
            "oss://export-bucket.oss-cn-hangzhou.aliyuncs.com, https://oss-cn-hangzhou.aliyuncs.com, cn-hangzhou",
            "s3://export-bucket.oss-cn-hangzhou.aliyuncs.com, https://oss-cn-hangzhou.aliyuncs.com, cn-hangzhou",
            "cos://export-bucket, https://cos.ap-guangzhou.myqcloud.com, ap-guangzhou",
            "cosn://export-bucket, https://cos.ap-guangzhou.myqcloud.com, ap-guangzhou",
            "obs://export-bucket, https://obs.cn-north-4.myhuaweicloud.com, cn-north-4",
            "bos://export-bucket, https://s3.us-east-1.amazonaws.com, us-east-1",
            "s3a://export-bucket, https://s3.us-east-1.amazonaws.com, us-east-1",
            "s3n://export-bucket, https://s3.us-east-1.amazonaws.com, us-east-1"
    })
    public void testS3CompatibleExportNormalizesPath(String location, String endpoint, String region) throws Exception {
        BrokerDesc broker = new BrokerDesc(null, s3Properties(endpoint, region));
        ExportJob job = generateExportJob(location + "/nested/result_", broker);
        Assertions.assertEquals("s3://export-bucket/nested/result_", job.getExportPath());
        Map<String, String> backendProperties = job.getBrokerDesc().getBackendConfigProperties();
        Assertions.assertEquals(endpoint, backendProperties.get("AWS_ENDPOINT"));
        Assertions.assertEquals(region, backendProperties.get("AWS_REGION"));
        Assertions.assertEquals("test-ak", backendProperties.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("test-sk", backendProperties.get("AWS_SECRET_KEY"));
    }

    @Test
    public void testGcsExportNormalizesPathAndPreservesCredentials() throws Exception {
        for (String scheme : new String[] {"gs", "s3"}) {
            for (String provider : new String[] {"DEFAULT", "COMPUTE_ENGINE"}) {
                for (String serviceAccount : new String[] {"", "target@test-project.iam.gserviceaccount.com"}) {
                    Map<String, String> properties = new HashMap<>();
                    properties.put("provider", "GCP");
                    properties.put("s3.endpoint", "https://storage.googleapis.com");
                    properties.put("gs.credential_provider_type", provider);
                    if (!serviceAccount.isEmpty()) {
                        properties.put("gs.impersonation_service_account", serviceAccount);
                    }
                    ExportJob job = generateExportJob(scheme + "://export-bucket/nested/result_",
                            new BrokerDesc(null, properties));
                    Assertions.assertEquals("s3://export-bucket/nested/result_", job.getExportPath());
                    Map<String, String> backendProperties = job.getBrokerDesc().getBackendConfigProperties();
                    Assertions.assertEquals("GCP", backendProperties.get("provider"));
                    Assertions.assertEquals("https://storage.googleapis.com", backendProperties.get("AWS_ENDPOINT"));
                    Assertions.assertEquals(provider, backendProperties.get("gs.credential_provider_type"));
                    Assertions.assertEquals(serviceAccount,
                            backendProperties.getOrDefault("gs.impersonation_service_account", ""));
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            "http, false", "https, false", "http, true", "https, true",
            "HTTP, false", "HTTPS, false"
    })
    public void testHttpExportPreservesPath(String scheme, boolean usePathStyle) throws Exception {
        Map<String, String> properties = s3Properties("https://s3.us-east-1.amazonaws.com", "us-east-1");
        properties.put("use_path_style", Boolean.toString(usePathStyle));
        String path = scheme + "://s3.us-east-1.amazonaws.com/export-bucket/nested/result_";
        Assertions.assertEquals(path, generateExportJob(path, new BrokerDesc(null, properties)).getExportPath());
    }

    @Test
    public void testNonS3ExportPreservesPath() throws Exception {
        String hdfsPath = "hdfs:/tmp/export_test_";
        Assertions.assertEquals(hdfsPath, generateExportJob(hdfsPath,
                new BrokerDesc(null, ImmutableMap.of("hadoop.username", "doris"))).getExportPath());
        String brokerPath = "oss://export-bucket/nested/result_";
        Assertions.assertEquals(brokerPath, generateExportJob(brokerPath,
                new BrokerDesc("broker", StorageType.BROKER, ImmutableMap.of())).getExportPath());
        String localPath = "file:///tmp/export_test_";
        Assertions.assertEquals(localPath, generateExportJob(localPath,
                new BrokerDesc("local", StorageType.LOCAL, null)).getExportPath());
    }

    private Map<String, String> s3Properties(String endpoint, String region) {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", endpoint);
        properties.put("s3.region", region);
        properties.put("s3.access_key", "test-ak");
        properties.put("s3.secret_key", "test-sk");
        return properties;
    }

    private ExportJob generateExportJob(String path, BrokerDesc broker) throws Exception {
        ExportCommand command = new ExportCommand(ImmutableList.of("T1"), ImmutableList.of(), Optional.empty(),
                path, ImmutableMap.of(), Optional.of(broker));
        // Inspect the job before registration so the test never starts an export task.
        Method method = ExportCommand.class.getDeclaredMethod("generateExportJob",
                ConnectContext.class, Map.class, TableNameInfo.class);
        method.setAccessible(true);
        return (ExportJob) method.invoke(command, connectContext, ImmutableMap.of(),
                new TableNameInfo("internal", "export_test", "T1"));
    }

    @Test
    void testEnableInt96TimestampsProperty() {
        Map<String, String> properties = ImmutableMap.of(
                ParquetFileFormatProperties.ENABLE_INT96_TIMESTAMPS, "false");
        ExportCommand exportCommand = new ExportCommand(
                Collections.singletonList("test_table"), Collections.emptyList(), Optional.empty(),
                "file:///tmp/export", properties, Optional.empty());

        Assertions.assertDoesNotThrow(
                () -> Deencapsulation.invoke(exportCommand, "checkPropertyKey", properties));
    }
}
