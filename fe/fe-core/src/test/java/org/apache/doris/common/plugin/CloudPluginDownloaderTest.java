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

package org.apache.doris.common.plugin;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.plugin.CloudPluginDownloader.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * Unit tests for CloudPluginDownloader using package-private methods for direct white-box testing.
 */
public class CloudPluginDownloaderTest {

    private Cloud.GetObjStoreInfoResponse mockResponse;
    private Cloud.ObjectStoreInfoPB mockObjInfo;
    private MetaServiceProxy mockMetaServiceProxy;

    @BeforeEach
    void setUp() {
        mockResponse = Mockito.mock(Cloud.GetObjStoreInfoResponse.class);
        mockObjInfo = Mockito.mock(Cloud.ObjectStoreInfoPB.class);
        mockMetaServiceProxy = Mockito.mock(MetaServiceProxy.class);
    }

    // ============== validateInput Tests ==============

    @Test
    void testValidateInput() {
        // Positive cases
        Assertions.assertDoesNotThrow(() -> {
            CloudPluginDownloader.validateInput(PluginType.JDBC_DRIVERS, "mysql.jar");
            CloudPluginDownloader.validateInput(PluginType.JAVA_UDF, "my_udf.jar");
        });

        // Empty/null name
        IllegalArgumentException ex1 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CloudPluginDownloader.validateInput(PluginType.JDBC_DRIVERS, ""));
        Assertions.assertEquals("Plugin name cannot be empty", ex1.getMessage());

        IllegalArgumentException ex2 = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CloudPluginDownloader.validateInput(PluginType.JDBC_DRIVERS, null));
        Assertions.assertEquals("Plugin name cannot be empty", ex2.getMessage());

        // Unsupported types
        UnsupportedOperationException ex3 = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> CloudPluginDownloader.validateInput(PluginType.CONNECTORS, "test.jar"));
        Assertions.assertTrue(ex3.getMessage().contains("is not supported yet"));
    }

    // ============== getCloudStorageInfo Tests ==============

    @Test
    void testGetCloudStorageInfo() throws Exception {
        try (MockedStatic<MetaServiceProxy> mockedStatic = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedStatic.when(MetaServiceProxy::getInstance).thenReturn(mockMetaServiceProxy);

            // Success case
            Cloud.MetaServiceResponseStatus okStatus = Cloud.MetaServiceResponseStatus.newBuilder()
                    .setCode(Cloud.MetaServiceCode.OK).build();
            Mockito.when(mockResponse.getStatus()).thenReturn(okStatus);
            Mockito.when(mockResponse.getObjInfoList()).thenReturn(Collections.singletonList(mockObjInfo));
            Mockito.when(mockResponse.getObjInfo(0)).thenReturn(mockObjInfo);
            Mockito.when(mockMetaServiceProxy.getObjStoreInfo(Mockito.any())).thenReturn(mockResponse);

            Cloud.ObjectStoreInfoPB result = CloudPluginDownloader.getCloudStorageInfo();
            Assertions.assertEquals(mockObjInfo, result);

            // Error response
            Cloud.MetaServiceResponseStatus failedStatus = Cloud.MetaServiceResponseStatus.newBuilder()
                    .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT).setMsg("Test error").build();
            Mockito.when(mockResponse.getStatus()).thenReturn(failedStatus);

            RuntimeException ex1 = Assertions.assertThrows(RuntimeException.class,
                    CloudPluginDownloader::getCloudStorageInfo);
            Assertions.assertTrue(ex1.getMessage().contains("Failed to get storage info"));

            // Empty storage list
            Mockito.when(mockResponse.getStatus()).thenReturn(okStatus);
            Mockito.when(mockResponse.getObjInfoList()).thenReturn(Collections.emptyList());

            RuntimeException ex2 = Assertions.assertThrows(RuntimeException.class,
                    CloudPluginDownloader::getCloudStorageInfo);
            Assertions.assertTrue(ex2.getMessage().contains("Only SaaS cloud storage is supported"));
        }
    }

    // ============== buildS3Path Tests ==============

    @Test
    void testBuildS3Path() {
        Mockito.when(mockObjInfo.getBucket()).thenReturn("test-bucket");

        // With prefix
        Mockito.when(mockObjInfo.hasPrefix()).thenReturn(true);
        Mockito.when(mockObjInfo.getPrefix()).thenReturn("test-prefix");
        Assertions.assertEquals("s3://test-bucket/test-prefix/plugins/jdbc_drivers/mysql.jar",
                CloudPluginDownloader.buildS3Path(mockObjInfo, PluginType.JDBC_DRIVERS, "mysql.jar"));

        // Without prefix
        Mockito.when(mockObjInfo.hasPrefix()).thenReturn(false);
        Assertions.assertEquals("s3://test-bucket/plugins/java_udf/my_udf.jar",
                CloudPluginDownloader.buildS3Path(mockObjInfo, PluginType.JAVA_UDF, "my_udf.jar"));

        // All plugin types
        Assertions.assertEquals("s3://test-bucket/plugins/connectors/test.jar",
                CloudPluginDownloader.buildS3Path(mockObjInfo, PluginType.CONNECTORS, "test.jar"));
        Assertions.assertEquals("s3://test-bucket/plugins/hadoop_conf/test.xml",
                CloudPluginDownloader.buildS3Path(mockObjInfo, PluginType.HADOOP_CONF, "test.xml"));
    }

    // ============== Integration Test ==============

    @Test
    void testDownloadFromCloudIntegration() {
        // Basic integration test - should fail early due to validation
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CloudPluginDownloader.downloadFromCloud(PluginType.JDBC_DRIVERS, "", "/tmp/test.jar"));
        Assertions.assertEquals("Plugin name cannot be empty", ex.getMessage());

        // Should fail at MetaService level (no real cloud environment)
        RuntimeException ex2 = Assertions.assertThrows(RuntimeException.class,
                () -> CloudPluginDownloader.downloadFromCloud(PluginType.JDBC_DRIVERS, "mysql.jar", "/tmp/test.jar"));
        Assertions.assertTrue(ex2.getMessage().contains("Failed to download plugin"));
    }

    @Test
    void testBuildS3PathEdgeCases() {
        // Test empty bucket (edge case)
        Mockito.when(mockObjInfo.getBucket()).thenReturn("");
        Mockito.when(mockObjInfo.hasPrefix()).thenReturn(false);
        String result = CloudPluginDownloader.buildS3Path(mockObjInfo, PluginType.JDBC_DRIVERS, "test.jar");
        Assertions.assertEquals("s3:///plugins/jdbc_drivers/test.jar", result);

        // Test special characters in name
        Mockito.when(mockObjInfo.getBucket()).thenReturn("test-bucket");
        String specialResult = CloudPluginDownloader.buildS3Path(mockObjInfo, PluginType.JAVA_UDF, "test-udf@v1.0.jar");
        Assertions.assertEquals("s3://test-bucket/plugins/java_udf/test-udf@v1.0.jar", specialResult);
    }

    // ============== Enum Tests ==============

    @Test
    void testPluginTypeEnum() {
        Assertions.assertEquals("JDBC_DRIVERS", PluginType.JDBC_DRIVERS.name());
        Assertions.assertEquals("JAVA_UDF", PluginType.JAVA_UDF.name());
        Assertions.assertEquals("CONNECTORS", PluginType.CONNECTORS.name());
        Assertions.assertEquals("HADOOP_CONF", PluginType.HADOOP_CONF.name());
        Assertions.assertEquals(4, PluginType.values().length);
    }
}
