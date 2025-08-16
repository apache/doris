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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CloudPluginDownloaderTest {

    @Test
    public void testPluginTypeDirectoryNames() {
        // Test PluginType directory name mapping
        Assertions.assertEquals("jdbc_drivers", CloudPluginDownloader.PluginType.JDBC_DRIVERS.getDirectoryName());
        Assertions.assertEquals("java_udf", CloudPluginDownloader.PluginType.JAVA_UDF.getDirectoryName());
        Assertions.assertEquals("connectors", CloudPluginDownloader.PluginType.CONNECTORS.getDirectoryName());
        Assertions.assertEquals("hadoop_conf", CloudPluginDownloader.PluginType.HADOOP_CONF.getDirectoryName());
    }

    @Test
    public void testPluginTypeEnumValues() {
        // Test PluginType enum values exist
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.JAVA_UDF);
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.JDBC_DRIVERS);
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.CONNECTORS);
        Assertions.assertNotNull(CloudPluginDownloader.PluginType.HADOOP_CONF);

        // Test enum name method
        Assertions.assertEquals("JAVA_UDF", CloudPluginDownloader.PluginType.JAVA_UDF.name());
        Assertions.assertEquals("JDBC_DRIVERS", CloudPluginDownloader.PluginType.JDBC_DRIVERS.name());
    }

    @Test
    public void testDownloadFromCloudParameterValidation() {
        // Test with null plugin name - should throw RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF,
                    null, "/local/path");
        });

        // Test with empty plugin name - should throw RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.JAVA_UDF,
                    "", "/local/path");
        });

        // Test with unsupported plugin type - should throw RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.CONNECTORS,
                    "test.jar", "/local/path");
        });

        Assertions.assertThrows(RuntimeException.class, () -> {
            CloudPluginDownloader.downloadFromCloud(
                    CloudPluginDownloader.PluginType.HADOOP_CONF,
                    "test.jar", "/local/path");
        });
    }
}
