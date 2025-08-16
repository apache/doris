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

import com.google.common.base.Strings;

/**
 * CloudPluginDownloader is the unified entry point for plugin downloads in cloud mode.
 * <p>
 * Architecture design (decoupled separation):
 * 1. CloudPluginConfigProvider - Get S3 authentication info and path configuration from cloud mode
 * 2. S3PluginDownloader - Execute pure S3 download operations (no dependency on cloud mode)
 * 3. CloudPluginDownloader - Compose the above two, providing a simple API
 * <p>
 * Advantages:
 * - Responsibility separation: Configuration retrieval vs file download
 * - Extensibility: Non-cloud mode users can manually configure S3 parameters using S3PluginDownloader
 * - Easy testing: Each component can be tested independently
 */
public class CloudPluginDownloader {

    /**
     * Plugin type enumeration
     */
    public enum PluginType {
        JDBC_DRIVERS("jdbc_drivers"),
        JAVA_UDF("java_udf"),
        CONNECTORS("connectors"),
        HADOOP_CONF("hadoop_conf");

        private final String directoryName;

        PluginType(String directoryName) {
            this.directoryName = directoryName;
        }

        public String getDirectoryName() {
            return directoryName;
        }
    }

    /**
     * Download plugin from cloud storage
     *
     * @param pluginType plugin type
     * @param pluginName plugin name
     * @param localTargetPath local target path
     * @return returns local path on success
     * @throws RuntimeException if download fails
     */
    public static String downloadFromCloud(PluginType pluginType, String pluginName, String localTargetPath) {
        // Check supported plugin types first
        if (pluginType != PluginType.JDBC_DRIVERS && pluginType != PluginType.JAVA_UDF) {
            throw new RuntimeException("Unsupported plugin type for cloud download: " + pluginType);
        }

        if (Strings.isNullOrEmpty(pluginName)) {
            throw new RuntimeException("pluginName cannot be null or empty");
        }

        // 1. Get cloud configuration and build S3 path
        S3PluginDownloader.S3Config s3Config = CloudPluginConfigProvider.getCloudS3Config();
        String instanceId = CloudPluginConfigProvider.getCloudInstanceId();

        // 2. Direct path construction
        String s3Path = String.format("s3://%s/%s/plugins/%s/%s",
                s3Config.bucket, instanceId, pluginType.getDirectoryName(), pluginName);

        // 3. Execute download with graceful degradation based on user intent
        try (S3PluginDownloader downloader = new S3PluginDownloader(s3Config)) {
            return downloader.downloadFile(s3Path, localTargetPath);
        } catch (Exception e) {
            // Handle download failure gracefully
            throw new RuntimeException("Failed to download plugin from cloud: " + e.getMessage());
        }
    }
}
