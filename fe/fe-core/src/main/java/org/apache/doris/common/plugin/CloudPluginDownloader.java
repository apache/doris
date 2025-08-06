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

import org.apache.doris.common.Config;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger LOG = LogManager.getLogger(CloudPluginDownloader.class);

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
     * Download plugin (main entry method, backward compatible)
     *
     * @param pluginType plugin type
     * @param pluginName plugin name (can be null for CONNECTORS batch download)
     * @param localTargetPath local target path
     * @return returns local path on success, empty string on failure
     */
    public static String downloadPluginIfNeeded(PluginType pluginType, String pluginName, String localTargetPath) {
        // Return directly in non-cloud mode
        if (!Config.isCloudMode()) {
            return "";
        }

        try {
            return downloadFromCloud(pluginType, pluginName, localTargetPath, null);
        } catch (Exception e) {
            LOG.warn("Failed to download plugin {} from cloud: {}", pluginName, e.getMessage());
            return "";
        }
    }

    /**
     * Download plugin from cloud storage (supports MD5 verification)
     *
     * @param pluginType plugin type
     * @param pluginName plugin name
     * @param localTargetPath local target path
     * @param expectedMd5 optional MD5 verification value
     * @return returns local path on success, empty string on failure
     */
    public static String downloadFromCloud(PluginType pluginType, String pluginName,
            String localTargetPath, String expectedMd5) {
        try {
            // 1. Get cloud S3 configuration
            S3PluginDownloader.S3Config s3Config = CloudPluginConfigProvider.getCloudS3Config();
            if (s3Config == null) {
                LOG.warn("Cannot get cloud S3 configuration");
                return "";
            }

            // 2. Build S3 path
            String s3Path = buildS3Path(s3Config, pluginType, pluginName);
            if (s3Path == null) {
                LOG.warn("Cannot build S3 path for plugin {}/{}", pluginType.getDirectoryName(), pluginName);
                return "";
            }

            // 3. Use S3 downloader to download
            try (S3PluginDownloader downloader = new S3PluginDownloader(s3Config)) {
                if (pluginType == PluginType.CONNECTORS && Strings.isNullOrEmpty(pluginName)) {
                    // Batch download connectors directory
                    return downloader.downloadAndExtractDirectory(s3Path, localTargetPath);
                } else {
                    // Single file download
                    return downloader.downloadFile(s3Path, localTargetPath, expectedMd5);
                }
            }

        } catch (Exception e) {
            LOG.warn("Failed to download plugin from cloud: {}", e.getMessage());
            return "";
        }
    }

    /**
     * Manually configured S3 plugin download (also usable in non-cloud mode)
     *
     * @param s3Config manually configured S3 parameters
     * @param remoteS3Path remote S3 path
     * @param localPath local path
     * @param expectedMd5 optional MD5 verification value
     * @return returns local path on success, empty string on failure
     */
    public static String downloadFromS3(S3PluginDownloader.S3Config s3Config,
            String remoteS3Path, String localPath, String expectedMd5) {
        try (S3PluginDownloader downloader = new S3PluginDownloader(s3Config)) {
            return downloader.downloadFile(remoteS3Path, localPath, expectedMd5);
        } catch (Exception e) {
            LOG.warn("Failed to download from S3: {}", e.getMessage());
            return "";
        }
    }

    private static String buildS3Path(S3PluginDownloader.S3Config s3Config, PluginType pluginType, String pluginName) {
        try {
            // Build relative path
            String relativePath = CloudPluginConfigProvider.buildPluginPath(pluginType.getDirectoryName(), pluginName);
            if (relativePath == null) {
                return null;
            }

            // Build complete S3 path
            if (Strings.isNullOrEmpty(pluginName)) {
                // Directory path, ensure it ends with /
                return "s3://" + s3Config.bucket + "/" + relativePath + "/";
            } else {
                // File path
                return "s3://" + s3Config.bucket + "/" + relativePath;
            }

        } catch (Exception e) {
            LOG.warn("Failed to build S3 path: {}", e.getMessage());
            return null;
        }
    }
}
