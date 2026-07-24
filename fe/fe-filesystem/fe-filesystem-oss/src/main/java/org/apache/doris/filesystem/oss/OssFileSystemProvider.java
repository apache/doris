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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for Alibaba Cloud OSS.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by an endpoint containing {@code aliyuncs.com}. OSS-specific property
 * keys are consumed by {@link OssObjStorage}, which uses the Alibaba Cloud native SDK.
 */
public class OssFileSystemProvider implements FileSystemProvider<OssFileSystemProperties> {

    private static final String STORAGE_TYPE_KEY = "_STORAGE_TYPE_";
    private static final String STORAGE_TYPE_OSS = "OSS";
    private static final String STORAGE_TYPE_OSS_HDFS = "OSS_HDFS";
    private static final String PROVIDER_KEY = "provider";
    private static final String FS_OSS_SUPPORT = "fs.oss.support";
    private static final String FS_OSS_HDFS_SUPPORT = "fs.oss-hdfs.support";
    private static final String DEPRECATED_OSS_HDFS_SUPPORT = "oss.hdfs.enabled";
    // OSS-HDFS (JindoFS) endpoints live on the *.oss-dls.aliyuncs.com host; they contain
    // "aliyuncs.com" but are served by OssHdfsFileSystemProvider, not native OSS.
    private static final String OSS_HDFS_ENDPOINT_MARKER = "oss-dls.aliyuncs.com";
    private static final String[] ENDPOINT_NAMES = {
            OssFileSystemProperties.ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "dlf.endpoint", "dlf.catalog.endpoint", "fs.oss.endpoint", "OSS_ENDPOINT"};

    @Override
    public boolean supports(Map<String, String> properties) {
        // OSS-HDFS (JindoFS) is served by OssHdfsFileSystemProvider. Routing is first-match-wins over
        // an unordered list, so native OSS must positively disclaim any OSS-HDFS config to stay disjoint.
        if (STORAGE_TYPE_OSS_HDFS.equalsIgnoreCase(properties.get(STORAGE_TYPE_KEY))) {
            return false;
        }
        if (STORAGE_TYPE_OSS.equalsIgnoreCase(properties.get(STORAGE_TYPE_KEY))) {
            return true;
        }
        // Explicit OSS-HDFS flags outrank even fs.oss.support in the kernel's
        // StorageProperties.createPrimary; mirror that precedence here.
        if (Boolean.parseBoolean(properties.getOrDefault(FS_OSS_HDFS_SUPPORT, "false"))
                || Boolean.parseBoolean(properties.getOrDefault(DEPRECATED_OSS_HDFS_SUPPORT, "false"))) {
            return false;
        }
        if (isExplicitOss(properties)) {
            return true;
        }
        String endpoint = firstPresent(properties, ENDPOINT_NAMES);
        if (endpoint == null || !endpoint.contains("aliyuncs.com")) {
            return false;
        }
        return !endpoint.contains(OSS_HDFS_ENDPOINT_MARKER);
    }

    @Override
    public OssFileSystemProperties bind(Map<String, String> properties) {
        return OssFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(OssFileSystemProperties properties) throws IOException {
        return new OssFileSystem(new OssObjStorage(properties));
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "OSS";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(OssFileSystemProperties.class);
    }

    private boolean isExplicitOss(Map<String, String> properties) {
        return STORAGE_TYPE_OSS.equalsIgnoreCase(properties.get(PROVIDER_KEY))
                || Boolean.parseBoolean(properties.getOrDefault(FS_OSS_SUPPORT, "false"));
    }

    private String firstPresent(Map<String, String> properties, String[] names) {
        for (String name : names) {
            if (StringUtils.isNotBlank(properties.get(name))) {
                return properties.get(name);
            }
        }
        return null;
    }
}
