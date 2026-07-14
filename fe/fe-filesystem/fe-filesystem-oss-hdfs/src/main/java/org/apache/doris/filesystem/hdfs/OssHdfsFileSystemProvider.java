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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.hdfs.properties.OssHdfsProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for Aliyun OSS-HDFS (JindoFS), addressed with {@code oss://} URIs and served by an
 * HDFS-compatible {@code DFSFileSystem}. Registered via META-INF/services alongside
 * {@link HdfsFileSystemProvider}.
 *
 * <p>Routing is kept strictly disjoint from plain HDFS: the authoritative {@code _STORAGE_TYPE_}
 * marker set by fe-core's StoragePropertiesConverter wins, and the heuristic fallback keys on
 * {@link OssHdfsProperties#guessIsMe} (oss.hdfs flag / oss-dls endpoint) or an {@code oss://} URI.
 * The framework selects providers first-match-wins over an unordered list, so the two providers'
 * {@code supports()} predicates must not overlap.</p>
 */
public class OssHdfsFileSystemProvider implements FileSystemProvider<FileSystemProperties> {

    private static final String FS_OSS_HDFS_SUPPORT = "fs.oss-hdfs.support";
    private static final String DEPRECATED_OSS_HDFS_SUPPORT = "oss.hdfs.enabled";
    private static final String FS_OSS_SUPPORT = "fs.oss.support";

    @Override
    public boolean supports(Map<String, String> properties) {
        String storageType = properties.get("_STORAGE_TYPE_");
        if ("OSS_HDFS".equals(storageType)) {
            return true;
        }
        if ("HDFS".equals(storageType)) {
            // An explicit plain-HDFS marker belongs to HdfsFileSystemProvider, never here.
            return false;
        }
        if ("OSS".equals(storageType)) {
            // A native-OSS (S3-compatible) marker belongs to OssFileSystemProvider, never here.
            return false;
        }
        // Explicit kernel flags, mirroring StorageProperties.createPrimary precedence:
        // fs.oss-hdfs.support / oss.hdfs.enabled declare OSS-HDFS and win over everything below,
        // while fs.oss.support declares native OSS, so the bare oss:// URI fallback must yield.
        if (isFlagTrue(properties, FS_OSS_HDFS_SUPPORT) || isFlagTrue(properties, DEPRECATED_OSS_HDFS_SUPPORT)) {
            return true;
        }
        if (isFlagTrue(properties, FS_OSS_SUPPORT)) {
            return false;
        }
        // No authoritative marker: fall back to the same detection fe-core uses to pick
        // OSSHdfsProperties, plus a bare oss:// URI.
        if (OssHdfsProperties.guessIsMe(properties)) {
            return true;
        }
        String uri = properties.get("HDFS_URI");
        if (uri == null) {
            uri = properties.get("fs.defaultFS");
        }
        return uri != null && uri.toLowerCase(Locale.ROOT).startsWith("oss://");
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        OssHdfsProperties ossHdfsProperties = new OssHdfsProperties(properties);
        ossHdfsProperties.initNormalizeAndCheckProps();
        return new DFSFileSystem(ossHdfsProperties.getBackendConfigProperties());
    }

    @Override
    public String name() {
        return "OSS_HDFS";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(OssHdfsProperties.class);
    }

    private static boolean isFlagTrue(Map<String, String> properties, String key) {
        return Boolean.parseBoolean(properties.getOrDefault(key, "false"));
    }
}
