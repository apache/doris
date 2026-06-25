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
import org.apache.doris.filesystem.hdfs.properties.HdfsProperties;
import org.apache.doris.filesystem.hdfs.properties.OssHdfsProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for plain HDFS-family filesystems: hdfs, viewfs, ofs, jfs.
 * Registered via META-INF/services for Java ServiceLoader discovery.
 *
 * <p>Aliyun OSS-HDFS ({@code oss://}, JindoFS) is intentionally NOT handled here — it has its own
 * {@link OssHdfsFileSystemProvider}. Routing is kept strictly disjoint: the authoritative
 * {@code _STORAGE_TYPE_} marker wins, and the heuristic fallback excludes anything
 * {@link OssHdfsProperties#guessIsMe} claims. {@code oss} is therefore absent from
 * {@link #SUPPORTED_SCHEMES}.</p>
 */
public class HdfsFileSystemProvider implements FileSystemProvider<FileSystemProperties> {

    public static final Set<String> SUPPORTED_SCHEMES = Set.of("hdfs", "viewfs", "ofs", "jfs");

    @Override
    public boolean supports(Map<String, String> properties) {
        // Authoritative match: StoragePropertiesConverter always sets this key for HDFS storage,
        // including Hive catalog properties that may not carry explicit HDFS connection keys.
        String storageType = properties.get("_STORAGE_TYPE_");
        if ("HDFS".equals(storageType)) {
            return true;
        }
        if ("OSS_HDFS".equals(storageType)) {
            // An explicit OSS-HDFS marker belongs to OssHdfsFileSystemProvider, never here.
            return false;
        }
        // No authoritative marker: never claim a configuration that OSS-HDFS owns.
        if (OssHdfsProperties.guessIsMe(properties)) {
            return false;
        }
        String uri = properties.get("HDFS_URI");
        if (uri == null) {
            uri = properties.get("fs.defaultFS");
        }
        if (uri != null) {
            int schemeEnd = uri.indexOf("://");
            if (schemeEnd > 0) {
                String scheme = uri.substring(0, schemeEnd).toLowerCase();
                return SUPPORTED_SCHEMES.contains(scheme);
            }
        }
        return properties.containsKey("dfs.nameservices")
                || properties.containsKey("hadoop.kerberos.principal");
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        // Resolve raw user properties through the migrated HdfsProperties so that typed auth
        // params (hdfs.authentication.*) are translated to Hadoop keys, xml resources are
        // loaded, and defaults are injected — instead of passing raw keys straight through.
        HdfsProperties hdfsProperties = new HdfsProperties(properties);
        hdfsProperties.initNormalizeAndCheckProps();
        return new DFSFileSystem(hdfsProperties.getBackendConfigProperties());
    }

    @Override
    public String name() {
        return "HDFS";
    }
}
