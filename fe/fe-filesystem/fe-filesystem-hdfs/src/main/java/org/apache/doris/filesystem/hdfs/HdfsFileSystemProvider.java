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
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for HDFS-family filesystems: hdfs, viewfs, ofs, jfs, oss.
 * Registered via META-INF/services for Java ServiceLoader discovery.
 */
public class HdfsFileSystemProvider implements FileSystemProvider {

    private static final Set<String> SUPPORTED_SCHEMES = Set.of("hdfs", "viewfs", "ofs", "jfs", "oss");

    @Override
    public boolean supports(Map<String, String> properties) {
        // Authoritative match: StoragePropertiesConverter always sets this key for HDFS storage,
        // including Hive catalog properties that may not carry explicit HDFS connection keys.
        if ("HDFS".equals(properties.get("_STORAGE_TYPE_"))) {
            return true;
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
        return new DFSFileSystem(properties);
    }

    @Override
    public String name() {
        return "HDFS";
    }
}
