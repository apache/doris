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

package org.apache.doris.filesystem.jfs;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.hdfs.DFSFileSystem;
import org.apache.doris.filesystem.jfs.properties.JfsProperties;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for JuiceFS ({@code jfs://}), served by the HDFS-compatible {@code DFSFileSystem}.
 * Routing is by URI scheme only; fe-core marks jfs as the generic "HDFS" storage type, so scheme
 * is what keeps this disjoint from {@code HdfsFileSystemProvider}.
 */
public class JfsFileSystemProvider implements FileSystemProvider<FileSystemProperties> {

    private static final Set<String> SUPPORTED_SCHEMES = Set.of("jfs");

    @Override
    public boolean supports(Map<String, String> properties) {
        String uri = properties.get("HDFS_URI");
        if (uri == null) {
            uri = properties.get("fs.defaultFS");
        }
        if (uri == null) {
            return false;
        }
        int schemeEnd = uri.indexOf("://");
        if (schemeEnd <= 0) {
            return false;
        }
        return SUPPORTED_SCHEMES.contains(uri.substring(0, schemeEnd).toLowerCase(Locale.ROOT));
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        JfsProperties jfsProperties = new JfsProperties(properties);
        jfsProperties.initNormalizeAndCheckProps();
        return new DFSFileSystem(jfsProperties.getBackendConfigProperties());
    }

    @Override
    public String name() {
        return "JFS";
    }
}
