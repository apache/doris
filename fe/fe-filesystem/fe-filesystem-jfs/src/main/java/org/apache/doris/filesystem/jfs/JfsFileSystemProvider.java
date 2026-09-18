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
public class JfsFileSystemProvider implements FileSystemProvider<JfsProperties> {

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
    public JfsProperties bind(Map<String, String> properties) {
        JfsProperties jfsProperties = new JfsProperties(properties);
        jfsProperties.initNormalizeAndCheckProps();
        return jfsProperties;
    }

    @Override
    public FileSystem create(JfsProperties properties) throws IOException {
        return new DFSFileSystem(properties.getBackendConfigProperties());
    }

    @Override
    public boolean supportsGuess(Map<String, String> properties) {
        // fe-core routes jfs:// through HdfsProperties; the plugin split gives jfs its own
        // provider, claimed by uri scheme on the raw map (plus fs.defaultFS). No explicit
        // fs.jfs.support flag exists in fe-core, so supportsExplicit stays false.
        String uri = null;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if ("uri".equalsIgnoreCase(entry.getKey())) {
                uri = entry.getValue();
                break;
            }
        }
        if (uri == null) {
            uri = properties.get("fs.defaultFS");
        }
        if (uri == null) {
            return false;
        }
        int schemeEnd = uri.indexOf("://");
        return schemeEnd > 0
                && SUPPORTED_SCHEMES.contains(uri.substring(0, schemeEnd).toLowerCase(Locale.ROOT));
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "JFS";
    }
}
