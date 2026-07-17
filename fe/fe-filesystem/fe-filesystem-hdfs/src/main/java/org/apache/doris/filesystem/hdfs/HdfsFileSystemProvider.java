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
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for plain HDFS-family filesystems: hdfs, viewfs only.
 * Registered via META-INF/services for Java ServiceLoader discovery.
 *
 * <p>Aliyun OSS-HDFS ({@code oss://}, JindoFS) is served by the sibling {@code fe-filesystem-oss-hdfs}
 * plugin, and JuiceFS ({@code jfs://}) by the sibling {@code fe-filesystem-jfs} plugin;
 * {@code ofs://} (Tencent CHDFS) is broker-routed by fe-core and is not an SPI filesystem here.
 * Routing is kept strictly disjoint: a concrete uri scheme is authoritative and only
 * {@code hdfs}/{@code viewfs} are claimed here; the {@code _STORAGE_TYPE_} "HDFS" marker is only a
 * fallback when there is no uri scheme.</p>
 */
public class HdfsFileSystemProvider implements FileSystemProvider<FileSystemProperties> {

    public static final Set<String> SUPPORTED_SCHEMES = Set.of("hdfs", "viewfs");

    @Override
    public boolean supports(Map<String, String> properties) {
        String storageType = properties.get("_STORAGE_TYPE_");
        // OSS-HDFS (oss://, JindoFS) belongs to OssHdfsFileSystemProvider.
        if ("OSS_HDFS".equals(storageType)) {
            return false;
        }
        String uri = properties.get("HDFS_URI");
        if (uri == null) {
            uri = properties.get("fs.defaultFS");
        }
        if (uri != null) {
            int schemeEnd = uri.indexOf("://");
            if (schemeEnd > 0) {
                // A concrete scheme is authoritative: only hdfs/viewfs are ours. jfs:// and oss://
                // are served by sibling filesystem plugins and ofs:// is broker-routed by fe-core,
                // so all are declined here even though fe-core also marks jfs as "HDFS".
                return SUPPORTED_SCHEMES.contains(uri.substring(0, schemeEnd).toLowerCase(Locale.ROOT));
            }
        }
        // No uri scheme: fall back to the authoritative HDFS marker or HA/kerberos hints
        // (e.g. Hive catalog properties without explicit connection URIs).
        if ("HDFS".equals(storageType)) {
            return true;
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
