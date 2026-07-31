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
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.net.URI;
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
public class HdfsFileSystemProvider implements FileSystemProvider<HdfsProperties> {

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
    public HdfsProperties bind(Map<String, String> properties) {
        // Resolve raw user properties through the migrated HdfsProperties so that typed auth
        // params (hdfs.authentication.*) are translated to Hadoop keys, xml resources are
        // loaded, and defaults are injected — instead of passing raw keys straight through.
        HdfsProperties hdfsProperties = new HdfsProperties(properties);
        hdfsProperties.initNormalizeAndCheckProps();
        return hdfsProperties;
    }

    @Override
    public FileSystem create(HdfsProperties properties) throws IOException {
        return new DFSFileSystem(properties.getBackendConfigProperties());
    }

    private static final Set<String> GUESS_HINT_KEYS = Set.of("hdfs.authentication.type",
            "hadoop.security.authentication", "hadoop.username", "fs.defaultFS",
            "hdfs.authentication.kerberos.principal", "hadoop.kerberos.principal",
            "dfs.nameservices", "hdfs.config.resources");

    @Override
    public boolean supportsExplicit(Map<String, String> properties) {
        return Boolean.parseBoolean(properties.getOrDefault("fs.hdfs.support", "false"));
    }

    @Override
    public boolean supportsGuess(Map<String, String> properties) {
        // Verbatim port of fe-core HdfsProperties.guessIsMe/HdfsPropertiesUtils
        // .validateUriIsHdfsUri, minus jfs:// which the dedicated JFS plugin claims (the bind
        // registry orders JFS ahead of HDFS). Two legacy quirks preserved deliberately:
        // a present-but-scheme-less uri THROWS (legacy ran the HDFS guess first, so this error
        // pre-empted every other provider's guess and failed the whole routing); an unparsable
        // uri (e.g. s3 glob syntax with '{') is swallowed and treated as not-HDFS.
        String uriStr = rawUserUri(properties);
        if (uriStr != null && !uriStr.trim().isEmpty()) {
            URI uri = null;
            try {
                uri = URI.create(uriStr);
            } catch (Exception ex) {
                // legacy warned and fell through to the hint keys
            }
            if (uri != null) {
                String scheme = uri.getScheme();
                if (scheme == null || scheme.trim().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Invalid uri: " + uriStr + ", extract schema is null");
                }
                if (SUPPORTED_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT))) {
                    return true;
                }
            }
        }
        return GUESS_HINT_KEYS.stream().anyMatch(properties::containsKey);
    }

    private static String rawUserUri(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if ("uri".equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "HDFS";
    }
}
