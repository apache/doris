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

package org.apache.doris.datasource.storage;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.thrift.TFileType;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * THE single fe-core-side registry for storage routing metadata. If you are integrating a new
 * storage system, this is the only file in this package you may need to touch:
 *
 * <ul>
 *   <li><b>New S3-compatible plugin</b>: touch NOTHING here. Ship the fe-filesystem plugin
 *       (provider + typed props with self-declared {@code storageFamilyName()} /
 *       {@code legacyCacheSchemes()} / {@code sensitivePropertyKeys()}); routing consults
 *       unlisted providers after the built-in set, and an S3-compatible binding automatically
 *       joins the {@link StorageTypeId#S3} family.</li>
 *   <li><b>New protocol with its own uri scheme / BE file type</b>: add a {@link Provider} row
 *       (bind priority + type id) and the scheme rows in the static block below — plus the BE
 *       side ({@code TFileType}, readers), which is the actual bulk of such an integration.</li>
 * </ul>
 *
 * <p>Everything else in this package is either the consumer facade ({@link StorageAdapter}),
 * frozen wire glue ({@link S3ThriftAdapter}, {@link CloudObjectStoreAdapter}) or legacy
 * quarantine ({@link S3ResourceCompat}) — none of it is extension surface.</p>
 */
public final class StorageRegistry {

    /**
     * One row per built-in SPI provider, declared in BIND PRIORITY ORDER — {@code values()}
     * order IS the routing order used by {@code FileSystemPluginManager.bindPrimary/bindAll}
     * (the legacy registry order, with JFS hoisted ahead of HDFS so a {@code jfs://} uri beats
     * HDFS's key-hint guess). Each row carries the one thing that must stay fe-core-side: the
     * legacy type id (JFS has no id of its own — fe-core historically rode jfs:// on
     * HdfsProperties, so it maps to HDFS).
     *
     * <p>Providers absent from this table are still routable: they are consulted after every
     * built-in provider, in registration order, and an S3-compatible one joins the S3 family
     * id — see {@code UnlistedProviderExtensibilityTest}.</p>
     */
    public enum Provider {
        JFS(StorageTypeId.HDFS),
        HDFS(StorageTypeId.HDFS),
        OSS_HDFS(StorageTypeId.OSS_HDFS),
        OSS(StorageTypeId.OSS),
        S3(StorageTypeId.S3),
        OBS(StorageTypeId.OBS),
        COS(StorageTypeId.COS),
        GCS(StorageTypeId.GCS),
        AZURE(StorageTypeId.AZURE),
        MINIO(StorageTypeId.MINIO),
        OZONE(StorageTypeId.OZONE),
        BROKER(StorageTypeId.BROKER),
        LOCAL(StorageTypeId.LOCAL),
        HTTP(StorageTypeId.HTTP);

        private final StorageTypeId typeId;

        Provider(StorageTypeId typeId) {
            this.typeId = typeId;
        }

        public StorageTypeId typeId() {
            return typeId;
        }

        /** Case-insensitive lookup by SPI provider name; empty for providers not in the table. */
        public static Optional<Provider> byName(String providerName) {
            if (providerName == null) {
                return Optional.empty();
            }
            try {
                return Optional.of(valueOf(providerName.toUpperCase(Locale.ROOT)));
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        }
    }

    // ------------------------------------------------------------------
    // URI scheme routing table: scheme → StorageTypeId / FileSystemType / TFileType.
    // Successor of the legacy scheme-type mapper; semantics frozen against it:
    //   - blank/absent scheme defaults to HDFS (legacy paths without scheme);
    //   - "oss" maps to plain OSS for scheme-only lookups — OSS-HDFS shares the scheme and is
    //     identified from catalog context, never from the scheme (see LocationPath);
    //   - "local" maps to HDFS (legacy quirk), "file" maps to LOCAL;
    //   - "ofs"/"gfs" stay broker-routed;
    //   - abfs/abfss OneLake endpoints are special-cased by the caller (LocationPath), not here.
    // The scheme→TFileType leg is a BE wire-contract remnant and intentionally stays here.
    // ------------------------------------------------------------------

    private static final Map<String, StorageTypeId> SCHEME_TO_TYPE = new HashMap<>();
    private static final Map<String, FileSystemType> SCHEME_TO_FS_TYPE = new HashMap<>();
    private static final Map<String, TFileType> SCHEME_TO_FILE_TYPE = new HashMap<>();

    private StorageRegistry() {
    }

    private static void register(String scheme, StorageTypeId type, FileSystemType fsType, TFileType fileType) {
        SCHEME_TO_TYPE.put(scheme, type);
        SCHEME_TO_FS_TYPE.put(scheme, fsType);
        SCHEME_TO_FILE_TYPE.put(scheme, fileType);
    }

    static {
        register("s3", StorageTypeId.S3, FileSystemType.S3, TFileType.FILE_S3);
        register("s3a", StorageTypeId.S3, FileSystemType.S3, TFileType.FILE_S3);
        register("s3n", StorageTypeId.S3, FileSystemType.S3, TFileType.FILE_S3);
        register("cosn", StorageTypeId.COS, FileSystemType.S3, TFileType.FILE_S3);
        register("ofs", StorageTypeId.BROKER, FileSystemType.OFS, TFileType.FILE_BROKER);
        register("gfs", StorageTypeId.BROKER, FileSystemType.HDFS, TFileType.FILE_BROKER);
        // JuiceFS is mounted through a Hadoop FileSystem implementation, HDFS-compatible path.
        register("jfs", StorageTypeId.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
        register("viewfs", StorageTypeId.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
        register("file", StorageTypeId.LOCAL, FileSystemType.FILE, TFileType.FILE_LOCAL);
        register("oss", StorageTypeId.OSS, FileSystemType.S3, TFileType.FILE_S3);
        register("obs", StorageTypeId.OBS, FileSystemType.S3, TFileType.FILE_S3);
        register("cos", StorageTypeId.COS, FileSystemType.S3, TFileType.FILE_S3);
        register("gs", StorageTypeId.GCS, FileSystemType.S3, TFileType.FILE_S3);
        register("abfs", StorageTypeId.AZURE, FileSystemType.S3, TFileType.FILE_S3);
        register("abfss", StorageTypeId.AZURE, FileSystemType.S3, TFileType.FILE_S3);
        register("wasb", StorageTypeId.AZURE, FileSystemType.S3, TFileType.FILE_S3);
        register("wasbs", StorageTypeId.AZURE, FileSystemType.S3, TFileType.FILE_S3);
        register("hdfs", StorageTypeId.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
        register("local", StorageTypeId.HDFS, FileSystemType.HDFS, TFileType.FILE_HDFS);
        register("http", StorageTypeId.HTTP, FileSystemType.HTTP, TFileType.FILE_HTTP);
        register("https", StorageTypeId.HTTP, FileSystemType.HTTP, TFileType.FILE_HTTP);
    }

    /**
     * Maps a URI scheme to its storage type. A blank scheme defaults to HDFS — legacy systems
     * often omitted the scheme in HDFS paths ("/user/hadoop/data"); unknown schemes return null
     * (legacy mapper parity).
     */
    public static StorageTypeId fromScheme(String scheme) {
        if (StringUtils.isBlank(scheme)) {
            return StorageTypeId.HDFS;
        }
        return SCHEME_TO_TYPE.get(scheme.toLowerCase());
    }

    /** Maps a URI scheme to the filesystem access type; null scheme defaults to HDFS. */
    public static FileSystemType fromSchemeToFileSystemType(String scheme) {
        if (scheme == null) {
            return FileSystemType.HDFS;
        }
        return SCHEME_TO_FS_TYPE.get(scheme.toLowerCase());
    }

    /** Maps a URI scheme to the BE TFileType; null scheme defaults to FILE_HDFS. */
    public static TFileType fromSchemeToFileType(String scheme) {
        if (scheme == null) {
            return TFileType.FILE_HDFS;
        }
        return SCHEME_TO_FILE_TYPE.get(scheme.toLowerCase());
    }
}
