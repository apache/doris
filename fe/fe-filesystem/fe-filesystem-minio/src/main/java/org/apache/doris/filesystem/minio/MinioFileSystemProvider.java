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

package org.apache.doris.filesystem.minio;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.s3.S3CompatSignals;
import org.apache.doris.filesystem.s3.S3FileSystem;
import org.apache.doris.filesystem.s3.S3FileSystemProperties;
import org.apache.doris.filesystem.s3.S3ObjStorage;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for MinIO (S3-compatible object storage).
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Selected explicitly by {@code provider=MINIO} or {@code fs.minio.support=true}; otherwise,
 * when the user declared no filesystem explicitly, by {@link S3CompatSignals#guessIsMinio}
 * ({@code minio.*} keys, since MinIO endpoints have no recognizable domain pattern).
 * Delegates I/O to {@link S3FileSystem}.
 */
public class MinioFileSystemProvider implements FileSystemProvider<MinioFileSystemProperties> {

    private static final String STORAGE_TYPE_MINIO = "MINIO";
    private static final String FS_MINIO_SUPPORT = "fs.minio.support";

    @Override
    public boolean supports(Map<String, String> properties) {
        if (STORAGE_TYPE_MINIO.equalsIgnoreCase(properties.get(S3CompatSignals.PROVIDER_KEY))
                || S3CompatSignals.isFsSupport(properties, FS_MINIO_SUPPORT)) {
            return true;
        }
        return S3CompatSignals.guessAllowed(properties) && S3CompatSignals.guessIsMinio(properties);
    }

    @Override
    public MinioFileSystemProperties bind(Map<String, String> properties) {
        return MinioFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(MinioFileSystemProperties properties) throws IOException {
        S3FileSystemProperties delegate = S3FileSystemProperties.of(properties.toS3CompatibleKv());
        return new S3FileSystem(delegate,
                new S3ObjStorage(delegate, properties.getSupportedSchemes()));
    }

    private static final String[] MINIO_GUESS_IDENTIFIERS = {
            "minio.access_key", "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key", "s3.access_key",
            "minio.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"};
    private static final String[] MINIO_GUESS_ENDPOINT_NAMES = {
            "minio.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"};
    // Endpoint markers of the clouds whose guessIsMe fe-core's MinioProperties.guessIsMe
    // consults for mutual exclusion (Azure/COS/OSS/S3, plus GCS which the dialect split adds).
    private static final String[] OTHER_CLOUD_ENDPOINT_MARKERS = {
            "aliyuncs.com", "myqcloud.com", "amazonaws.com", "storage.googleapis.com",
            ".blob.core.windows.net", ".dfs.core.windows.net", ".blob.core.chinacloudapi.cn"};
    private static final String[] S3_GUESS_REGION_NAMES = {
            "s3.region", "glue.region", "aws.glue.region", "iceberg.rest.signing-region",
            "rest.signing-region", "client.region"};

    @Override
    public boolean supportsExplicit(Map<String, String> properties) {
        return S3CompatSignals.isFsSupport(properties, FS_MINIO_SUPPORT);
    }

    @Override
    public boolean supportsGuess(Map<String, String> properties) {
        // Verbatim port of fe-core MinioProperties.guessIsMe: MinIO is the "any other
        // S3-compatible" fallback — it claims the map iff none of Azure/COS/OSS/S3 would claim
        // it AND at least one identifier key is present. The sibling guesses are replicated
        // here (plugins cannot call each other's classes), preserving legacy control flow
        // EXACTLY: legacy S3.guessIsMe SHORT-CIRCUITS on a present endpoint alias (claiming
        // only an amazonaws endpoint), and consults its uri/region fallbacks ONLY when no
        // endpoint alias exists. A present non-cloud endpoint (e.g. a private MinIO address)
        // therefore stays MinIO's even when an s3.region key is also set.
        if ("azure".equalsIgnoreCase(properties.get(S3CompatSignals.PROVIDER_KEY))) {
            return false;
        }
        String endpoint = null;
        for (String name : MINIO_GUESS_ENDPOINT_NAMES) {
            String value = properties.get(name);
            if (value != null && !value.isBlank()) {
                endpoint = value;
                break;
            }
        }
        if (endpoint != null) {
            String lower = endpoint.toLowerCase(java.util.Locale.ROOT);
            for (String marker : OTHER_CLOUD_ENDPOINT_MARKERS) {
                if (lower.contains(marker)) {
                    return false;
                }
            }
            // endpoint present and unclaimed by any cloud marker: legacy S3's uri/region
            // fallbacks never run in this case — do NOT exclude on region keys here.
        } else {
            // No endpoint alias: legacy S3.guessIsMe falls back to the uri marker, then to
            // region keys — replicate both exclusion legs.
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if ("uri".equalsIgnoreCase(entry.getKey()) && entry.getValue() != null) {
                    // case-sensitive contains, byte-identical to legacy S3Properties.guessIsMe
                    if (entry.getValue().contains("amazonaws.com")) {
                        return false;
                    }
                    break;
                }
            }
            for (String name : S3_GUESS_REGION_NAMES) {
                String value = properties.get(name);
                if (value != null && !value.isBlank()) {
                    return false;
                }
            }
        }
        for (String name : MINIO_GUESS_IDENTIFIERS) {
            String value = properties.get(name);
            if (value != null && !value.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "MINIO";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(MinioFileSystemProperties.class);
    }
}
