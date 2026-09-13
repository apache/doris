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

package org.apache.doris.filesystem.gcs;

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
 * SPI provider for Google Cloud Storage over the S3 interoperability endpoint (HMAC keys).
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Selected explicitly by {@code provider=GCS}/{@code GCP} or {@code fs.gcs.support=true};
 * otherwise, when the user declared no filesystem explicitly, by
 * {@link S3CompatSignals#guessIsGcs} (a {@code gs.endpoint}, or an endpoint on
 * {@code storage.googleapis.com}). Delegates I/O to {@link S3FileSystem}.
 */
public class GcsFileSystemProvider implements FileSystemProvider<GcsFileSystemProperties> {

    private static final String STORAGE_TYPE_GCS = "GCS";
    private static final String FS_GCS_SUPPORT = "fs.gcs.support";

    @Override
    public boolean supports(Map<String, String> properties) {
        String providerValue = properties.get(S3CompatSignals.PROVIDER_KEY);
        // "GCP" is the value legacy GCSProperties stamps into getBackendConfigProperties().
        if (STORAGE_TYPE_GCS.equalsIgnoreCase(providerValue)
                || "GCP".equalsIgnoreCase(providerValue)
                || S3CompatSignals.isFsSupport(properties, FS_GCS_SUPPORT)) {
            return true;
        }
        return S3CompatSignals.guessAllowed(properties) && S3CompatSignals.guessIsGcs(properties);
    }

    @Override
    public GcsFileSystemProperties bind(Map<String, String> properties) {
        return GcsFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(GcsFileSystemProperties properties) throws IOException {
        S3FileSystemProperties delegate = S3FileSystemProperties.of(properties.toS3CompatibleKv());
        return new S3FileSystem(delegate,
                new S3ObjStorage(delegate, properties.getSupportedSchemes()));
    }

    @Override
    public boolean supportsExplicit(Map<String, String> properties) {
        return S3CompatSignals.isFsSupport(properties, FS_GCS_SUPPORT);
    }

    @Override
    public boolean supportsGuess(Map<String, String> properties) {
        return S3CompatSignals.guessIsGcs(properties);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "GCS";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(GcsFileSystemProperties.class);
    }
}
