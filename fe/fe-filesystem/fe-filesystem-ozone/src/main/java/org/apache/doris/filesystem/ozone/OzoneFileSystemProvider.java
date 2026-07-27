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

package org.apache.doris.filesystem.ozone;

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
 * SPI provider for Apache Ozone via its S3 Gateway.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Selected ONLY explicitly, by {@code provider=OZONE} or {@code fs.ozone.support=true}: legacy
 * {@code OzoneProperties} has no {@code guessIsMe}, so {@link S3CompatSignals#guessIsOzone} is
 * always false and an unflagged Ozone map falls through to the generic S3 provider — which serves
 * it correctly, since the S3 Gateway is S3-compatible. Delegates I/O to {@link S3FileSystem}.
 */
public class OzoneFileSystemProvider implements FileSystemProvider<OzoneFileSystemProperties> {

    private static final String STORAGE_TYPE_OZONE = "OZONE";
    private static final String FS_OZONE_SUPPORT = "fs.ozone.support";

    @Override
    public boolean supports(Map<String, String> properties) {
        if (STORAGE_TYPE_OZONE.equalsIgnoreCase(properties.get(S3CompatSignals.PROVIDER_KEY))
                || S3CompatSignals.isFsSupport(properties, FS_OZONE_SUPPORT)) {
            return true;
        }
        return S3CompatSignals.guessAllowed(properties) && S3CompatSignals.guessIsOzone(properties);
    }

    @Override
    public OzoneFileSystemProperties bind(Map<String, String> properties) {
        return OzoneFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(OzoneFileSystemProperties properties) throws IOException {
        S3FileSystemProperties delegate = S3FileSystemProperties.of(properties.toS3CompatibleKv());
        return new S3FileSystem(delegate,
                new S3ObjStorage(delegate, properties.getSupportedSchemes()));
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "OZONE";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(OzoneFileSystemProperties.class);
    }
}
