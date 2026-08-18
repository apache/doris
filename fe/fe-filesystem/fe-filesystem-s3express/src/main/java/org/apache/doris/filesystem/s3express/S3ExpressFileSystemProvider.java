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

package org.apache.doris.filesystem.s3express;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.s3.S3CompatSignals;
import org.apache.doris.filesystem.s3.S3FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/** SPI provider for S3 Express configurations. */
public final class S3ExpressFileSystemProvider
        implements FileSystemProvider<S3ExpressFileSystemProperties> {

    @Override
    public boolean supports(Map<String, String> properties) {
        return S3CompatSignals.isS3Express(properties);
    }

    @Override
    public boolean supportsExplicit(Map<String, String> properties) {
        return S3CompatSignals.isS3Express(properties)
                && S3CompatSignals.hasAnyExplicitFsSupport(properties);
    }

    @Override
    public boolean supportsGuess(Map<String, String> properties) {
        return S3CompatSignals.isS3Express(properties);
    }

    @Override
    public S3ExpressFileSystemProperties bind(Map<String, String> properties) {
        return S3ExpressFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(S3ExpressFileSystemProperties properties) throws IOException {
        return new S3ExpressFileSystem(properties);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return S3CompatSignals.S3_EXPRESS_PROVIDER;
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(S3FileSystemProperties.class);
    }
}
