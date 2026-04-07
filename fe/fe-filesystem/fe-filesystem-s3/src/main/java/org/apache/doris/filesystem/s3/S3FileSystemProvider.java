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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;

/**
 * SPI provider for AWS S3 and S3-compatible storage (MinIO, etc.).
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by presence of AWS_ACCESS_KEY with either AWS_ENDPOINT or AWS_REGION.
 * S3 is intentionally the last-resort provider; cloud-specific providers (OSS, COS, OBS)
 * should match first via their endpoint domain patterns.
 */
public class S3FileSystemProvider implements FileSystemProvider {

    @Override
    public boolean supports(Map<String, String> properties) {
        Map<String, String> normalized = S3ObjStorage.normalizeProperties(properties);
        String accessKey = normalized.get(S3ObjStorage.PROP_ACCESS_KEY);
        String endpoint = normalized.get(S3ObjStorage.PROP_ENDPOINT);
        String region = normalized.get(S3ObjStorage.PROP_REGION);
        // Require access key + (endpoint or region)
        return accessKey != null && !accessKey.isEmpty()
                && (endpoint != null || region != null);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        Map<String, String> normalized = S3ObjStorage.normalizeProperties(properties);
        S3ObjStorage storage = new S3ObjStorage(normalized);
        return new S3FileSystem(storage);
    }

    @Override
    public String name() {
        return "S3";
    }
}
