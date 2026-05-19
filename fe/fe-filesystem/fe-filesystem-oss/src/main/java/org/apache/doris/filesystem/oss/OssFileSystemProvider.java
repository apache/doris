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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Map;

/**
 * SPI provider for Alibaba Cloud OSS.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by an endpoint containing {@code aliyuncs.com}. OSS-specific property
 * keys are consumed by {@link OssObjStorage}, which uses the Alibaba Cloud native SDK.
 */
public class OssFileSystemProvider implements FileSystemProvider<FileSystemProperties> {

    @Override
    public boolean supports(Map<String, String> properties) {
        if ("OSS".equals(properties.get("_STORAGE_TYPE_"))) {
            return true;
        }
        String endpoint = properties.get("OSS_ENDPOINT");
        if (endpoint == null) {
            endpoint = properties.get("AWS_ENDPOINT");
        }
        return endpoint != null && endpoint.contains("aliyuncs.com");
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return new OssFileSystem(new OssObjStorage(properties));
    }

    @Override
    public String name() {
        return "OSS";
    }
}
