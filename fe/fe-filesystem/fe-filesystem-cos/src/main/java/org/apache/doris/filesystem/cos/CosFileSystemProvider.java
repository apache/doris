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

package org.apache.doris.filesystem.cos;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Map;

/**
 * SPI provider for Tencent Cloud COS.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by an endpoint containing {@code myqcloud.com}. COS-specific property
 * keys are consumed by {@link CosObjStorage}, which uses the Tencent Cloud native SDK.
 */
public class CosFileSystemProvider implements FileSystemProvider<CosFileSystemProperties> {

    private static final String STORAGE_TYPE_KEY = "_STORAGE_TYPE_";
    private static final String STORAGE_TYPE_COS = "COS";
    private static final String PROVIDER_KEY = "provider";
    private static final String FS_COS_SUPPORT = "fs.cos.support";
    private static final String[] ENDPOINT_NAMES = {
            CosFileSystemProperties.ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "COS_ENDPOINT"};

    @Override
    public boolean supports(Map<String, String> properties) {
        if (isExplicitCos(properties)) {
            return true;
        }
        String endpoint = firstPresent(properties, ENDPOINT_NAMES);
        return endpoint != null && endpoint.contains("myqcloud.com");
    }

    @Override
    public CosFileSystemProperties bind(Map<String, String> properties) {
        return CosFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(CosFileSystemProperties properties) throws IOException {
        return new CosFileSystem(new CosObjStorage(properties));
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "COS";
    }

    private boolean isExplicitCos(Map<String, String> properties) {
        return STORAGE_TYPE_COS.equalsIgnoreCase(properties.get(STORAGE_TYPE_KEY))
                || STORAGE_TYPE_COS.equalsIgnoreCase(properties.get(PROVIDER_KEY))
                || Boolean.parseBoolean(properties.getOrDefault(FS_COS_SUPPORT, "false"));
    }

    private String firstPresent(Map<String, String> properties, String[] names) {
        for (String name : names) {
            if (StringUtils.isNotBlank(properties.get(name))) {
                return properties.get(name);
            }
        }
        return null;
    }
}
