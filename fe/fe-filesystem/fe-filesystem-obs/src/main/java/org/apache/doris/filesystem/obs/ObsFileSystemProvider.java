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

package org.apache.doris.filesystem.obs;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for Huawei Cloud OBS.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by an endpoint containing {@code myhuaweicloud.com}. OBS-specific property
 * keys are consumed by {@link ObsObjStorage}, which uses the Huawei Cloud native SDK.
 */
public class ObsFileSystemProvider implements FileSystemProvider<ObsFileSystemProperties> {

    private static final String STORAGE_TYPE_KEY = "_STORAGE_TYPE_";
    private static final String STORAGE_TYPE_OBS = "OBS";
    private static final String PROVIDER_KEY = "provider";
    private static final String FS_OBS_SUPPORT = "fs.obs.support";
    private static final String[] ENDPOINT_NAMES = {
            ObsFileSystemProperties.ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "OBS_ENDPOINT"};

    @Override
    public boolean supports(Map<String, String> properties) {
        if (isExplicitObs(properties)) {
            return true;
        }
        String endpoint = firstPresent(properties, ENDPOINT_NAMES);
        return endpoint != null && endpoint.contains("myhuaweicloud.com");
    }

    @Override
    public ObsFileSystemProperties bind(Map<String, String> properties) {
        return ObsFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(ObsFileSystemProperties properties) throws IOException {
        return new ObsFileSystem(new ObsObjStorage(properties));
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "OBS";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(ObsFileSystemProperties.class);
    }

    private boolean isExplicitObs(Map<String, String> properties) {
        return STORAGE_TYPE_OBS.equalsIgnoreCase(properties.get(STORAGE_TYPE_KEY))
                || STORAGE_TYPE_OBS.equalsIgnoreCase(properties.get(PROVIDER_KEY))
                || Boolean.parseBoolean(properties.getOrDefault(FS_OBS_SUPPORT, "false"));
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
