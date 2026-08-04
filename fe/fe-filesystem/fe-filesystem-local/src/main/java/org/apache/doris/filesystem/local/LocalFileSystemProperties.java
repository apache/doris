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

package org.apache.doris.filesystem.local;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Thin typed-property model for local-filesystem storage. fe-core's LocalProperties is a
 * raw-map holder (backend map == original props, uri == file_path); this mirrors it so the
 * registry's bindPrimary/bindAll can construct every storage type uniformly.
 */
public final class LocalFileSystemProperties implements FileSystemProperties {

    public static final String PROP_FILE_PATH = "file_path";

    private final Map<String, String> rawProperties;

    LocalFileSystemProperties(Map<String, String> rawProperties) {
        this.rawProperties = Collections.unmodifiableMap(new HashMap<>(rawProperties));
    }

    @Override
    public String providerName() {
        return "LOCAL";
    }

    @Override
    public StorageKind kind() {
        return StorageKind.LOCAL;
    }

    @Override
    public FileSystemType type() {
        return FileSystemType.FILE;
    }

    @Override
    public Map<String, String> rawProperties() {
        return rawProperties;
    }

    @Override
    public Map<String, String> matchedProperties() {
        return Collections.emptyMap();
    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProperties) {
        return loadProperties == null ? null : loadProperties.get(PROP_FILE_PATH);
    }

    @Override
    public String storageFamilyName() {
        return "local";
    }

}
