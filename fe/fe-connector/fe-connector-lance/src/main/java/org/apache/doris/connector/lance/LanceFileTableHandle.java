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


package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A Lance dataset that a file table function names, as {@code s3("uri" = ..., "format" = "lance")}, pinned to the
 * version the statement reads. It carries the table function's storage: the options the FE and the BE open the
 * dataset with, and the properties of the BE's file system. {@link #toString()} shows neither.
 */
final class LanceFileTableHandle implements ConnectorTableHandle {
    private static final long serialVersionUID = 1L;

    private final String uri;
    private final long version;
    private final HashMap<String, String> storageOptions;
    private final HashMap<String, String> backendStorageProperties;

    LanceFileTableHandle(String uri, long version, Map<String, String> storageOptions,
            Map<String, String> backendStorageProperties) {
        this.uri = Objects.requireNonNull(uri, "uri");
        this.version = version;
        this.storageOptions = new HashMap<>(storageOptions);
        this.backendStorageProperties = new HashMap<>(backendStorageProperties);
    }

    String getUri() {
        return uri;
    }

    long getVersion() {
        return version;
    }

    /** The options both the FE and the BE open the dataset with. */
    Map<String, String> getStorageOptions() {
        return storageOptions;
    }

    /** The table function's storage in the BE's spelling, which the BE's file system layer reads. */
    Map<String, String> getBackendStorageProperties() {
        return backendStorageProperties;
    }

    @Override
    public String toString() {
        return "LanceFileTableHandle{version=" + version + "}";
    }
}
