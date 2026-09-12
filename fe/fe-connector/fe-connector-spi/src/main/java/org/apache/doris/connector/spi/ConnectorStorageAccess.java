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

package org.apache.doris.connector.spi;

import org.apache.doris.filesystem.properties.BackendStorageKind;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One location resolved against one request-local storage binding and credential generation.
 *
 * <p>Consumers must use its URI, reader and backend properties together, without merging another
 * catalog-wide credential map. The reader is a Thrift enum name so this SPI remains RPC-neutral.
 * This value contains execution credentials: keep it request-local and never publish it in metadata,
 * profiles or logs. Its diagnostic representation deliberately omits both the URI and properties.
 */
public final class ConnectorStorageAccess {
    private final String providerName;
    private final String normalizedUri;
    private final BackendStorageKind backendKind;
    private final String backendFileType;
    private final Map<String, String> backendProperties;

    public ConnectorStorageAccess(String providerName, String normalizedUri, BackendStorageKind backendKind,
            String backendFileType, Map<String, String> backendProperties) {
        this.providerName = Objects.requireNonNull(providerName, "providerName is required");
        this.normalizedUri = Objects.requireNonNull(normalizedUri, "normalizedUri is required");
        this.backendKind = Objects.requireNonNull(backendKind, "backendKind is required");
        this.backendFileType = Objects.requireNonNull(backendFileType, "backendFileType is required");
        this.backendProperties = Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(backendProperties, "backendProperties is required")));
    }

    public String getProviderName() {
        return providerName;
    }

    public String getNormalizedUri() {
        return normalizedUri;
    }

    public BackendStorageKind getBackendKind() {
        return backendKind;
    }

    public String getBackendFileType() {
        return backendFileType;
    }

    public Map<String, String> getBackendProperties() {
        return backendProperties;
    }

    @Override
    public String toString() {
        return "ConnectorStorageAccess{provider=" + providerName + ", backendKind=" + backendKind
                + ", backendFileType=" + backendFileType + ", location/credentials=<redacted>}";
    }
}
