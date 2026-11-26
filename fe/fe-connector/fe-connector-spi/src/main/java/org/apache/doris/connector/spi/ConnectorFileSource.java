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

import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What a file table function names when its format belongs to a plugin: the location and the storage to read it
 * with. The engine builds one for each table function; see {@link ConnectorFileSourceOps}.
 *
 * <p>The storage carries the user's credentials. {@link #toString()} leaves it out, and so must anything the
 * connector derives from it for display.</p>
 */
public final class ConnectorFileSource {
    private final String format;
    private final String location;
    private final List<StorageProperties> storageProperties;
    private final Map<String, String> backendStorageProperties;

    public ConnectorFileSource(String format, String location, List<StorageProperties> storageProperties,
            Map<String, String> backendStorageProperties) {
        this.format = Objects.requireNonNull(format, "format");
        this.location = Objects.requireNonNull(location, "location");
        this.storageProperties = Collections.unmodifiableList(new ArrayList<>(storageProperties));
        this.backendStorageProperties = Collections.unmodifiableMap(new LinkedHashMap<>(backendStorageProperties));
    }

    /** The format, in lower case, as the plugin declares it. */
    public String getFormat() {
        return format;
    }

    /**
     * The location the user gave, as the engine's storage binding normalizes it: for {@code s3()} an
     * {@code s3://bucket/path} URI, whichever object-store scheme the user wrote.
     */
    public String getLocation() {
        return location;
    }

    /** The storage the location is read with, bound from the table function's properties. */
    public List<StorageProperties> getStorageProperties() {
        return storageProperties;
    }

    /**
     * The same storage in the BE's spelling, as the engine sends it with every file scan: a connector that
     * plans the scan passes it on under {@code ScanNodePropertyKeys.LOCATION_PREFIX}.
     */
    public Map<String, String> getBackendStorageProperties() {
        return backendStorageProperties;
    }

    @Override
    public String toString() {
        return "ConnectorFileSource{format=" + format + ", location=" + location + "}";
    }
}
