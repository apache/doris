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

package org.apache.doris.connector.api;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata for a database (schema) in a connector catalog.
 */
public final class ConnectorDatabaseMetadata {

    private final String name;
    private final Map<String, String> properties;

    public ConnectorDatabaseMetadata(String name,
            Map<String, String> properties) {
        this.name = Objects.requireNonNull(name, "name");
        this.properties = properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(properties);
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorDatabaseMetadata)) {
            return false;
        }
        ConnectorDatabaseMetadata that = (ConnectorDatabaseMetadata) o;
        return name.equals(that.name)
                && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, properties);
    }

    @Override
    public String toString() {
        return "ConnectorDatabaseMetadata{name='" + name + "'}";
    }
}
