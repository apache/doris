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

package org.apache.doris.connector;

import org.apache.doris.connector.api.ConnectorSession;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable implementation of {@link ConnectorSession}.
 *
 * <p>Instances are created via {@link ConnectorSessionBuilder}.
 */
public class ConnectorSessionImpl implements ConnectorSession {

    private final String queryId;
    private final String user;
    private final String timeZone;
    private final String locale;
    private final long catalogId;
    private final String catalogName;
    private final Map<String, String> catalogProperties;
    private final Map<String, String> sessionProperties;

    ConnectorSessionImpl(String queryId, String user, String timeZone, String locale,
            long catalogId, String catalogName, Map<String, String> catalogProperties,
            Map<String, String> sessionProperties) {
        this.queryId = queryId != null ? queryId : "";
        this.user = user != null ? user : "";
        this.timeZone = timeZone != null ? timeZone : "UTC";
        this.locale = locale != null ? locale : "en_US";
        this.catalogId = catalogId;
        this.catalogName = catalogName != null ? catalogName : "";
        this.catalogProperties = catalogProperties != null
                ? Collections.unmodifiableMap(catalogProperties) : Collections.emptyMap();
        this.sessionProperties = sessionProperties != null
                ? Collections.unmodifiableMap(sessionProperties) : Collections.emptyMap();
    }

    @Override
    public String getQueryId() {
        return queryId;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getTimeZone() {
        return timeZone;
    }

    @Override
    public String getLocale() {
        return locale;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String name, Class<T> type) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        // Session properties take precedence over catalog properties.
        String value = sessionProperties.get(name);
        if (value == null) {
            value = catalogProperties.get(name);
        }
        if (value == null) {
            return null;
        }
        if (type == String.class) {
            return (T) value;
        }
        if (type == Integer.class || type == int.class) {
            return (T) Integer.valueOf(value);
        }
        if (type == Long.class || type == long.class) {
            return (T) Long.valueOf(value);
        }
        if (type == Boolean.class || type == boolean.class) {
            return (T) Boolean.valueOf(value);
        }
        throw new IllegalArgumentException(
                "Unsupported property type: " + type.getName());
    }

    @Override
    public Map<String, String> getCatalogProperties() {
        return catalogProperties;
    }

    @Override
    public Map<String, String> getSessionProperties() {
        return sessionProperties;
    }

    @Override
    public String toString() {
        return "ConnectorSession{queryId='" + queryId
                + "', user='" + user
                + "', catalogName='" + catalogName + "'}";
    }
}
