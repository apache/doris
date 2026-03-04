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

package org.apache.doris.authentication;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Persistent metadata for AUTHENTICATION INTEGRATION.
 */
public class AuthenticationIntegrationMeta implements Writable {
    public static final String TYPE_PROPERTY = "type";

    @SerializedName(value = "n")
    private String name;
    @SerializedName(value = "t")
    private String type;
    @SerializedName(value = "p")
    private Map<String, String> properties;
    @SerializedName(value = "c")
    private String comment;

    private AuthenticationIntegrationMeta() {
        this.name = "";
        this.type = "";
        this.properties = Collections.emptyMap();
        this.comment = null;
    }

    public AuthenticationIntegrationMeta(String name, String type, Map<String, String> properties, String comment) {
        this.name = Objects.requireNonNull(name, "name can not be null");
        this.type = Objects.requireNonNull(type, "type can not be null");
        this.properties = Collections.unmodifiableMap(
                new LinkedHashMap<>(Objects.requireNonNull(properties, "properties can not be null")));
        this.comment = comment;
    }

    /**
     * Build metadata from CREATE SQL arguments.
     */
    public static AuthenticationIntegrationMeta fromCreateSql(
            String integrationName, Map<String, String> properties, String comment) throws DdlException {
        if (properties == null || properties.isEmpty()) {
            throw new DdlException("Property 'type' is required in CREATE AUTHENTICATION INTEGRATION");
        }
        String type = null;
        Map<String, String> copiedProperties = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = Objects.requireNonNull(entry.getKey(), "property key can not be null");
            if (TYPE_PROPERTY.equalsIgnoreCase(key)) {
                if (type != null) {
                    throw new DdlException("Property 'type' is duplicated in CREATE AUTHENTICATION INTEGRATION");
                }
                type = entry.getValue();
                continue;
            }
            copiedProperties.put(key, entry.getValue());
        }
        if (type == null || type.isEmpty()) {
            throw new DdlException("Property 'type' is required in CREATE AUTHENTICATION INTEGRATION");
        }
        return new AuthenticationIntegrationMeta(integrationName, type, copiedProperties, comment);
    }

    /**
     * Build a new metadata object after ALTER ... SET PROPERTIES.
     */
    public AuthenticationIntegrationMeta withAlterProperties(Map<String, String> propertiesDelta) throws DdlException {
        if (propertiesDelta == null || propertiesDelta.isEmpty()) {
            throw new DdlException("ALTER AUTHENTICATION INTEGRATION should contain at least one property");
        }
        for (String key : propertiesDelta.keySet()) {
            if (TYPE_PROPERTY.equalsIgnoreCase(key)) {
                throw new DdlException("ALTER AUTHENTICATION INTEGRATION does not allow modifying property 'type'");
            }
        }
        Map<String, String> mergedProperties = new LinkedHashMap<>(properties);
        mergedProperties.putAll(propertiesDelta);
        return new AuthenticationIntegrationMeta(name, type, mergedProperties, comment);
    }

    /**
     * Build a new metadata object after ALTER ... UNSET PROPERTIES.
     */
    public AuthenticationIntegrationMeta withUnsetProperties(Set<String> propertiesToUnset) throws DdlException {
        if (propertiesToUnset == null || propertiesToUnset.isEmpty()) {
            throw new DdlException("ALTER AUTHENTICATION INTEGRATION should contain at least one property");
        }
        Map<String, String> reducedProperties = new LinkedHashMap<>(properties);
        for (String key : propertiesToUnset) {
            if (TYPE_PROPERTY.equalsIgnoreCase(key)) {
                throw new DdlException("ALTER AUTHENTICATION INTEGRATION does not allow modifying property 'type'");
            }
            reducedProperties.remove(key);
        }
        return new AuthenticationIntegrationMeta(name, type, reducedProperties, comment);
    }

    public AuthenticationIntegrationMeta withComment(String newComment) {
        return new AuthenticationIntegrationMeta(name, type, properties, newComment);
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> toSqlPropertiesView() {
        Map<String, String> allProperties = new LinkedHashMap<>();
        allProperties.put(TYPE_PROPERTY, type);
        allProperties.putAll(properties);
        return allProperties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AuthenticationIntegrationMeta read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AuthenticationIntegrationMeta.class);
    }
}
