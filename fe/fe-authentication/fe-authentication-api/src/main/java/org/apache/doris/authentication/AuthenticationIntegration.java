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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Authentication Integration - represents a configured authentication instance.
 *
 * <p>An Integration is a named configuration for an authentication plugin.
 * Multiple Integrations can use the same plugin type with different configurations.
 *
 * <p>Example SQL:
 * <pre>{@code
 * -- Create two LDAP integrations with different configurations
 * CREATE AUTHENTICATION INTEGRATION corp_ldap
 *   TYPE = 'ldap'
 *   WITH (
 *     'server' = 'ldap://corp.example.com:389',
 *     'base_dn' = 'dc=corp,dc=example,dc=com'
 *   );
 *
 * CREATE AUTHENTICATION INTEGRATION partner_ldap
 *   TYPE = 'ldap'
 *   WITH (
 *     'server' = 'ldap://partner.example.com:389',
 *     'base_dn' = 'dc=partner,dc=example,dc=com'
 *   );
 * }</pre>
 *
 * <p>Design principles:
 * <ul>
 *   <li>Integration name is globally unique</li>
 *   <li>Type corresponds to plugin name ("ldap", "oidc", etc.)</li>
 *   <li>Properties are plugin-specific configuration</li>
 *   <li>Immutable after creation (use builder for construction)</li>
 * </ul>
 */
public final class AuthenticationIntegration {

    /** Integration name (globally unique) */
    private final String name;

    /** Plugin type (must match a registered plugin name) */
    private final String type;

    /** Plugin-specific configuration properties */
    private final Map<String, String> properties;

    /** Optional comment/description */
    private final String comment;

    private AuthenticationIntegration(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "name is required");
        this.type = Objects.requireNonNull(builder.type, "type is required");
        this.properties = Collections.unmodifiableMap(new HashMap<>(builder.properties));
        this.comment = builder.comment;
    }

    /**
     * Returns the integration name.
     *
     * @return integration name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the plugin type.
     *
     * @return plugin type (e.g., "ldap", "oidc")
     */
    public String getType() {
        return type;
    }

    /**
     * Returns all configuration properties.
     *
     * @return immutable map of properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Gets a property value.
     *
     * @param key property key
     * @return optional property value
     */
    public Optional<String> getProperty(String key) {
        return Optional.ofNullable(properties.get(key));
    }

    /**
     * Gets a property value with default.
     *
     * @param key property key
     * @param defaultValue default value if not found
     * @return property value or default
     */
    public String getProperty(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    /**
     * Returns the optional comment.
     *
     * @return optional comment
     */
    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuthenticationIntegration that = (AuthenticationIntegration) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "AuthenticationIntegration{"
                + "name='" + name + '\''
                + ", type='" + type + '\''
                + ", properties=" + properties.size() + " entries"
                + '}';
    }

    /**
     * Creates a new builder.
     *
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder initialized with values from this integration.
     *
     * @return builder with copied values
     */
    public Builder toBuilder() {
        return new Builder()
                .name(name)
                .type(type)
                .properties(properties)
                .comment(comment);
    }

    /**
     * Builder for {@link AuthenticationIntegration}.
     */
    public static final class Builder {
        private String name;
        private String type;
        private Map<String, String> properties = new HashMap<>();
        private String comment;

        private Builder() {
        }

        /**
         * Sets the integration name.
         *
         * @param name integration name
         * @return this builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the plugin type.
         *
         * @param type plugin type
         * @return this builder
         */
        public Builder type(String type) {
            this.type = type;
            return this;
        }

        /**
         * Sets all properties.
         *
         * @param properties configuration properties
         * @return this builder
         */
        public Builder properties(Map<String, String> properties) {
            this.properties = properties != null ? new HashMap<>(properties) : new HashMap<>();
            return this;
        }

        /**
         * Adds a single property.
         *
         * @param key property key
         * @param value property value
         * @return this builder
         */
        public Builder property(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        /**
         * Sets the comment.
         *
         * @param comment comment text
         * @return this builder
         */
        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * Builds the AuthenticationIntegration.
         *
         * @return built integration
         * @throws NullPointerException if name or type is null
         */
        public AuthenticationIntegration build() {
            return new AuthenticationIntegration(this);
        }
    }
}
