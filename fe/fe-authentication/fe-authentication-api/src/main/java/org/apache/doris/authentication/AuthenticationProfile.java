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

/**
 * Authenticator configuration - defines a named authentication method configuration.
 *
 * <p>This class represents a configured instance of an authenticator plugin.
 * For example, you might have multiple LDAP configurations for different
 * organizational units:
 * <ul>
 *   <li>{@code corp_ldap} - for corporate LDAP</li>
 *   <li>{@code partner_ldap} - for partner LDAP</li>
 * </ul>
 *
 * <p>Design reference: Similar to Trino's authenticator configuration files,
 * where each authenticator is configured via a properties file with a unique name.
 *
 * <p>AuthenticationProfile represents a single authentication configuration instance,
 * conceptually equivalent to an "Authenticator Configuration".
 *
 * <p>Key concepts:
 * <ul>
 *   <li><b>name</b> - Unique identifier for this configuration</li>
 *   <li><b>pluginType</b> - The type of authenticator plugin to use</li>
 *   <li><b>config</b> - Plugin-specific configuration properties</li>
 *   <li><b>roleMapping</b> - External group to Doris role mapping</li>
 *   <li><b>jitUserEnabled</b> - Whether to auto-create users on first login</li>
 * </ul>
 *
 * <p>Example DDL:
 * <pre>{@code
 * CREATE AUTHENTICATOR corp_ldap
 *   TYPE = 'ldap'
 *   PROPERTIES (
 *     'ldap_server' = 'ldap://ldap.example.com:389',
 *     'ldap_base_dn' = 'dc=example,dc=com'
 *   )
 *   ROLE_MAPPING (
 *     'developers' = 'dev_role'
 *   );
 * }</pre>
 */
public final class AuthenticationProfile {

    /**
     * Default authenticator name for built-in password authentication.
     */
    public static final String DEFAULT_PASSWORD_AUTHENTICATOR = "__default_password__";

    private final String name;
    private final AuthenticationPluginType pluginType;
    private volatile boolean enabled;
    private final int priority;
    private final Map<String, String> config;
    private final Map<String, String> roleMapping;
    private final boolean jitUserEnabled;
    private final String comment;

    private AuthenticationProfile(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "name is required");
        this.pluginType = Objects.requireNonNull(builder.pluginType, "pluginType is required");
        this.enabled = builder.enabled;
        this.priority = builder.priority;
        this.config = Collections.unmodifiableMap(new HashMap<>(builder.config));
        this.roleMapping = Collections.unmodifiableMap(new HashMap<>(builder.roleMapping));
        this.jitUserEnabled = builder.jitUserEnabled;
        this.comment = builder.comment;
    }

    /**
     * Returns the unique name of this authenticator configuration.
     *
     * @return the configuration name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the authenticator plugin type.
     *
     * @return the plugin type
     */
    public AuthenticationPluginType getPluginType() {
        return pluginType;
    }

    /**
     * Returns whether this authenticator is enabled.
     *
     * @return true if enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether this authenticator is enabled.
     *
     * @param enabled true to enable
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the priority (lower value = higher priority).
     * Used when multiple authenticators could handle a request.
     *
     * @return the priority
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Returns the plugin configuration properties.
     *
     * @return immutable configuration map
     */
    public Map<String, String> getConfig() {
        return config;
    }

    /**
     * Gets a configuration value.
     *
     * @param key the configuration key
     * @return the value, or null if not set
     */
    public String getConfigValue(String key) {
        return config.get(key);
    }

    /**
     * Gets a configuration value with default.
     *
     * @param key the configuration key
     * @param defaultValue the default value if not set
     * @return the value, or defaultValue if not set
     */
    public String getConfigValue(String key, String defaultValue) {
        return config.getOrDefault(key, defaultValue);
    }

    /**
     * Returns the external group to Doris role mapping.
     *
     * @return immutable role mapping
     */
    public Map<String, String> getRoleMapping() {
        return roleMapping;
    }

    /**
     * Maps an external group to Doris role.
     *
     * @param externalGroup the external group name
     * @return the mapped Doris role, or null if no mapping
     */
    public String mapRole(String externalGroup) {
        return roleMapping.get(externalGroup);
    }

    /**
     * Returns whether JIT (Just-In-Time) user creation is enabled.
     * When enabled, users authenticated via this method are automatically
     * created in Doris if they don't exist.
     *
     * @return true if JIT user creation is enabled
     */
    public boolean isJitUserEnabled() {
        return jitUserEnabled;
    }

    /**
     * Returns the comment/description.
     *
     * @return the comment, may be null
     */
    public String getComment() {
        return comment;
    }

    /**
     * Checks if this is the default password authenticator.
     *
     * @return true if this is the default
     */
    public boolean isDefault() {
        return DEFAULT_PASSWORD_AUTHENTICATOR.equals(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuthenticationProfile that = (AuthenticationProfile) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "AuthenticationProfile{"
                + "name='" + name + '\''
                + ", type=" + pluginType
                + ", enabled=" + enabled
                + ", priority=" + priority
                + '}';
    }

    /**
     * Creates a new builder for AuthenticationProfile.
     *
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates the default password authenticator configuration.
     *
     * @return the default authenticator
     */
    public static AuthenticationProfile createDefault() {
        return builder()
                .name(DEFAULT_PASSWORD_AUTHENTICATOR)
                .pluginType(AuthenticationPluginType.PASSWORD)
                .enabled(true)
                .priority(Integer.MAX_VALUE)
                .comment("Default built-in password authentication")
                .build();
    }

    /**
     * Builder for {@link AuthenticationProfile}.
     */
    public static final class Builder {
        private String name;
        private AuthenticationPluginType pluginType;
        private boolean enabled = true;
        private int priority = 100;
        private Map<String, String> config = new HashMap<>();
        private Map<String, String> roleMapping = new HashMap<>();
        private boolean jitUserEnabled = false;
        private String comment;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder pluginType(AuthenticationPluginType pluginType) {
            this.pluginType = pluginType;
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder config(Map<String, String> config) {
            this.config = config != null ? config : new HashMap<>();
            return this;
        }

        public Builder configProperty(String key, String value) {
            this.config.put(key, value);
            return this;
        }

        public Builder roleMapping(Map<String, String> roleMapping) {
            this.roleMapping = roleMapping != null ? roleMapping : new HashMap<>();
            return this;
        }

        public Builder mapRole(String externalGroup, String dorisRole) {
            this.roleMapping.put(externalGroup, dorisRole);
            return this;
        }

        public Builder jitUserEnabled(boolean jitUserEnabled) {
            this.jitUserEnabled = jitUserEnabled;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public AuthenticationProfile build() {
            return new AuthenticationProfile(this);
        }
    }
}
