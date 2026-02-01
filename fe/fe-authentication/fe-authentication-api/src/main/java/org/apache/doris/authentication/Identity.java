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

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Authenticated identity information.
 *
 * <p>This class implements {@link Principal} and represents the result of
 * successful authentication. It contains both the standard principal information
 * and additional metadata like expiration time and authenticator configuration name.
 *
 * <p>Design note: Identity extends the Principal concept with:
 * <ul>
 *   <li>Expiration time - for session/token management</li>
 *   <li>Authenticator config name - for audit and debugging</li>
 *   <li>Authenticator type - for plugin identification</li>
 * </ul>
 *
 * <p>Use the {@link Builder} to construct instances.
 *
 * @see Principal
 * @see BasicPrincipal
 */
public final class Identity implements Principal {

    private final String username;
    private final String authenticatorName;
    private final AuthenticationPluginType authenticatorType;
    private final String authenticatorConfigName;
    private final String externalPrincipal;
    private final Set<String> externalGroups;
    private final Instant expiresAt;
    private final Map<String, String> attributes;

    private Identity(Builder builder) {
        this.username = Objects.requireNonNull(builder.username, "username is required");
        this.authenticatorName = Objects.requireNonNull(builder.authenticatorName, "authenticatorName is required");
        this.authenticatorType = Objects.requireNonNull(builder.authenticatorType, "authenticatorType is required");
        this.authenticatorConfigName = builder.authenticatorConfigName;
        this.externalPrincipal = builder.externalPrincipal;
        this.externalGroups = Collections.unmodifiableSet(new HashSet<>(builder.externalGroups));
        this.expiresAt = builder.expiresAt;
        this.attributes = Collections.unmodifiableMap(new HashMap<>(builder.attributes));
    }

    // ==================== Principal Interface ====================

    @Override
    public String getName() {
        return username;
    }

    @Override
    public String getAuthenticator() {
        return authenticatorName;
    }

    @Override
    public Optional<String> getExternalPrincipal() {
        return Optional.ofNullable(externalPrincipal);
    }

    @Override
    public Set<String> getExternalGroups() {
        return externalGroups;
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
    }

    // ==================== Identity-specific Methods ====================

    /**
     * Returns the Doris username.
     * This is an alias for {@link #getName()}.
     *
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Returns the authenticator plugin type.
     *
     * @return the authenticator type
     */
    public AuthenticationPluginType getAuthenticatorType() {
        return authenticatorType;
    }

    /**
     * Returns the authenticator configuration name.
     * This is the name of the AuthenticatorConfig that was used.
     *
     * @return optional authenticator config name
     */
    public Optional<String> getAuthenticatorConfigName() {
        return Optional.ofNullable(authenticatorConfigName);
    }

    /**
     * Returns the identity expiration time.
     * After this time, the identity should be re-validated.
     *
     * @return optional expiration time
     */
    public Optional<Instant> getExpiresAt() {
        return Optional.ofNullable(expiresAt);
    }

    /**
     * Checks if this identity has expired.
     *
     * @return true if expired, false if still valid or no expiration set
     */
    public boolean isExpired() {
        return expiresAt != null && Instant.now().isAfter(expiresAt);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Identity identity = (Identity) o;
        return Objects.equals(username, identity.username)
                && Objects.equals(authenticatorName, identity.authenticatorName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, authenticatorName);
    }

    @Override
    public String toString() {
        return "Identity{"
                + "username='" + username + '\''
                + ", authenticator='" + authenticatorName + '\''
                + (authenticatorConfigName != null ? ", config='" + authenticatorConfigName + '\'' : "")
                + (externalPrincipal != null ? ", externalPrincipal='" + externalPrincipal + '\'' : "")
                + '}';
    }

    /**
     * Creates a new builder for Identity.
     *
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link Identity}.
     */
    public static final class Builder {
        private String username;
        private String authenticatorName;
        private AuthenticationPluginType authenticatorType;
        private String authenticatorConfigName;
        private String externalPrincipal;
        private Set<String> externalGroups = new HashSet<>();
        private Instant expiresAt;
        private Map<String, String> attributes = new HashMap<>();

        private Builder() {
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder authenticatorName(String authenticatorName) {
            this.authenticatorName = authenticatorName;
            return this;
        }

        public Builder authenticatorType(AuthenticationPluginType authenticatorType) {
            this.authenticatorType = authenticatorType;
            return this;
        }

        public Builder authenticatorConfigName(String authenticatorConfigName) {
            this.authenticatorConfigName = authenticatorConfigName;
            return this;
        }

        public Builder externalPrincipal(String externalPrincipal) {
            this.externalPrincipal = externalPrincipal;
            return this;
        }

        public Builder externalGroups(Set<String> externalGroups) {
            this.externalGroups = externalGroups != null ? externalGroups : new HashSet<>();
            return this;
        }

        public Builder addExternalGroup(String group) {
            this.externalGroups.add(group);
            return this;
        }

        public Builder expiresAt(Instant expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }

        public Builder attributes(Map<String, String> attributes) {
            this.attributes = attributes != null ? attributes : new HashMap<>();
            return this;
        }

        public Builder attribute(String key, String value) {
            this.attributes.put(key, value);
            return this;
        }

        public Identity build() {
            return new Identity(this);
        }
    }
}
