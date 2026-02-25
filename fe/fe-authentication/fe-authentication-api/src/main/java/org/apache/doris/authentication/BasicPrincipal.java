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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Basic implementation of {@link Principal}.
 *
 * <p>This is the standard principal implementation returned by authenticators
 * after successful authentication.
 *
 * <p>Use the {@link Builder} to construct instances:
 * <pre>{@code
 * Principal principal = BasicPrincipal.builder()
 *     .name("alice")
 *     .authenticator("ldap")
 *     .externalPrincipal("cn=alice,ou=users,dc=example,dc=com")
 *     .externalGroups(Set.of("developers", "analysts"))
 *     .attribute("email", "alice@example.com")
 *     .build();
 * }</pre>
 */
public final class BasicPrincipal implements Principal {

    private final String name;
    private final String authenticator;
    private final String externalPrincipal;
    private final Set<String> externalGroups;
    private final Map<String, String> attributes;
    private final boolean servicePrincipal;

    private BasicPrincipal(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "name is required");
        this.authenticator = Objects.requireNonNull(builder.authenticator, "authenticator is required");
        this.externalPrincipal = builder.externalPrincipal;
        this.externalGroups = Collections.unmodifiableSet(new HashSet<>(builder.externalGroups));
        this.attributes = Collections.unmodifiableMap(new HashMap<>(builder.attributes));
        this.servicePrincipal = builder.servicePrincipal;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getAuthenticator() {
        return authenticator;
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

    @Override
    public boolean isServicePrincipal() {
        return servicePrincipal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BasicPrincipal that = (BasicPrincipal) o;
        return Objects.equals(name, that.name)
                && Objects.equals(authenticator, that.authenticator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, authenticator);
    }

    @Override
    public String toString() {
        return "BasicPrincipal{"
                + "name='" + name + '\''
                + ", authenticator='" + authenticator + '\''
                + (externalPrincipal != null ? ", externalPrincipal='" + externalPrincipal + '\'' : "")
                + '}';
    }

    /**
     * Creates a new builder for BasicPrincipal.
     *
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder initialized with values from an existing principal.
     *
     * @param principal the principal to copy from
     * @return new builder instance with copied values
     */
    public static Builder builder(Principal principal) {
        return new Builder()
                .name(principal.getName())
                .authenticator(principal.getAuthenticator())
                .externalPrincipal(principal.getExternalPrincipal().orElse(null))
                .externalGroups(principal.getExternalGroups())
                .attributes(principal.getAttributes())
                .servicePrincipal(principal.isServicePrincipal());
    }

    /**
     * Builder for {@link BasicPrincipal}.
     */
    public static final class Builder {
        private String name;
        private String authenticator;
        private String externalPrincipal;
        private Set<String> externalGroups = new HashSet<>();
        private Map<String, String> attributes = new HashMap<>();
        private boolean servicePrincipal = false;

        private Builder() {
        }

        /**
         * Sets the principal name (username).
         *
         * @param name the username
         * @return this builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the authenticator name.
         *
         * @param authenticator the authenticator name
         * @return this builder
         */
        public Builder authenticator(String authenticator) {
            this.authenticator = authenticator;
            return this;
        }

        /**
         * Sets the external principal identifier.
         *
         * @param externalPrincipal the external principal (e.g., LDAP DN)
         * @return this builder
         */
        public Builder externalPrincipal(String externalPrincipal) {
            this.externalPrincipal = externalPrincipal;
            return this;
        }

        /**
         * Sets the external groups.
         *
         * @param externalGroups set of external group names
         * @return this builder
         */
        public Builder externalGroups(Set<String> externalGroups) {
            this.externalGroups = externalGroups != null ? externalGroups : new HashSet<>();
            return this;
        }

        /**
         * Adds an external group.
         *
         * @param group the group name
         * @return this builder
         */
        public Builder addExternalGroup(String group) {
            this.externalGroups.add(group);
            return this;
        }

        /**
         * Sets all attributes.
         *
         * @param attributes map of attributes
         * @return this builder
         */
        public Builder attributes(Map<String, String> attributes) {
            this.attributes = attributes != null ? attributes : new HashMap<>();
            return this;
        }

        /**
         * Adds a single attribute.
         *
         * @param key attribute key
         * @param value attribute value
         * @return this builder
         */
        public Builder attribute(String key, String value) {
            this.attributes.put(key, value);
            return this;
        }

        /**
         * Sets whether this is a service principal (non-human / machine account).
         *
         * <p>This flag is intended to distinguish service accounts from human users
         * for policy and audit purposes. It does NOT refer to Kerberos "service
         * principal" names.</p>
         *
         * @param servicePrincipal true if service account
         * @return this builder
         */
        public Builder servicePrincipal(boolean servicePrincipal) {
            this.servicePrincipal = servicePrincipal;
            return this;
        }

        /**
         * Builds the BasicPrincipal.
         *
         * @return the built principal
         * @throws NullPointerException if name or authenticator is null
         */
        public BasicPrincipal build() {
            return new BasicPrincipal(this);
        }
    }
}
