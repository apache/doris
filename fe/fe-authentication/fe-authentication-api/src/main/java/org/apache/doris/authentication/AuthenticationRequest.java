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
 * Protocol-agnostic, encryption-agnostic authentication request.
 *
 * <p>This class represents an authentication request that has been constructed
 * by the Protocol Adapter Layer from protocol-specific data and normalized
 * into a common format.
 *
 * <p>The request contains:
 * <ul>
 *   <li>Core fields - username, credential (abstract), source info</li>
 *   <li>Target info - database, catalog</li>
 *   <li>Client info - client type</li>
 *   <li>Extension properties - for business layer extensions</li>
 * </ul>
 *
 * <p>Key design improvements:
 * <ul>
 *   <li>Removed protocol-specific fields (challenge, authState) - handled by Protocol Adapter</li>
 *   <li>Uses Credential interface for type safety instead of byte[] + credentialType</li>
 *   <li>Simplified extension properties - only for business layer, not protocol details</li>
 * </ul>
 *
 * <p>Use the {@link Builder} to construct instances.
 */
public final class AuthenticationRequest {

    // === Core fields (required for all authentication) ===
    private final String username;
    private final CredentialType credentialType;
    private final byte[] credential;
    private final String remoteHost;
    private final int remotePort;

    // === Target info ===
    private final String database;
    private final String catalog;

    // === Client info ===
    private final String clientType;

    // === Extension properties (business layer, not protocol-specific) ===
    private final Map<String, Object> properties;

    private AuthenticationRequest(Builder builder) {
        this.username = Objects.requireNonNull(builder.username, "username is required");
        this.credentialType = Objects.requireNonNull(builder.credentialType, "credentialType is required");
        this.credential = builder.credential;
        this.remoteHost = builder.remoteHost;
        this.remotePort = builder.remotePort;
        this.database = builder.database;
        this.catalog = builder.catalog;
        this.clientType = builder.clientType;
        this.properties = builder.properties != null
                ? Collections.unmodifiableMap(new HashMap<>(builder.properties))
                : Collections.emptyMap();
    }

    /**
     * Returns the username attempting to authenticate.
     *
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Returns the credential type.
     *
     * @return the credential type
     */
    public CredentialType getCredentialType() {
        return credentialType;
    }

    /**
     * Returns the credential data.
     * The format depends on the credential type.
     *
     * @return credential bytes, may be null
     */
    public byte[] getCredential() {
        return credential;
    }

    /**
     * Returns the client IP address.
     *
     * @return remote host IP, may be null
     */
    public String getRemoteHost() {
        return remoteHost;
    }

    /**
     * Returns the client port.
     *
     * @return remote port
     */
    public int getRemotePort() {
        return remotePort;
    }

    /**
     * Returns the target database if specified.
     *
     * @return optional database name
     */
    public Optional<String> getDatabase() {
        return Optional.ofNullable(database);
    }

    /**
     * Returns the target catalog if specified.
     *
     * @return optional catalog name
     */
    public Optional<String> getCatalog() {
        return Optional.ofNullable(catalog);
    }

    /**
     * Returns the client type identifier (e.g., "jdbc", "cli", "odbc", "python").
     *
     * @return client type, may be null
     */
    public String getClientType() {
        return clientType;
    }

    /**
     * Returns additional properties.
     *
     * @return immutable properties map
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * Gets a property value.
     *
     * @param key the property key
     * @return optional property value
     */
    public Optional<Object> getProperty(String key) {
        return Optional.ofNullable(properties.get(key));
    }

    /**
     * Gets a string property value.
     *
     * @param key the property key
     * @return optional string value
     */
    public Optional<String> getStringProperty(String key) {
        Object value = properties.get(key);
        return value instanceof String ? Optional.of((String) value) : Optional.empty();
    }

    @Override
    public String toString() {
        return "AuthenticationRequest{"
                + "username='" + username + '\''
                + ", credentialType=" + credentialType
                + ", remoteHost='" + remoteHost + '\''
                + ", clientType='" + clientType + '\''
                + '}';
    }

    /**
     * Creates a new builder for AuthenticationRequest.
     *
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link AuthenticationRequest}.
     */
    public static final class Builder {
        private String username;
        private CredentialType credentialType;
        private byte[] credential;
        private String remoteHost;
        private int remotePort;
        private String database;
        private String catalog;
        private String clientType;
        private Map<String, Object> properties;

        private Builder() {
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder credentialType(CredentialType credentialType) {
            this.credentialType = credentialType;
            return this;
        }

        public Builder credential(byte[] credential) {
            this.credential = credential;
            return this;
        }

        public Builder remoteHost(String remoteHost) {
            this.remoteHost = remoteHost;
            return this;
        }

        public Builder remotePort(int remotePort) {
            this.remotePort = remotePort;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder clientType(String clientType) {
            this.clientType = clientType;
            return this;
        }

        public Builder properties(Map<String, Object> properties) {
            this.properties = properties;
            return this;
        }

        public Builder property(String key, Object value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        public AuthenticationRequest build() {
            return new AuthenticationRequest(this);
        }
    }
}
