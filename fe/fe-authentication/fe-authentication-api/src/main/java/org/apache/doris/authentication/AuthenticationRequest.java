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

import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Protocol-agnostic authentication request.
 *
 * <p>This class represents an authentication request that has been extracted
 * from protocol-specific packets (MySQL, PostgreSQL, HTTP, etc.) and normalized
 * into a common format.
 *
 * <p>The request contains:
 * <ul>
 *   <li>User identification - username</li>
 *   <li>Credentials - password, token, certificate, etc.</li>
 *   <li>Connection info - source IP, client type</li>
 *   <li>Context - target database, catalog</li>
 *   <li>Authentication state - for multi-step auth flows</li>
 * </ul>
 *
 * <p>Use the {@link Builder} to construct instances.
 */
public final class AuthenticationRequest {

    private final String username;
    private final String requestedProfile;
    private final CredentialType credentialType;
    private final byte[] credential;
    private final String sourceIp;
    private final int sourcePort;
    private final String database;
    private final String catalog;
    private final byte[] challenge;
    private final X509Certificate[] clientCertificates;
    private final String clientType;
    private final String protocol;
    private final Object authState;
    private final Map<String, String> properties;

    private AuthenticationRequest(Builder builder) {
        this.username = Objects.requireNonNull(builder.username, "username is required");
        this.requestedProfile = builder.requestedProfile;
        this.credentialType = Objects.requireNonNull(builder.credentialType, "credentialType is required");
        this.credential = builder.credential;
        this.sourceIp = builder.sourceIp;
        this.sourcePort = builder.sourcePort;
        this.database = builder.database;
        this.catalog = builder.catalog;
        this.challenge = builder.challenge;
        this.clientCertificates = builder.clientCertificates;
        this.clientType = builder.clientType;
        this.protocol = builder.protocol;
        this.authState = builder.authState;
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
     * Returns the explicitly requested authentication profile name, if any.
     *
     * @return optional profile name
     */
    public Optional<String> getRequestedProfile() {
        return Optional.ofNullable(requestedProfile);
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
     * @return source IP, may be null
     */
    public String getSourceIp() {
        return sourceIp;
    }

    /**
     * Returns the client port.
     *
     * @return source port
     */
    public int getSourcePort() {
        return sourcePort;
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
     * Returns the challenge data for challenge-response authentication.
     *
     * @return optional challenge bytes
     */
    public Optional<byte[]> getChallenge() {
        return Optional.ofNullable(challenge);
    }

    /**
     * Returns the client certificate chain for certificate authentication.
     *
     * @return optional certificate chain
     */
    public Optional<X509Certificate[]> getClientCertificates() {
        return Optional.ofNullable(clientCertificates);
    }

    /**
     * Returns the client type identifier (e.g., "mysql-connector-java").
     *
     * @return client type, may be null
     */
    public String getClientType() {
        return clientType;
    }

    /**
     * Returns the protocol identifier (e.g., "mysql", "http").
     *
     * @return protocol identifier, may be null
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Returns the authentication state for multi-step authentication.
     *
     * @return optional auth state
     */
    public Optional<Object> getAuthState() {
        return Optional.ofNullable(authState);
    }

    /**
     * Returns additional properties.
     *
     * @return immutable properties map
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Gets a property value.
     *
     * @param key the property key
     * @return optional property value
     */
    public Optional<String> getProperty(String key) {
        return Optional.ofNullable(properties.get(key));
    }

    @Override
    public String toString() {
        return "AuthenticationRequest{"
                + "username='" + username + '\''
                + (requestedProfile != null ? ", requestedProfile='" + requestedProfile + '\'' : "")
                + ", credentialType=" + credentialType
                + ", sourceIp='" + sourceIp + '\''
                + ", protocol='" + protocol + '\''
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
        private String requestedProfile;
        private CredentialType credentialType;
        private byte[] credential;
        private String sourceIp;
        private int sourcePort;
        private String database;
        private String catalog;
        private byte[] challenge;
        private X509Certificate[] clientCertificates;
        private String clientType;
        private String protocol;
        private Object authState;
        private Map<String, String> properties;

        private Builder() {
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder requestedProfile(String requestedProfile) {
            this.requestedProfile = requestedProfile;
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

        public Builder sourceIp(String sourceIp) {
            this.sourceIp = sourceIp;
            return this;
        }

        public Builder sourcePort(int sourcePort) {
            this.sourcePort = sourcePort;
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

        public Builder challenge(byte[] challenge) {
            this.challenge = challenge;
            return this;
        }

        public Builder clientCertificates(X509Certificate[] clientCertificates) {
            this.clientCertificates = clientCertificates;
            return this;
        }

        public Builder clientType(String clientType) {
            this.clientType = clientType;
            return this;
        }

        public Builder protocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder authState(Object authState) {
            this.authState = authState;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder property(String key, String value) {
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
