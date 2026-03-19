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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.mysql.authenticate.password.Password;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AuthenticateRequest {
    private final String userName;
    // TODO(authentication): remove password compatibility field after all authenticators
    // consume generic credentials directly.
    private final Password password;
    private final String remoteHost;
    private final int remotePort;
    private final String clientType;
    private final String credentialType;
    private final byte[] credential;
    private final Map<String, Object> properties;

    public AuthenticateRequest(String userName, Password password, String remoteIp) {
        this(builder()
                .userName(userName)
                .password(password)
                .remoteHost(remoteIp));
    }

    private AuthenticateRequest(Builder builder) {
        this.userName = Objects.requireNonNull(builder.userName, "userName is required");
        this.password = builder.password;
        this.remoteHost = builder.remoteHost;
        this.remotePort = builder.remotePort;
        this.clientType = builder.clientType;
        this.credentialType = builder.credentialType;
        this.credential = builder.credential == null ? null : builder.credential.clone();
        this.properties = builder.properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(builder.properties));
    }

    public String getUserName() {
        return userName;
    }

    public Password getPassword() {
        return password;
    }

    public String getRemoteIp() {
        return remoteHost;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public int getRemotePort() {
        return remotePort;
    }

    public String getClientType() {
        return clientType;
    }

    public String getCredentialType() {
        return credentialType;
    }

    public byte[] getCredential() {
        return credential == null ? null : credential.clone();
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String userName;
        private Password password;
        private String remoteHost;
        private int remotePort;
        private String clientType;
        private String credentialType;
        private byte[] credential;
        private Map<String, Object> properties;

        private Builder() {
        }

        public Builder userName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder password(Password password) {
            this.password = password;
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

        public Builder clientType(String clientType) {
            this.clientType = clientType;
            return this;
        }

        public Builder credentialType(String credentialType) {
            this.credentialType = credentialType;
            return this;
        }

        public Builder credential(byte[] credential) {
            this.credential = credential == null ? null : credential.clone();
            return this;
        }

        public Builder properties(Map<String, Object> properties) {
            this.properties = properties == null ? null : new HashMap<>(properties);
            return this;
        }

        public Builder property(String key, Object value) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(key, value);
            return this;
        }

        public AuthenticateRequest build() {
            return new AuthenticateRequest(this);
        }
    }
}
