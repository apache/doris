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

package org.apache.doris.datasource;

import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.Objects;
import java.util.OptionalLong;

public class DelegatedCredential {
    private final Type type;
    private final String token;
    private final Long expiresAtMillis;

    public DelegatedCredential(Type type, String token) {
        this(type, token, OptionalLong.empty());
    }

    public DelegatedCredential(Type type, String token, OptionalLong expiresAtMillis) {
        this.type = Objects.requireNonNull(type, "type is required");
        this.token = Objects.requireNonNull(token, "token is required");
        Objects.requireNonNull(expiresAtMillis, "expiresAtMillis is required");
        this.expiresAtMillis = expiresAtMillis.isPresent() ? expiresAtMillis.getAsLong() : null;
    }

    public Type getType() {
        return type;
    }

    public String getToken() {
        return token;
    }

    public String getIcebergCredentialKey() {
        return type.getIcebergCredentialKey();
    }

    public OptionalLong getExpiresAtMillis() {
        return expiresAtMillis == null ? OptionalLong.empty() : OptionalLong.of(expiresAtMillis);
    }

    public boolean isExpired(long currentTimeMillis) {
        return expiresAtMillis != null && currentTimeMillis >= expiresAtMillis;
    }

    @Override
    public String toString() {
        return "DelegatedCredential{"
                + "type=" + type
                + ", token=<redacted>"
                + ", expiresAtMillis=" + expiresAtMillis
                + '}';
    }

    public enum Type {
        ACCESS_TOKEN(OAuth2Properties.TOKEN),
        ID_TOKEN(OAuth2Properties.ID_TOKEN_TYPE),
        JWT(OAuth2Properties.JWT_TOKEN_TYPE),
        SAML(OAuth2Properties.SAML2_TOKEN_TYPE);

        private final String icebergCredentialKey;

        Type(String icebergCredentialKey) {
            this.icebergCredentialKey = icebergCredentialKey;
        }

        public String getIcebergCredentialKey() {
            return icebergCredentialKey;
        }
    }
}
