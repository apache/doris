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

package org.apache.doris.connector.api;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Neutral, immutable SPI carrier for a user's per-connection delegated (OIDC/JWT/SAML) credential.
 *
 * <p>The engine captures the credential at authentication time (fe-core {@code DelegatedCredential}) and
 * copies it into this neutral DTO on the {@link ConnectorSession} — see
 * {@link ConnectorSession#getDelegatedCredential()}. A connector that declares
 * {@link ConnectorCapability#SUPPORTS_USER_SESSION} consumes it to project per-user identity onto the
 * remote metadata source (e.g. an Iceberg REST catalog's {@code SessionCatalog}), WITHOUT importing any
 * fe-core type. The field shape mirrors fe-core {@code DelegatedCredential} exactly so the copy is a
 * lossless 1:1 mapping.</p>
 *
 * <p>The {@link #getToken() token} is security-sensitive: it is connection-scoped and in-memory only, and
 * must never be edit-logged, persisted, or rendered by {@code SHOW} / profile / {@code information_schema}
 * surfaces. {@link #toString()} redacts it.</p>
 */
public final class ConnectorDelegatedCredential {

    private final Type type;
    private final String token;
    // Absolute expiry in epoch millis, or null when the authenticator supplied none. Kept as a boxed Long
    // (not OptionalLong) so the field is nullable; getExpiresAtMillis() re-wraps it, mirroring fe-core.
    private final Long expiresAtMillis;

    public ConnectorDelegatedCredential(Type type, String token) {
        this(type, token, OptionalLong.empty());
    }

    public ConnectorDelegatedCredential(Type type, String token, OptionalLong expiresAtMillis) {
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

    public OptionalLong getExpiresAtMillis() {
        return expiresAtMillis == null ? OptionalLong.empty() : OptionalLong.of(expiresAtMillis);
    }

    public boolean isExpired(long currentTimeMillis) {
        // Inclusive comparison (>=) on purpose (mirrors fe-core DelegatedCredential.isExpired): at the exact
        // expiration instant the credential is already treated as expired, so a token is never handed to the
        // downstream REST server right as it stops being accepted — fail closed on the boundary.
        return expiresAtMillis != null && currentTimeMillis >= expiresAtMillis;
    }

    @Override
    public String toString() {
        return "ConnectorDelegatedCredential{"
                + "type=" + type
                + ", token=<redacted>"
                + ", expiresAtMillis=" + expiresAtMillis
                + '}';
    }

    /**
     * Credential kinds, mirroring fe-core {@code DelegatedCredential.Type} one-to-one. The connector maps
     * each to the matching Iceberg OAuth2 token-type key when the delegated-token mode is
     * {@code token_exchange}; {@code access_token} mode passes the token verbatim as the OAuth2 bearer.
     */
    public enum Type {
        ACCESS_TOKEN,
        ID_TOKEN,
        JWT,
        SAML
    }
}
