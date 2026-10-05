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

import org.apache.doris.qe.ConnectContext;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Save information that may need to pass to the external data source from Doris.
 * Such as user info, session variable, predicates, etc.
 */
public class SessionContext {
    private static final SessionContext EMPTY = new SessionContext("", null);

    private final String sessionId;
    private final DelegatedCredential delegatedCredential;

    private SessionContext(String sessionId, DelegatedCredential delegatedCredential) {
        this.sessionId = sessionId;
        this.delegatedCredential = delegatedCredential;
    }

    public static SessionContext empty() {
        return EMPTY;
    }

    /**
     * Returns the {@link SessionContext} bound to the current connection, or {@link #empty()} when
     * there is no active connection or no session context has been set (for example background
     * threads and internal callers). Callers can rely on a non-null result.
     */
    public static SessionContext current() {
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            return empty();
        }
        SessionContext sessionContext = context.getSessionContext();
        return sessionContext == null ? empty() : sessionContext;
    }

    public static SessionContext of(DelegatedCredential delegatedCredential) {
        if (delegatedCredential == null) {
            return empty();
        }
        return new SessionContext(UUID.randomUUID().toString(), delegatedCredential);
    }

    public static SessionContext of(String sessionId, DelegatedCredential delegatedCredential) {
        return new SessionContext(Objects.requireNonNull(sessionId, "sessionId is required"),
                Objects.requireNonNull(delegatedCredential, "delegatedCredential is required"));
    }

    public String getSessionId() {
        return sessionId;
    }

    public Optional<DelegatedCredential> getDelegatedCredential() {
        return Optional.ofNullable(delegatedCredential);
    }

    public boolean hasDelegatedCredential() {
        return delegatedCredential != null;
    }

    public OptionalLong getDelegatedCredentialExpiresAtMillis() {
        return delegatedCredential == null ? OptionalLong.empty() : delegatedCredential.getExpiresAtMillis();
    }

    public boolean isDelegatedCredentialExpired(long currentTimeMillis) {
        return delegatedCredential != null && delegatedCredential.isExpired(currentTimeMillis);
    }
}
