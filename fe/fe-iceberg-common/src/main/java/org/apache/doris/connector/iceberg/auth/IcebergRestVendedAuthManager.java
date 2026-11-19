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

package org.apache.doris.connector.iceberg.auth;


import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Shares the live catalog authentication with storage refreshes. The REST catalog owns its lifetime. */
public final class IcebergRestVendedAuthManager implements AuthManager {
    public static final String CONTEXT_ID = "doris.vended-credentials.context-id";
    private static final String DELEGATE_TYPE = "doris.vended-credentials.auth-type";
    private static final String DELEGATE_SIGV4 = "doris.vended-credentials.sigv4-enabled";
    private static final String SESSION_PREFIX = "doris.vended-credentials.session.";

    public static Map<String, String> withSession(Map<String, String> properties, SessionContext context) {
        Map<String, String> result = new HashMap<>(properties);
        if (context.credentials() != null && !context.credentials().isEmpty()) {
            result.put(SESSION_PREFIX + "id", context.sessionId());
            if (context.identity() != null) {
                result.put(SESSION_PREFIX + "identity", context.identity());
            }
            context.credentials().forEach((key, value) -> result.put(SESSION_PREFIX + "credential." + key, value));
            context.properties().forEach((key, value) -> result.put(SESSION_PREFIX + "property." + key, value));
        }
        return result;
    }

    static AuthSession contextualSession(AuthManager manager, AuthSession catalog, Map<String, String> properties) {
        if (!properties.containsKey(SESSION_PREFIX + "id")) {
            return catalog;
        }
        Map<String, String> credentials = new HashMap<>();
        Map<String, String> contextProperties = new HashMap<>();
        properties.forEach((key, value) -> {
            if (key.startsWith(SESSION_PREFIX + "credential.")) {
                credentials.put(key.substring((SESSION_PREFIX + "credential.").length()), value);
            } else if (key.startsWith(SESSION_PREFIX + "property.")) {
                contextProperties.put(key.substring((SESSION_PREFIX + "property.").length()), value);
            }
        });
        return manager.contextualSession(new SessionContext(properties.get(SESSION_PREFIX + "id"),
                properties.get(SESSION_PREFIX + "identity"), credentials, contextProperties), catalog);
    }

    private static final Map<String, GcsVendedCredentials.CatalogCredentials> CONTEXTS = new ConcurrentHashMap<>();

    private final String name;
    private AuthManager delegate;
    private String contextId;
    private GcsVendedCredentials.CatalogCredentials credentials;
    private boolean closed;

    public IcebergRestVendedAuthManager(String name) {
        this.name = name;
    }

    /** Install before RESTCatalog.initialize, so its normal init/catalog/table auth flows remain authoritative. */
    public static void configure(Map<String, String> properties) {
        properties.put(CONTEXT_ID, UUID.randomUUID().toString());
        String type = properties.put(AuthProperties.AUTH_TYPE, IcebergRestVendedAuthManager.class.getName());
        if (type != null) {
            properties.put(DELEGATE_TYPE, type);
        }
        String sigv4 = properties.remove("rest.sigv4-enabled");
        if (sigv4 != null) {
            properties.put(DELEGATE_SIGV4, sigv4);
        }
    }

    public static GcsVendedCredentials.CatalogCredentials credentials(Map<String, String> properties) {
        String id = properties.get(CONTEXT_ID);
        GcsVendedCredentials.CatalogCredentials context = id == null ? null : CONTEXTS.get(id);
        if (context == null) {
            throw new VendedCredentialsException(
                    "GCS refresh requires a live REST catalog authentication context", null);
        }
        return context;
    }

    private static Map<String, String> delegateProperties(Map<String, String> properties) {
        Map<String, String> result = new HashMap<>(properties);
        result.remove(AuthProperties.AUTH_TYPE);
        String type = result.remove(DELEGATE_TYPE);
        if (type != null) {
            result.put(AuthProperties.AUTH_TYPE, type);
        }
        String sigv4 = result.remove(DELEGATE_SIGV4);
        if (sigv4 != null) {
            result.put("rest.sigv4-enabled", sigv4);
        }
        return result;
    }

    @Override
    public AuthSession initSession(RESTClient client, Map<String, String> properties) {
        contextId = properties.get(CONTEXT_ID);
        Map<String, String> original = delegateProperties(properties);
        delegate = AuthManagers.loadAuthManager(name, original);
        return delegate.initSession(client, original);
    }

    @Override
    public AuthSession catalogSession(RESTClient client, Map<String, String> properties) {
        if (delegate == null) {
            // Iceberg's native GCS FileIO may ask for a refresh auth manager too. Borrow the
            // existing session; closing that FileIO must not close the catalog's authentication.
            credentials(properties); // Fail immediately when the catalog has already closed.
            return new AuthSession() {
                @Override
                public HTTPRequest authenticate(HTTPRequest request) {
                    return credentials(properties).session(properties).authenticate(request);
                }

                @Override
                public void close() {
                    // The catalog owns this session.
                }
            };
        }
        AuthSession session = delegate.catalogSession(client, delegateProperties(properties));
        credentials = new GcsVendedCredentials.CatalogCredentials(client, delegate, session, properties);
        if (CONTEXTS.putIfAbsent(contextId, credentials) != null) {
            credentials.close();
            session.close();
            throw new IllegalStateException("Duplicate REST catalog credential context");
        }
        return session;
    }

    @Override
    public AuthSession contextualSession(SessionContext context, AuthSession parent) {
        return delegate.contextualSession(context, parent);
    }

    @Override
    public AuthSession tableSession(TableIdentifier table, Map<String, String> properties, AuthSession parent) {
        return delegate.tableSession(table, properties, parent);
    }

    @Override
    public AuthSession tableSession(RESTClient client, Map<String, String> properties) {
        return delegate == null ? catalogSession(client, properties)
                : delegate.tableSession(client, delegateProperties(properties));
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (credentials != null) {
                CONTEXTS.remove(contextId, credentials);
                credentials.close();
            }
            if (delegate != null) {
                delegate.close();
            }
        }
    }
}
