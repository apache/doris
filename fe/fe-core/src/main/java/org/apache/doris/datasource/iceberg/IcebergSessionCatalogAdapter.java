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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties.DelegatedTokenMode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.BaseSessionCatalog;
import org.apache.iceberg.catalog.BaseViewSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;

class IcebergSessionCatalogAdapter {
    private static final String SESSION_CATALOG_FIELD = "sessionCatalog";

    private final Catalog catalog;
    private final Optional<BaseSessionCatalog> sessionCatalog;
    private final DelegatedTokenMode delegatedTokenMode;

    IcebergSessionCatalogAdapter(Catalog catalog) {
        this(catalog, DelegatedTokenMode.ACCESS_TOKEN);
    }

    IcebergSessionCatalogAdapter(Catalog catalog, DelegatedTokenMode delegatedTokenMode) {
        this.catalog = catalog;
        this.sessionCatalog = extractSessionCatalog(catalog);
        this.delegatedTokenMode = delegatedTokenMode;
    }

    Catalog catalog(SessionContext context) {
        if (!hasDelegatedCredential(context)) {
            return catalog;
        }
        BaseSessionCatalog activeSessionCatalog = requireSessionCatalog();
        return activeSessionCatalog.asCatalog(toIcebergSessionContext(context, delegatedTokenMode));
    }

    SupportsNamespaces namespaces(SessionContext context) {
        return (SupportsNamespaces) catalog(context);
    }

    Catalog delegatedCatalog(SessionContext context) {
        return requireSessionCatalog().asCatalog(toIcebergSessionContext(
                requireDelegatedCredential(context), delegatedTokenMode));
    }

    SupportsNamespaces delegatedNamespaces(SessionContext context) {
        return (SupportsNamespaces) delegatedCatalog(context);
    }

    Optional<ViewCatalog> delegatedViewCatalog(SessionContext context) {
        BaseSessionCatalog activeSessionCatalog = requireSessionCatalog();
        if (activeSessionCatalog instanceof BaseViewSessionCatalog) {
            return Optional.of(((BaseViewSessionCatalog) activeSessionCatalog)
                    .asViewCatalog(toIcebergSessionContext(requireDelegatedCredential(context), delegatedTokenMode)));
        }
        requireDelegatedCredential(context);
        return Optional.empty();
    }

    Optional<ViewCatalog> viewCatalog(SessionContext context) {
        if (!hasDelegatedCredential(context)) {
            return catalog instanceof ViewCatalog ? Optional.of((ViewCatalog) catalog) : Optional.empty();
        }
        BaseSessionCatalog sessionCatalog = requireSessionCatalog();
        if (sessionCatalog instanceof BaseViewSessionCatalog) {
            return Optional.of(((BaseViewSessionCatalog) sessionCatalog)
                    .asViewCatalog(toIcebergSessionContext(context, delegatedTokenMode)));
        }
        return Optional.empty();
    }

    private Optional<BaseSessionCatalog> extractSessionCatalog(Catalog catalog) {
        Class<?> clazz = catalog.getClass();
        while (clazz != null && clazz != Object.class) {
            try {
                Field field = clazz.getDeclaredField(SESSION_CATALOG_FIELD);
                field.setAccessible(true);
                Object value = field.get(catalog);
                if (value instanceof BaseSessionCatalog) {
                    return Optional.of((BaseSessionCatalog) value);
                }
                throw new IllegalStateException("Iceberg REST sessionCatalog field is not a BaseSessionCatalog");
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Failed to access Iceberg REST sessionCatalog field", e);
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    static org.apache.iceberg.catalog.SessionCatalog.SessionContext toIcebergSessionContext(
            SessionContext context) {
        return toIcebergSessionContext(context, DelegatedTokenMode.ACCESS_TOKEN);
    }

    @VisibleForTesting
    static org.apache.iceberg.catalog.SessionCatalog.SessionContext toIcebergSessionContext(
            SessionContext context, DelegatedTokenMode delegatedTokenMode) {
        Map<String, String> credentials = ImmutableMap.of();
        if (context.getDelegatedCredential().isPresent()) {
            credentials = toIcebergCredentials(context.getDelegatedCredential().get(), delegatedTokenMode);
        }
        return new org.apache.iceberg.catalog.SessionCatalog.SessionContext(
                context.getSessionId(), null, credentials, ImmutableMap.of());
    }

    private BaseSessionCatalog requireSessionCatalog() {
        if (!sessionCatalog.isPresent()) {
            throw new IllegalStateException("Iceberg REST user session requires a session-aware Iceberg catalog");
        }
        return sessionCatalog.get();
    }

    private static SessionContext requireDelegatedCredential(SessionContext context) {
        if (!hasDelegatedCredential(context)) {
            throw new IllegalStateException("Iceberg REST user session requires delegated credential");
        }
        return context;
    }

    private static Map<String, String> toIcebergCredentials(
            DelegatedCredential credential, DelegatedTokenMode delegatedTokenMode) {
        if (delegatedTokenMode == DelegatedTokenMode.ACCESS_TOKEN) {
            return ImmutableMap.of(OAuth2Properties.TOKEN, credential.getToken());
        }
        return ImmutableMap.of(credential.getIcebergCredentialKey(), credential.getToken());
    }

    private static boolean hasDelegatedCredential(SessionContext context) {
        return context != null && context.hasDelegatedCredential();
    }
}
