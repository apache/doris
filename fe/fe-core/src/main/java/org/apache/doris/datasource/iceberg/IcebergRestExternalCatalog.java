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

import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;

import com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IcebergRestExternalCatalog extends IcebergExternalCatalog implements IcebergUserSessionCatalog {
    private static final Logger LOG = LogManager.getLogger(IcebergRestExternalCatalog.class);

    public IcebergRestExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void onClose() {
        super.onClose();
        // The default Catalog is RESTSessionCatalog.asCatalog(empty), which is not itself Closeable; the
        // closeable REST client/auth lives on the RESTSessionCatalog owned by IcebergRestProperties, so close
        // it here. For a REST catalog the metastore properties are always IcebergRestProperties; they are only
        // null before any properties have been parsed (e.g. closing a never-initialized catalog).
        MetastoreProperties metaProps = catalogProperty.getMetastoreProperties();
        if (metaProps != null) {
            ((IcebergRestProperties) metaProps).closeRestSessionCatalog();
        }
    }

    @Override
    public List<String> getDbNames() {
        SessionContext ctx = SessionContext.current();
        if (!shouldBypassDatabaseCache(ctx)) {
            return super.getDbNames();
        }
        makeSureInitialized();
        return listLocalDatabaseNamesWithoutCache(ctx);
    }

    @Override
    public List<String> getDbNamesOrEmpty() {
        SessionContext ctx = SessionContext.current();
        if (!shouldBypassDatabaseCache(ctx)) {
            return super.getDbNamesOrEmpty();
        }
        try {
            return getDbNames();
        } catch (Exception e) {
            LOG.warn("failed to get db names in catalog {}", getName(), e);
            return Collections.emptyList();
        }
    }

    @Override
    public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
        if (dbName == null || dbName.isEmpty() || isSystemDatabase(dbName)) {
            return super.getDbNullable(dbName);
        }
        SessionContext ctx = SessionContext.current();
        if (!shouldBypassDatabaseCache(ctx)) {
            return super.getDbNullable(dbName);
        }
        // User-session lookup must fail fast instead of silently hiding initialization errors.
        makeSureInitialized();
        return getDbNullableWithoutCache(ctx, dbName);
    }

    @Override
    protected boolean shouldBypassTableNameCache(SessionContext ctx) {
        return shouldBypassDatabaseCache(ctx);
    }

    @Override
    protected SessionContext getCatalogInitializationSessionContext() {
        SessionContext ctx = SessionContext.current();
        return shouldBypassDatabaseCache(ctx) ? ctx : SessionContext.empty();
    }

    protected List<String> listDatabaseNames(SessionContext ctx) {
        return ((IcebergMetadataOps) metadataOps).listDatabaseNames(ctx);
    }

    private ExternalDatabase<? extends ExternalTable> getDbNullableWithoutCache(SessionContext ctx,
            String requestedLocalDbName) {
        Optional<String> remoteDbName = resolveRemoteDatabaseName(ctx, requestedLocalDbName);
        if (!remoteDbName.isPresent()) {
            return null;
        }
        String localDbName = localDatabaseName(remoteDbName.get());
        return buildDbForInit(remoteDbName.get(), localDbName, Util.genIdByName(getName(), localDbName),
                InitCatalogLog.Type.ICEBERG, false);
    }

    private Optional<String> resolveRemoteDatabaseName(SessionContext ctx, String requestedLocalDbName) {
        return listFilteredRemoteDatabaseNames(ctx).stream()
                .filter(remoteDbName -> localDatabaseNameMatches(localDatabaseName(remoteDbName), requestedLocalDbName))
                .findFirst();
    }

    private List<String> listLocalDatabaseNamesWithoutCache(SessionContext ctx) {
        List<String> localDbNames = listFilteredRemoteDatabaseNames(ctx).stream()
                .map(this::localDatabaseName)
                .collect(Collectors.toCollection(Lists::newArrayList));
        // System databases are Doris-internal and always visible. The cached path
        // (ExternalCatalog#getFilteredDatabaseNames) appends them unconditionally, but that path is
        // bypassed for user-session metadata, so mirror the same injection here.
        localDbNames.remove(InfoSchemaDb.DATABASE_NAME);
        localDbNames.add(InfoSchemaDb.DATABASE_NAME);
        localDbNames.remove(MysqlDb.DATABASE_NAME);
        localDbNames.add(MysqlDb.DATABASE_NAME);
        return localDbNames;
    }

    private List<String> listFilteredRemoteDatabaseNames(SessionContext ctx) {
        Map<String, Boolean> includeDatabaseMap = getIncludeDatabaseMap();
        Map<String, Boolean> excludeDatabaseMap = getExcludeDatabaseMap();
        return Lists.newArrayList(listDatabaseNames(ctx)).stream()
                .filter(dbName -> isDatabaseVisible(dbName, includeDatabaseMap, excludeDatabaseMap))
                .collect(Collectors.toList());
    }

    private boolean isDatabaseVisible(String dbName, Map<String, Boolean> includeDatabaseMap,
            Map<String, Boolean> excludeDatabaseMap) {
        if (excludeDatabaseMap.containsKey(dbName)) {
            return false;
        }
        return includeDatabaseMap.isEmpty() || includeDatabaseMap.containsKey(dbName);
    }

    private boolean localDatabaseNameMatches(String localDbName, String requestedLocalDbName) {
        if (getLowerCaseDatabaseNames() == 1) {
            return localDbName.equals(requestedLocalDbName.toLowerCase());
        }
        if (getLowerCaseDatabaseNames() == 2) {
            return localDbName.equalsIgnoreCase(requestedLocalDbName);
        }
        return localDbName.equals(requestedLocalDbName);
    }

    private String localDatabaseName(String remoteDbName) {
        String localDbName = fromRemoteDatabaseName(remoteDbName);
        if (getLowerCaseDatabaseNames() == 1) {
            return localDbName.toLowerCase();
        }
        if (getLowerCaseDatabaseNames() == 2) {
            return remoteDbName;
        }
        return localDbName;
    }

    private boolean isSystemDatabase(String dbName) {
        return dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME) || dbName.equalsIgnoreCase(MysqlDb.DATABASE_NAME);
    }

    private boolean shouldBypassDatabaseCache(SessionContext ctx) {
        // Bypassing the shared cache and using a per-user session catalog are the same condition.
        return useSessionCatalog(ctx);
    }

    /**
     * Whether the given request should use a per-user session catalog. This is the single source of truth for
     * that decision, shared by the cache-bypass logic above and by {@link IcebergMetadataOps} via
     * {@link IcebergUserSessionCatalog}.
     *
     * <p>Three outcomes:
     * <ul>
     *   <li>dynamic identity disabled: {@code false} - use the shared default path;</li>
     *   <li>dynamic identity enabled and the request carries a delegated credential: {@code true} - use the
     *       per-user session catalog;</li>
     *   <li>dynamic identity enabled but the request has <em>no</em> delegated credential: throw. A catalog
     *       configured with {@code iceberg.rest.session=user} has no shared/default identity to fall back on,
     *       and silently borrowing another request's token would leak credentials across users. So a session
     *       without a token (e.g. a password login) is rejected here rather than served by the default path.</li>
     * </ul>
     */
    @Override
    public boolean useSessionCatalog(SessionContext ctx) {
        if (!isIcebergRestUserSessionEnabled()) {
            return false;
        }
        if (!ctx.hasDelegatedCredential()) {
            throw new IllegalStateException("Catalog " + getName() + " is configured with dynamic identity "
                    + "(iceberg.rest.session=user) but the current session has no delegated credential. "
                    + "Access requires a token-based identity (e.g. OAuth/OIDC/JWT).");
        }
        return true;
    }

    @Override
    public RESTSessionCatalog getRestSessionCatalog() {
        IcebergRestProperties props = restProperties();
        return props == null ? null : props.getRestSessionCatalog();
    }

    @Override
    public IcebergRestProperties.DelegatedTokenMode getDelegatedTokenMode() {
        IcebergRestProperties props = restProperties();
        return props == null ? IcebergRestProperties.DelegatedTokenMode.ACCESS_TOKEN : props.getDelegatedTokenMode();
    }

    @Override
    public boolean isViewEnabled() {
        IcebergRestProperties props = restProperties();
        return props != null && props.isIcebergRestViewEnabled();
    }

    @Override
    public boolean isNestedNamespaceEnabled() {
        IcebergRestProperties props = restProperties();
        return props != null && props.isIcebergRestNestedNamespaceEnabled();
    }

    /**
     * Whether REST user-session mode is enabled for this catalog.
     *
     * <p>This is REST-specific behavior, so it lives on {@link IcebergRestExternalCatalog} rather than the
     * generic {@link IcebergExternalCatalog} base class. The decision is made purely from catalog
     * properties and is safe to call before catalog initialization, e.g. from the cache-bypass decision in
     * {@link #getDbNames()} and {@link #getDbNullable(String)} which runs before initialization.
     */
    public boolean isIcebergRestUserSessionEnabled() {
        IcebergRestProperties props = restProperties();
        if (props != null) {
            return props.isIcebergRestUserSessionEnabled();
        }
        // Session gating may be checked before metastore properties are lazily materialized,
        // so fall back to the raw catalog property to preserve fail-fast behavior.
        return "user".equalsIgnoreCase(catalogProperty.getOrDefault("iceberg.rest.session", "none"));
    }

    private IcebergRestProperties restProperties() {
        MetastoreProperties metaProps = catalogProperty.getMetastoreProperties();
        return metaProps instanceof IcebergRestProperties ? (IcebergRestProperties) metaProps : null;
    }

}
