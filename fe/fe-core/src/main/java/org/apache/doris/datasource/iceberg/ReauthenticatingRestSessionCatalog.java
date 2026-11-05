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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.BaseViewSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A session catalog that transparently recovers from an expired/rejected credential on an Iceberg REST
 * catalog, instead of failing every request until the FE restarts.
 *
 * <p><b>Why this exists.</b> {@link RESTSessionCatalog} obtains an OAuth2 token at {@code initialize()} and
 * keeps it refreshed in the background. If a refresh permanently fails (e.g. the auth server was briefly
 * unreachable or overloaded at refresh time), the client is left holding a stale token and every subsequent
 * request fails with {@link NotAuthorizedException} (HTTP 401) forever — the client has no re-authentication
 * path of its own, and neither {@code REFRESH CATALOG} nor metadata-cache invalidation rebuilds it. The only
 * recovery is building a fresh client, which is exactly what this wrapper does: on a 401 from a request made
 * under the catalog's own identity, it rebuilds the delegate {@link RESTSessionCatalog} (forcing a fresh
 * OAuth2 token fetch), closes the wedged one, and retries the operation once. If the retry also fails, the
 * error propagates unchanged.
 *
 * <p><b>Why a wrapper at the session-catalog level.</b> Everything Doris hands out for a REST catalog — the
 * default {@code asCatalog(empty)} used by {@code IcebergMetadataOps}, the {@code asViewCatalog(empty)} view
 * path, and per-user delegated sessions — is a thin view that calls back into this class (see
 * {@link org.apache.iceberg.catalog.BaseSessionCatalog.AsCatalog}). Wrapping here means no holder of any of
 * those references ever sees a stale client after recovery, and non-REST catalogs are untouched because only
 * {@code IcebergRestProperties} constructs this class.
 *
 * <p><b>What is retried.</b> Both reads and mutations: a 401 is rejected by the server before the request is
 * processed, so retrying after re-authentication cannot double-apply an operation. Requests that carry a
 * per-user delegated credential are <b>not</b> recovered — a 401 there means that user's token is invalid,
 * and rebuilding the shared client cannot (and must not) fix it.
 *
 * <p><b>Boundary.</b> Recovery triggers on catalog-level operations. Objects already handed out (a loaded
 * {@link Table}/{@link View}, a builder from {@code buildTable}/{@code buildView}) keep their own reference
 * to the client they were created with; if the credential expires between obtaining such an object and using
 * it, that one use can still fail with a 401. The next catalog-level operation rebuilds the client, after
 * which reloading the object succeeds.
 */
public class ReauthenticatingRestSessionCatalog extends BaseViewSessionCatalog implements Closeable {

    private static final Logger LOG = LogManager.getLogger(ReauthenticatingRestSessionCatalog.class);

    private final Supplier<RESTSessionCatalog> delegateBuilder;
    private volatile RESTSessionCatalog delegate;

    public ReauthenticatingRestSessionCatalog(RESTSessionCatalog initialDelegate,
            Supplier<RESTSessionCatalog> delegateBuilder) {
        this.delegate = initialDelegate;
        this.delegateBuilder = delegateBuilder;
    }

    @VisibleForTesting
    RESTSessionCatalog currentDelegate() {
        return delegate;
    }

    private <T> T withAuthRecovery(SessionContext context, Supplier<T> op) {
        RESTSessionCatalog attemptedOn = delegate;
        try {
            return op.get();
        } catch (RuntimeException e) {
            if (!isAuthExpired(e) || !usesCatalogIdentity(context)) {
                throw e;
            }
            reauthenticate(attemptedOn, e);
            return op.get();
        }
    }

    private void runWithAuthRecovery(SessionContext context, Runnable op) {
        withAuthRecovery(context, () -> {
            op.run();
            return null;
        });
    }

    /**
     * Rebuilds the delegate (fresh client, fresh token) and closes the wedged one. Synchronized so that
     * concurrent 401s coalesce into a single rebuild: a thread whose failed attempt ran against an
     * already-replaced delegate skips the rebuild and just retries on the fresh one.
     */
    private synchronized void reauthenticate(RESTSessionCatalog attemptedOn, RuntimeException cause) {
        if (delegate != attemptedOn) {
            return;
        }
        LOG.warn("Iceberg REST catalog {} rejected its cached credential (401 Not Authorized) and the client "
                + "cannot recover it internally. Rebuilding the REST client to force re-authentication, "
                + "then retrying the request once.", name(), cause);
        RESTSessionCatalog replacement = delegateBuilder.get();
        RESTSessionCatalog wedged = delegate;
        delegate = replacement;
        try {
            wedged.close();
        } catch (IOException | RuntimeException e) {
            LOG.warn("Failed to close the replaced Iceberg REST client of catalog {}", name(), e);
        }
    }

    /**
     * A request made with a per-user delegated credential authenticates as that user, not as the catalog;
     * a 401 there is that token's problem and must not rebuild (or leak into) the shared client.
     */
    private static boolean usesCatalogIdentity(SessionContext context) {
        return context == null || context.credentials() == null || context.credentials().isEmpty();
    }

    private static boolean isAuthExpired(Throwable t) {
        return ExceptionUtils.getThrowableList(t).stream().anyMatch(c -> c instanceof NotAuthorizedException);
    }

    // ---- SessionCatalog ----

    @Override
    public void initialize(String name, Map<String, String> properties) {
        super.initialize(name, properties);
        delegate.initialize(name, properties);
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }

    @Override
    public List<TableIdentifier> listTables(SessionContext context, Namespace ns) {
        return withAuthRecovery(context, () -> delegate.listTables(context, ns));
    }

    @Override
    public Catalog.TableBuilder buildTable(SessionContext context, TableIdentifier ident, Schema schema) {
        return withAuthRecovery(context, () -> delegate.buildTable(context, ident, schema));
    }

    @Override
    public Table registerTable(SessionContext context, TableIdentifier ident, String metadataFileLocation) {
        return withAuthRecovery(context, () -> delegate.registerTable(context, ident, metadataFileLocation));
    }

    @Override
    public boolean tableExists(SessionContext context, TableIdentifier ident) {
        return withAuthRecovery(context, () -> delegate.tableExists(context, ident));
    }

    @Override
    public Table loadTable(SessionContext context, TableIdentifier ident) {
        return withAuthRecovery(context, () -> delegate.loadTable(context, ident));
    }

    @Override
    public boolean dropTable(SessionContext context, TableIdentifier ident) {
        return withAuthRecovery(context, () -> delegate.dropTable(context, ident));
    }

    @Override
    public boolean purgeTable(SessionContext context, TableIdentifier ident) {
        return withAuthRecovery(context, () -> delegate.purgeTable(context, ident));
    }

    @Override
    public void renameTable(SessionContext context, TableIdentifier from, TableIdentifier to) {
        runWithAuthRecovery(context, () -> delegate.renameTable(context, from, to));
    }

    @Override
    public void invalidateTable(SessionContext context, TableIdentifier ident) {
        runWithAuthRecovery(context, () -> delegate.invalidateTable(context, ident));
    }

    @Override
    public void createNamespace(SessionContext context, Namespace namespace, Map<String, String> metadata) {
        runWithAuthRecovery(context, () -> delegate.createNamespace(context, namespace, metadata));
    }

    @Override
    public List<Namespace> listNamespaces(SessionContext context, Namespace namespace) {
        return withAuthRecovery(context, () -> delegate.listNamespaces(context, namespace));
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(SessionContext context, Namespace namespace) {
        return withAuthRecovery(context, () -> delegate.loadNamespaceMetadata(context, namespace));
    }

    @Override
    public boolean dropNamespace(SessionContext context, Namespace namespace) {
        return withAuthRecovery(context, () -> delegate.dropNamespace(context, namespace));
    }

    @Override
    public boolean updateNamespaceMetadata(SessionContext context, Namespace namespace,
            Map<String, String> updates, Set<String> removals) {
        return withAuthRecovery(context, () -> delegate.updateNamespaceMetadata(context, namespace, updates,
                removals));
    }

    @Override
    public boolean namespaceExists(SessionContext context, Namespace namespace) {
        return withAuthRecovery(context, () -> delegate.namespaceExists(context, namespace));
    }

    // ---- ViewSessionCatalog ----

    @Override
    public List<TableIdentifier> listViews(SessionContext context, Namespace namespace) {
        return withAuthRecovery(context, () -> delegate.listViews(context, namespace));
    }

    @Override
    public View loadView(SessionContext context, TableIdentifier identifier) {
        return withAuthRecovery(context, () -> delegate.loadView(context, identifier));
    }

    @Override
    public boolean viewExists(SessionContext context, TableIdentifier identifier) {
        return withAuthRecovery(context, () -> delegate.viewExists(context, identifier));
    }

    @Override
    public ViewBuilder buildView(SessionContext context, TableIdentifier identifier) {
        return withAuthRecovery(context, () -> delegate.buildView(context, identifier));
    }

    @Override
    public boolean dropView(SessionContext context, TableIdentifier identifier) {
        return withAuthRecovery(context, () -> delegate.dropView(context, identifier));
    }

    @Override
    public void renameView(SessionContext context, TableIdentifier from, TableIdentifier to) {
        runWithAuthRecovery(context, () -> delegate.renameView(context, from, to));
    }

    @Override
    public void invalidateView(SessionContext context, TableIdentifier identifier) {
        runWithAuthRecovery(context, () -> delegate.invalidateView(context, identifier));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
