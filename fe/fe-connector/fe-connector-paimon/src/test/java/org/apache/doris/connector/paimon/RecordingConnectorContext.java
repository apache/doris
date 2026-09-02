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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.UnaryOperator;

/**
 * Hand-written {@link ConnectorContext} test double (no Mockito) used to assert that the
 * Paimon DDL path wraps every remote call in {@link #executeAuthenticated}.
 *
 * <p>Read-path tests just pass a fresh instance and ignore it. DDL tests assert on
 * {@link #authCount} (one wrap per DDL op) and use {@link #failAuth} to simulate an auth
 * failure: when set, {@link #executeAuthenticated} throws WITHOUT invoking the task, which
 * proves the seam call sits INSIDE the authenticator (if the production code called the seam
 * directly, the recording fake would log the call despite the auth failure).
 */
final class RecordingConnectorContext implements ConnectorContext, ConnectorStorageContext {

    // Storage services moved onto ConnectorStorageContext; this double implements both halves and hands
    // itself back, so its overrides below are the ones the connector reaches. Forgetting this getter would
    // silently give the connector NOOP and make those overrides dead code.
    @Override
    public ConnectorStorageContext getStorageContext() {
        return this;
    }

    int authCount;
    boolean failAuth;
    int failAuthOnInvocation = -1;

    // ---- sibling-connector seam hook (proves the decorator delegates createSiblingConnector) ----
    /** The type the wrapper forwarded to {@link #createSiblingConnector}. */
    String lastSiblingType;
    /** The properties the wrapper forwarded to {@link #createSiblingConnector}. */
    Map<String, String> lastSiblingProps;

    @Override
    public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
        lastSiblingType = catalogType;
        lastSiblingProps = properties;
        return null;
    }

    // ---- C2: getStorageProperties hook (FE-bound fe-filesystem storage props) ----
    /** Storage properties the fake returns from {@link #getStorageProperties()} (default: none). */
    List<StorageProperties> storageProperties = Collections.emptyList();

    // ---- FIX-URI-NORMALIZE / FIX-REST-VENDED-URI-NORMALIZE: normalizeStorageUri hook ----
    /** Number of times the connector invoked {@link #normalizeStorageUri}. */
    int normalizeCount;
    /** Number of times the connector asked for a batch normalizer (the once-per-scan derivation). */
    int newNormalizerCount;
    /** The vended token the connector passed to the most recent 2-arg {@link #normalizeStorageUri}. */
    Map<String, String> lastVendedToken;

    @Override
    public String getCatalogName() {
        return "test";
    }

    @Override
    public List<StorageProperties> getStorageProperties() {
        return storageProperties;
    }

    @Override
    public String normalizeStorageUri(String rawUri) {
        // The 1-arg form folds to the 2-arg with no token, so every caller path is recorded identically.
        return normalizeStorageUri(rawUri, null);
    }

    @Override
    public String normalizeStorageUri(String rawUri, Map<String, String> vendedToken) {
        normalizeCount++;
        lastVendedToken = vendedToken;
        // Deterministic stand-in for the engine's oss://->s3:// scheme rewrite, so a connector wiring
        // test can prove BOTH the data-file and DV paths were routed through this hook AND that the
        // per-table vended token is threaded to each (the real normalization is covered by
        // DefaultConnectorContextNormalizeUriTest in fe-core).
        if (rawUri != null && rawUri.startsWith("oss://")) {
            return "s3://" + rawUri.substring("oss://".length());
        }
        return rawUri;
    }

    @Override
    public UnaryOperator<String> newStorageUriNormalizer(Map<String, String> vendedToken) {
        // A DISTINGUISHABLE normalizer instance. The SPI default builds a fresh lambda that never touches
        // this context, so a decorator that forgets to forward this method silently gives back the default
        // one - correct results, but the once-per-scan storage-config derivation degrades to once per file
        // with nothing in the logs to say so.
        newNormalizerCount++;
        return rawUri -> normalizeStorageUri(rawUri, vendedToken);
    }

    @Override
    public long getCatalogId() {
        return 0;
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        authCount++;
        if (failAuth || authCount == failAuthOnInvocation) {
            // Deliberately do NOT call task -> the wrapped seam call must not run.
            throw new RuntimeException("auth failed");
        }
        return task.call();
    }

    // A distinguishable, non-null engine filesystem. The SPI default for getFileSystem is null, so a
    // decorator that forgets to forward it hands the connector null instead of this instance.
    final FileSystem engineFileSystem = (FileSystem) java.lang.reflect.Proxy.newProxyInstance(
            RecordingConnectorContext.class.getClassLoader(), new Class<?>[] {FileSystem.class},
            (proxy, method, args) -> null);

    @Override
    public FileSystem getFileSystem(ConnectorSession session) {
        return engineFileSystem;
    }

    // ---- external-change-poller seam hook (proves the connector notifies the engine, not just itself) ----
    /** {@code (dbName, tableName)} pairs the connector passed to {@link #notifyExternalTableChanged}. */
    final List<java.util.AbstractMap.SimpleEntry<String, String>> externalTableChangeNotifications =
            new java.util.ArrayList<>();

    @Override
    public void notifyExternalTableChanged(String remoteDbName, String remoteTableName) {
        externalTableChangeNotifications.add(
                new java.util.AbstractMap.SimpleEntry<>(remoteDbName, remoteTableName));
    }

}
