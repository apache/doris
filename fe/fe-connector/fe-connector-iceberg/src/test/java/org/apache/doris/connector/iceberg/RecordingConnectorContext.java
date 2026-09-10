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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStorageAccess;
import org.apache.doris.connector.spi.ConnectorStorageAccessResolver;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TFileType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Hand-written {@link ConnectorContext} test double (no Mockito), adapted verbatim from the paimon
 * connector's {@code RecordingConnectorContext}.
 *
 * <p>{@link IcebergConnectorMetadata} takes a context (ctor {@code (IcebergCatalogOps, Map,
 * ConnectorContext)}) and wraps every remote read in {@link #executeAuthenticated}; the read tests use
 * this double to assert one wrap per op via {@link #authCount}, and that {@link #getStorageProperties}
 * is threaded through. When {@link #failAuth} is set,
 * {@link #executeAuthenticated} throws WITHOUT invoking the task, which proves the seam call sits INSIDE
 * the authenticator.
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

    /** Storage properties for older scan fixtures and the legacy fixture adapter in
     * {@link #newStorageAccessResolver}; explicit access results bypass this map (default: none). */
    List<StorageProperties> storageProperties = Collections.emptyList();

    /** BE-canonical vended creds the fake returns from {@link #vendStorageCredentials} for a NON-EMPTY token
     * (an empty/null token -> empty result, mirroring {@code DefaultConnectorContext} — so a test can prove the
     * catalog-flag GATE end-to-end: flag off -> empty token -> no vended {@code location.*}). */
    Map<String, String> vendedBeProps = Collections.emptyMap();

    /** Raw URIs the connector routed through {@link #normalizeStorageUri} (data/delete-path normalization). */
    final List<String> normalizedUris = new ArrayList<>();
    /** Number of times the connector invoked {@link #normalizeStorageUri} (1- or 2-arg). */
    int normalizeCount;
    /** Number of times the connector built a scan-scoped normalizer via {@link #newStorageUriNormalizer}
     * (should be once per scan — the perf hoist guard). */
    int newNormalizerCount;
    /** The vended token the connector passed to the most recent 2-arg {@link #normalizeStorageUri} (T09). */
    Map<String, String> lastVendedToken;

    /** Request-level storage access seam. Tests can supply a binding result independently of the legacy maps. */
    Function<String, ConnectorStorageAccess> storageAccessResolver;
    BiPredicate<String, String> storageLocationPrefixMatcher = (rawLocation, rawPrefix) -> {
        throw new UnsupportedOperationException("Storage prefix matching is not configured for this test");
    };
    Set<String> storageAccessProviderNames = Collections.singleton("fake");
    int newStorageAccessResolverCount;
    int storageAccessResolveCount;
    Map<String, String> lastStorageAccessVendedToken;
    final List<String> resolvedStorageUris = new ArrayList<>();
    final List<ConnectorStorageAccess> resolvedStorageAccesses = new ArrayList<>();
    final List<String> prefixMatchLocations = new ArrayList<>();
    final List<String> prefixMatchPrefixes = new ArrayList<>();
    int getStoragePropertiesCount;
    int vendStorageCredentialsCount;
    int getBackendFileTypeCount;

    /** BE file type the fake returns from {@link #getBackendFileType} (T06 iceberg write sink). */
    TFileType backendFileType = TFileType.FILE_S3;
    /** The vended token the connector passed to the most recent {@link #getBackendFileType}. */
    Map<String, String> lastFileTypeVendedToken;

    /** Broker addresses the fake returns from {@link #getBrokerAddresses()} (broker write sink). Default none,
     * so a FILE_BROKER write fails loud ("No alive broker.") unless a test populates it. */
    List<ConnectorBrokerAddress> brokerAddresses = Collections.emptyList();

    @Override
    public String getCatalogName() {
        return "test";
    }

    @Override
    public String getBackendFileType(String rawUri, Map<String, String> vendedToken) {
        getBackendFileTypeCount++;
        lastFileTypeVendedToken = vendedToken;
        return backendFileType.name();
    }

    @Override
    public List<ConnectorBrokerAddress> getBrokerAddresses() {
        return brokerAddresses;
    }

    @Override
    public String normalizeStorageUri(String rawUri) {
        // The 1-arg form folds to the 2-arg with no token (mirrors DefaultConnectorContext), so every caller
        // path records identically.
        return normalizeStorageUri(rawUri, null);
    }

    @Override
    public String normalizeStorageUri(String rawUri, Map<String, String> vendedToken) {
        normalizedUris.add(rawUri);
        normalizeCount++;
        lastVendedToken = vendedToken;
        // Canonicalize the scheme the way DefaultConnectorContext does for native paths (oss/cos/obs/s3a ->
        // s3), so a test can prove the connector routes data/delete paths through this seam AND (2-arg) that
        // the per-table vended token is threaded to each. Identity for already-canonical s3:// paths.
        return rawUri == null ? null : rawUri.replaceFirst("^(oss|cos|obs|s3a)://", "s3://");
    }

    @Override
    public UnaryOperator<String> newStorageUriNormalizer(Map<String, String> vendedToken) {
        // Count the once-per-scan derivation (the perf hoist) but still record each per-URI normalize by
        // delegating every apply back to the recording normalizeStorageUri — so existing recording assertions
        // (normalizedUris / normalizeCount / lastVendedToken) keep firing, while newNormalizerCount proves the
        // token->config derivation is entered once per scan, not once per file.
        newNormalizerCount++;
        return rawUri -> normalizeStorageUri(rawUri, vendedToken);
    }

    @Override
    public ConnectorStorageAccessResolver newStorageAccessResolver(Map<String, String> vendedToken) {
        newStorageAccessResolverCount++;
        lastStorageAccessVendedToken = vendedToken;
        Function<String, ConnectorStorageAccess> resolver = storageAccessResolver;
        if (resolver == null) {
            // Preserve the older tests' configured static/vended maps and URI fake. This is not an engine
            // binding implementation: isolation tests supply an explicit access result through the seam above.
            Map<String, String> backendProperties = new HashMap<>();
            for (StorageProperties properties : IcebergCatalogFactory.selectEffectiveStorages(getStorageProperties())) {
                properties.toBackendProperties().ifPresent(b -> backendProperties.putAll(b.toMap()));
            }
            backendProperties.putAll(vendStorageCredentials(vendedToken));
            resolver = rawUri -> new ConnectorStorageAccess("fake", normalizeStorageUri(rawUri, vendedToken),
                    fixtureBackendKind(), getBackendFileType(rawUri, vendedToken), backendProperties);
        }
        Function<String, ConnectorStorageAccess> requestResolver = resolver;
        BiPredicate<String, String> requestPrefixMatcher = storageLocationPrefixMatcher;
        return new ConnectorStorageAccessResolver(storageAccessProviderNames, rawUri -> {
            storageAccessResolveCount++;
            resolvedStorageUris.add(rawUri);
            ConnectorStorageAccess access = requestResolver.apply(rawUri);
            resolvedStorageAccesses.add(access);
            return access;
        }, (rawLocation, rawPrefix) -> {
            prefixMatchLocations.add(rawLocation);
            prefixMatchPrefixes.add(rawPrefix);
            return requestPrefixMatcher.test(rawLocation, rawPrefix);
        });
    }

    private BackendStorageKind fixtureBackendKind() {
        switch (backendFileType) {
            case FILE_HDFS:
                return BackendStorageKind.HDFS;
            case FILE_BROKER:
                return BackendStorageKind.BROKER;
            case FILE_LOCAL:
                return BackendStorageKind.LOCAL;
            default:
                return BackendStorageKind.S3_COMPATIBLE;
        }
    }

    @Override
    public List<StorageProperties> getStorageProperties() {
        getStoragePropertiesCount++;
        return storageProperties;
    }

    @Override
    public Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) {
        vendStorageCredentialsCount++;
        // Mirror DefaultConnectorContext: an empty/null token yields no overlay; a non-empty token yields the
        // configured BE-canonical creds. The real normalization (StorageProperties.createAll ->
        // getBackendPropertiesFromStorageMap) is covered by fe-core's DefaultConnectorContext tests.
        return (rawVendedCredentials == null || rawVendedCredentials.isEmpty())
                ? Collections.emptyMap() : vendedBeProps;
    }

    /** The type the wrapper forwarded to {@link #createSiblingConnector} (proves the decorator delegates it). */
    String lastSiblingType;
    /** The properties the wrapper forwarded to {@link #createSiblingConnector}. */
    Map<String, String> lastSiblingProps;

    @Override
    public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
        lastSiblingType = catalogType;
        lastSiblingProps = properties;
        return null;
    }

    @Override
    public long getCatalogId() {
        return 0;
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        authCount++;
        if (failAuth) {
            // Deliberately do NOT call task -> the wrapped seam call must not run.
            throw new RuntimeException("auth failed");
        }
        return task.call();
    }

    /** Locations the connector asked the engine to clean (B1 managed-location cleanup). */
    final List<String> cleanedLocations = new ArrayList<>();
    /** The child-dirs arg paired with each {@link #cleanedLocations} entry (same index). */
    final List<List<String>> cleanedChildDirs = new ArrayList<>();

    @Override
    public void cleanupEmptyManagedLocation(String location, List<String> tableChildDirs) {
        cleanedLocations.add(location);
        cleanedChildDirs.add(tableChildDirs);
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

}
