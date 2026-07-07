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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetaInvalidator;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TFileType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Hand-written {@link ConnectorContext} test double (no Mockito), adapted verbatim from the paimon
 * connector's {@code RecordingConnectorContext}.
 *
 * <p>{@link IcebergConnectorMetadata} takes a context (ctor {@code (IcebergCatalogOps, Map,
 * ConnectorContext)}) and wraps every remote read in {@link #executeAuthenticated}; the read tests use
 * this double to assert one wrap per op via {@link #authCount}, and that {@link #getStorageProperties} /
 * {@link #loadHiveConfResources} are threaded through. When {@link #failAuth} is set,
 * {@link #executeAuthenticated} throws WITHOUT invoking the task, which proves the seam call sits INSIDE
 * the authenticator.
 */
final class RecordingConnectorContext implements ConnectorContext {

    int authCount;
    boolean failAuth;

    /** Map the fake returns from {@link #loadHiveConfResources} (the "resolved" hive-site.xml keys). */
    Map<String, String> hiveConfResources = Collections.emptyMap();
    /** Whether the connector invoked {@link #loadHiveConfResources}. */
    boolean hiveConfResourcesCalled;
    /** The {@code resources} string the connector passed to {@link #loadHiveConfResources}. */
    String lastHiveConfResourcesArg;

    /** Storage properties the fake returns from {@link #getStorageProperties()} — the typed fe-filesystem
     * seam both the scan and (design S3) the write path derive their BE-canonical static creds from via
     * {@code sp.toBackendProperties().toMap()} (default: none). */
    List<StorageProperties> storageProperties = Collections.emptyList();

    /** BE-canonical vended creds the fake returns from {@link #vendStorageCredentials} for a NON-EMPTY token
     * (an empty/null token -> empty result, mirroring {@code DefaultConnectorContext} — so a test can prove the
     * catalog-flag GATE end-to-end: flag off -> empty token -> no vended {@code location.*}). */
    Map<String, String> vendedBeProps = Collections.emptyMap();

    /** Raw URIs the connector routed through {@link #normalizeStorageUri} (data/delete-path normalization). */
    final List<String> normalizedUris = new ArrayList<>();
    /** Number of times the connector invoked {@link #normalizeStorageUri} (1- or 2-arg). */
    int normalizeCount;
    /** The vended token the connector passed to the most recent 2-arg {@link #normalizeStorageUri} (T09). */
    Map<String, String> lastVendedToken;

    /** BE file type the fake returns from {@link #getBackendFileType} (T06 iceberg write sink). */
    TFileType backendFileType = TFileType.FILE_S3;
    /** The vended token the connector passed to the most recent {@link #getBackendFileType}. */
    Map<String, String> lastFileTypeVendedToken;

    /** Broker addresses the fake returns from {@link #getBrokerAddresses()} (broker write sink). Default none,
     * so a FILE_BROKER write fails loud ("No alive broker.") unless a test populates it. */
    List<ConnectorBrokerAddress> brokerAddresses = Collections.emptyList();

    /** "db.table" keys the connector invalidated via {@link #getMetaInvalidator()} (P6.4 procedure dispatch). */
    final List<String> invalidatedTables = new ArrayList<>();

    @Override
    public ConnectorMetaInvalidator getMetaInvalidator() {
        return new ConnectorMetaInvalidator() {
            @Override
            public void invalidateTable(String dbName, String tableName) {
                invalidatedTables.add(dbName + "." + tableName);
            }
        };
    }

    @Override
    public String getCatalogName() {
        return "test";
    }

    @Override
    public String getBackendFileType(String rawUri, Map<String, String> vendedToken) {
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
    public List<StorageProperties> getStorageProperties() {
        return storageProperties;
    }

    @Override
    public Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) {
        // Mirror DefaultConnectorContext: an empty/null token yields no overlay; a non-empty token yields the
        // configured BE-canonical creds. The real normalization (StorageProperties.createAll ->
        // getBackendPropertiesFromStorageMap) is covered by fe-core's DefaultConnectorContext tests.
        return (rawVendedCredentials == null || rawVendedCredentials.isEmpty())
                ? Collections.emptyMap() : vendedBeProps;
    }

    @Override
    public Map<String, String> loadHiveConfResources(String resources) {
        hiveConfResourcesCalled = true;
        lastHiveConfResourcesArg = resources;
        return hiveConfResources;
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
}
