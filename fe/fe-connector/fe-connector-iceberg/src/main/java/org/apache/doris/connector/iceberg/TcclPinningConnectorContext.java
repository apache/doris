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

import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetaInvalidator;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * A {@link ConnectorContext} decorator that pins the thread-context classloader (TCCL) to the iceberg plugin
 * classloader for the duration of every {@link #executeAuthenticated} call, then delegates to the wrapped
 * engine context. Every other method is a pure pass-through.
 *
 * <p>WHY: iceberg-aws builds its S3 client lazily on the FIRST remote output of a {@code commit()}
 * ({@code S3FileIO.newOutputFile} &rarr; {@code AwsClientFactories$DefaultAwsClientFactory.s3()} &rarr;
 * {@code HttpClientProperties.applyHttpClientConfigurations}), which resolves
 * {@code org.apache.iceberg.aws.ApacheHttpClientConfigurations} via {@code DynMethods}, whose default loader
 * IS the TCCL. The engine thread that drives a DDL / DML / procedure commit runs under the default 'app'
 * TCCL, so that reflective load returns the parent (fe-core) copy of the class and
 * {@link ClassCastException}s against the child-loaded plugin copy the rest of the iceberg-aws stack uses.
 * Pinning the TCCL to the plugin loader keeps every reflective load on the plugin side.
 *
 * <p>This is the write/DDL/procedure-path analogue of the SAME split-brain guard already applied on the scan
 * path ({@code PluginDrivenScanNode.onPluginClassLoader}) and the catalog-build path
 * ({@code IcebergConnector.buildCatalogAuthenticated}). All three iceberg write seams route their remote
 * {@code commit()} through {@link ConnectorContext#executeAuthenticated} — branch/tag DDL
 * ({@code IcebergConnectorMetadata}), INSERT/UPDATE/DELETE/MERGE commits
 * ({@code IcebergConnectorTransaction}), and the snapshot procedures/actions
 * ({@code IcebergProcedureOps.runInAuthScope}) — so wrapping the single injected context once covers them all.
 *
 * <p>The pin is harmless for pure reads (it just runs the read under the plugin loader, exactly as the
 * catalog-build path already does) and idempotent when nested inside {@code buildCatalogAuthenticated}'s own
 * pin, which targets the same loader.
 */
final class TcclPinningConnectorContext implements ConnectorContext {

    private final ConnectorContext delegate;
    private final ClassLoader pluginClassLoader;

    TcclPinningConnectorContext(ConnectorContext delegate, ClassLoader pluginClassLoader) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.pluginClassLoader = Objects.requireNonNull(pluginClassLoader, "pluginClassLoader");
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginClassLoader);
            return delegate.executeAuthenticated(task);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    // ----- pure delegation -----

    @Override
    public String getCatalogName() {
        return delegate.getCatalogName();
    }

    @Override
    public long getCatalogId() {
        return delegate.getCatalogId();
    }

    @Override
    public Map<String, String> getEnvironment() {
        return delegate.getEnvironment();
    }

    @Override
    public ConnectorHttpSecurityHook getHttpSecurityHook() {
        return delegate.getHttpSecurityHook();
    }

    @Override
    public String sanitizeJdbcUrl(String jdbcUrl) {
        return delegate.sanitizeJdbcUrl(jdbcUrl);
    }

    @Override
    public ConnectorMetaInvalidator getMetaInvalidator() {
        return delegate.getMetaInvalidator();
    }

    @Override
    public Map<String, String> loadHiveConfResources(String resources) {
        return delegate.loadHiveConfResources(resources);
    }

    @Override
    public Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) {
        return delegate.vendStorageCredentials(rawVendedCredentials);
    }

    @Override
    public String normalizeStorageUri(String rawUri) {
        return delegate.normalizeStorageUri(rawUri);
    }

    @Override
    public String normalizeStorageUri(String rawUri, Map<String, String> rawVendedCredentials) {
        return delegate.normalizeStorageUri(rawUri, rawVendedCredentials);
    }

    @Override
    public String getBackendFileType(String rawUri, Map<String, String> rawVendedCredentials) {
        return delegate.getBackendFileType(rawUri, rawVendedCredentials);
    }

    @Override
    public List<ConnectorBrokerAddress> getBrokerAddresses() {
        return delegate.getBrokerAddresses();
    }

    @Override
    public Map<String, String> getBackendStorageProperties() {
        return delegate.getBackendStorageProperties();
    }

    @Override
    public List<StorageProperties> getStorageProperties() {
        return delegate.getStorageProperties();
    }

    @Override
    public void cleanupEmptyManagedLocation(String location, List<String> tableChildDirs) {
        delegate.cleanupEmptyManagedLocation(location, tableChildDirs);
    }
}
