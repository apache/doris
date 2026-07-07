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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.spi.ConnectorBrokerAddress;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetaInvalidator;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.kerberos.HadoopAuthenticator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * A {@link ConnectorContext} decorator that wraps every {@link #executeAuthenticated} call, then delegates to
 * the wrapped engine context. Every other method is a pure pass-through. The paimon analogue of the iceberg
 * connector's {@code TcclPinningConnectorContext}, wrapping the single FE-injected context once covers every
 * remote read/DDL/commit ({@code PaimonConnectorMetadata} routes them all through
 * {@link ConnectorContext#executeAuthenticated}).
 *
 * <p>TCCL: the pin keeps reflective loads on the plugin side for the duration of each op. The paimon plugin
 * bundles paimon-core + {@code hadoop-common}/{@code hadoop-hdfs-client} child-first, so any name-based
 * reflective load that defaults to the thread-context classloader would otherwise resolve the parent (fe-core)
 * copy and ClassCast against the child-loaded plugin copy — the same split-brain guard the iceberg connector
 * applies. The pin is harmless for pure reads (it just runs them under the plugin loader).
 *
 * <p>KERBEROS (single-owner auth): for a Kerberos catalog {@code pluginAuthenticator} supplies a plugin-side
 * {@link HadoopAuthenticator} and the op runs inside its {@code doAs}. This is REQUIRED because the plugin
 * bundles its own {@code hadoop-common} + {@code fe-kerberos} child-first, so the plugin's HDFS
 * {@code FileSystem} reads a DIFFERENT {@code UserGroupInformation} copy than the one the FE-injected
 * authenticator (built app-side by the fe-core {@code MetastoreProperties}) logs in — the app-side
 * {@code doAs} therefore never reaches the plugin FileSystem, which falls back to SIMPLE auth. The connector
 * is the only party that knows which UGI copy its FileSystem uses, so it owns the auth: on the Kerberos path
 * we run the plugin {@code doAs} and DELIBERATELY do NOT also call {@code delegate.executeAuthenticated}
 * (which only authenticates the unused app-loader UGI — dead weight plus a redundant keytab login). The
 * plugin {@code doAs} mirrors {@code HadoopExecutionAuthenticator.execute}
 * ({@code hadoopAuthenticator.doAs(task::call)}), so exception semantics are unchanged. When the supplier
 * returns {@code null} (non-Kerberos) the FE-injected path is preserved byte-for-byte.
 *
 * <p>Note: paimon has no live Kerberos regression suite, so this is verified by wiring/static reasoning; the
 * end-to-end gate is the iceberg Kerberos suite, which exercises the identical mechanism.
 */
final class TcclPinningConnectorContext implements ConnectorContext {

    private final ConnectorContext delegate;
    private final ClassLoader pluginClassLoader;
    private final Supplier<HadoopAuthenticator> pluginAuthenticator;

    TcclPinningConnectorContext(ConnectorContext delegate, ClassLoader pluginClassLoader,
            Supplier<HadoopAuthenticator> pluginAuthenticator) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.pluginClassLoader = Objects.requireNonNull(pluginClassLoader, "pluginClassLoader");
        this.pluginAuthenticator = Objects.requireNonNull(pluginAuthenticator, "pluginAuthenticator");
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginClassLoader);
            HadoopAuthenticator auth = pluginAuthenticator.get();
            if (auth == null) {
                // Non-Kerberos: keep the FE-injected auth path exactly as-is.
                return delegate.executeAuthenticated(task);
            }
            // Kerberos: the connector is the sole authenticator. Run the op under the PLUGIN's UGI copy (the
            // one the plugin's FileSystem reads); do NOT also invoke the FE-injected app-side authenticator.
            return auth.doAs(task::call);
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
    public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
        // Delegate to the raw engine context (not this wrapper): the sibling connector applies its OWN
        // TCCL/auth pinning over the context it is handed, so it must receive the unwrapped context to avoid
        // double-pinning to this plugin's loader. Keeps this decorator a true exhaustive pass-through.
        return delegate.createSiblingConnector(catalogType, properties);
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
