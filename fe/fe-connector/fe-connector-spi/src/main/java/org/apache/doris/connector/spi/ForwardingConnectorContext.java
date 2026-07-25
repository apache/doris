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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.UnaryOperator;

/**
 * Base class for a {@link ConnectorContext} decorator: forwards every method to the wrapped context.
 * A decorator that needs to do something extra on one method (pinning the thread-context classloader
 * to the plugin loader, say) overrides just that method; this class guarantees the rest still reach
 * the engine.
 *
 * <p><b>Why a base class rather than hand-written pass-throughs.</b> Nearly every method on
 * {@link ConnectorContext} has a default implementation whose semantics are a SILENT downgrade —
 * {@code getFileSystem} returns {@code null}, {@code executeAuthenticated} runs the task with no
 * authentication at all, {@code newStorageUriNormalizer} drops the per-scan memoization,
 * {@code getStorageProperties} returns nothing. A decorator that implements the interface directly and
 * copies each method by hand therefore fails OPEN: forget one and the call quietly lands on the
 * interface default instead of the engine, with no compiler complaint and, for a classloader-pinning
 * decorator, no pin either. The failure surfaces far away — for {@code getFileSystem} it looks like a
 * NullPointerException, or like "this catalog has no storage properties" if the caller checks for
 * null, which points at catalog configuration that is in fact perfectly fine.
 *
 * <p><b>When you add a method to {@link ConnectorContext}, add a forward here too.</b>
 * {@code ForwardingConnectorContextTest} enforces this. And if the new method can run plugin code,
 * every pinning subclass must additionally override it and apply its pin — this class only promises
 * that no call is lost, not that every call is pinned.
 */
public abstract class ForwardingConnectorContext implements ConnectorContext {

    private final ConnectorContext delegate;

    protected ForwardingConnectorContext(ConnectorContext delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * The wrapped context. Subclasses use it to call through without re-entering their own decoration,
     * and to hand the undecorated engine context to anything that must not inherit this decorator.
     */
    protected final ConnectorContext delegate() {
        return delegate;
    }

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
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        return delegate.executeAuthenticated(task);
    }

    @Override
    public ConnectorMetaInvalidator getMetaInvalidator() {
        return delegate.getMetaInvalidator();
    }

    @Override
    public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
        return delegate.createSiblingConnector(catalogType, properties);
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
    public UnaryOperator<String> newStorageUriNormalizer(Map<String, String> rawVendedCredentials) {
        return delegate.newStorageUriNormalizer(rawVendedCredentials);
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
    public void testBackendStorageConnectivity(int storageBackendTypeValue,
            Map<String, String> backendProperties) throws Exception {
        delegate.testBackendStorageConnectivity(storageBackendTypeValue, backendProperties);
    }

    @Override
    public List<StorageProperties> getStorageProperties() {
        return delegate.getStorageProperties();
    }

    @Override
    public FileSystem getFileSystem(ConnectorSession session) {
        return delegate.getFileSystem(session);
    }

    @Override
    public void cleanupEmptyManagedLocation(String location, List<String> tableChildDirs) {
        delegate.cleanupEmptyManagedLocation(location, tableChildDirs);
    }
}
