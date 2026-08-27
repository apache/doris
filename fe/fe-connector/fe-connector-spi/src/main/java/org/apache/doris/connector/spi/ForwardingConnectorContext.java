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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

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
 *
 * <p>Storage services need no forward of their own: {@link ConnectorContext#getStorageContext()} hands the
 * connector the engine's own {@link ConnectorStorageContext}, so however many are added, none can be lost
 * here. That is sound only while no storage method runs plugin code (none does today — see
 * {@link ConnectorStorageContext}); one that did would need a pinning subclass to override
 * {@code getStorageContext()} and return a pinning wrapper of its own.
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
    public Map<String, String> getConnectorConfig() {
        return delegate.getConnectorConfig();
    }

    @Override
    public ConnectorHttpSecurityHook getHttpSecurityHook() {
        return delegate.getHttpSecurityHook();
    }

    @Override
    public String sanitizeOutboundUrl(String url) {
        return delegate.sanitizeOutboundUrl(url);
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        return delegate.executeAuthenticated(task);
    }

    @Override
    public Connector createSiblingConnector(String catalogType, Map<String, String> properties) {
        return delegate.createSiblingConnector(catalogType, properties);
    }

    @Override
    public ConnectorStorageContext getStorageContext() {
        return delegate.getStorageContext();
    }

    @Override
    public ConnectorMetadataAccessObserver getMetadataAccessObserver() {
        return delegate.getMetadataAccessObserver();
    }
}
