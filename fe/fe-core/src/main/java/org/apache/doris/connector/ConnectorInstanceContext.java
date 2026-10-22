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

package org.apache.doris.connector;

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ForwardingConnectorContext;

import java.util.Map;

/** One construction/instance boundary, shared by its decorators but never by another connector instance. */
final class ConnectorInstanceContext extends ForwardingConnectorContext {
    private final String provider;
    private final Map<String, String> properties;
    private final ClassLoader loader;
    private CatalogMetaCache domain;

    ConnectorInstanceContext(ConnectorContext delegate, String provider, Map<String, String> properties,
            ClassLoader loader) {
        super(delegate);
        this.provider = provider;
        this.properties = properties;
        this.loader = loader;
    }

    @Override
    public synchronized CatalogMetaCache createCacheDomain(String engine, Map<String, String> properties) {
        if (domain == null) {
            domain = delegate() instanceof ConnectorInstanceContext
                    ? ((ConnectorInstanceContext) delegate()).childDomain(engine, loader)
                    : delegate().createCacheDomain(engine, properties);
        }
        if (domain.isClosed()) {
            throw new IllegalStateException("Connector cache domain has been retired");
        }
        return domain;
    }

    private synchronized CatalogMetaCache childDomain(String engine, ClassLoader loader) {
        return createCacheDomain(provider, properties).openDomain(engine, ScopePath.Coordinates.REMOTE, loader);
    }

    @Override
    public Connector createSiblingConnector(String type, Map<String, String> properties) {
        synchronized (this) {
            createCacheDomain(provider, this.properties);
        }
        return ConnectorFactory.createConnector(type, properties, this);
    }

    void rollback() {
        CatalogMetaCache captured;
        synchronized (this) {
            captured = domain;
        }
        if (captured != null) {
            captured.close();
        }
    }
}
