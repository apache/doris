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

package org.apache.doris.connectorconf.testplugins;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSecuritySurface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * A connector provider that builds its security surface from backends it discovers through its own
 * {@link ServiceLoader}, the way the Hive, Iceberg and Paimon providers declare their metastore backends' secret
 * keys through {@code MetaStoreProviders}. Its class bytes are copied into a temporary jar, so a test can load it
 * the way FE loads a shipped connector.
 *
 * <p>When a service descriptor in the provider's jars names a backend that cannot be loaded, the lookup throws
 * {@link java.util.ServiceConfigurationError}, which is an {@link Error} and neither a {@link RuntimeException}
 * nor a {@link LinkageError}.
 *
 * <p>It lives outside {@code org.apache.doris.connector.} for the reason {@link ConfProbeConnectorProviderA} does.
 */
public class BackendDiscoveringConnectorProvider implements ConnectorProvider {

    public static final String TYPE = "backend_discovering";

    /** What the provider's backends implement; its jars register them under this interface's name. */
    public interface Backend {
        String sensitiveKey();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        throw new UnsupportedOperationException(TYPE + " exists to be admitted or skipped at load time");
    }

    @Override
    public ConnectorSecuritySurface securitySurface() {
        List<String> keys = new ArrayList<>();
        for (Backend backend : ServiceLoader.load(Backend.class, getClass().getClassLoader())) {
            keys.add(backend.sensitiveKey());
        }
        return ConnectorSecuritySurface.EMPTY.withSensitiveKeys(keys);
    }
}
