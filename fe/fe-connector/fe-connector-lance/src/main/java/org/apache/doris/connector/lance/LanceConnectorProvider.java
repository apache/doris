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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSecuritySurface;

import java.util.Map;

/**
 * SPI entry point for the Lance connector.
 * Discovered via META-INF/services/org.apache.doris.connector.spi.ConnectorProvider.
 *
 * <p><b>Class-loading invariant (guarded by {@code LanceConnectorProviderIsolationTest}): neither this
 * class's static initializer nor its no-arg constructor may touch {@code org.lance.*} or the JNI loader
 * ({@code io.questdb.*}), directly or transitively.</b> The plugin loader instantiates every discovered
 * factory before it checks for a duplicate type name, so a second copy of this provider is constructed and
 * then discarded. A Lance class touched there would extract and load the native library once more from a
 * different classloader -- lance-core copies it to a fresh temporary file each time, so nothing fails, and a
 * second Rust runtime of about 180 MB stays mapped in the FE without anyone noticing.
 */
public class LanceConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "lance";
    }

    /** Binds and validates the properties this connector owns; the checking is the point. */
    @Override
    public void validateProperties(Map<String, String> properties) {
        LanceCatalogProperties.of(properties);
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new LanceConnector(properties, context);
    }

    /**
     * The REST namespace server, which the engine's SSRF gate probes at CREATE / ALTER CATALOG, and the REST
     * credentials, which SHOW CREATE CATALOG masks; see {@link LanceCatalogProperties#SECURITY_SURFACE}.
     */
    @Override
    public ConnectorSecuritySurface securitySurface() {
        return LanceCatalogProperties.SECURITY_SURFACE;
    }
}
