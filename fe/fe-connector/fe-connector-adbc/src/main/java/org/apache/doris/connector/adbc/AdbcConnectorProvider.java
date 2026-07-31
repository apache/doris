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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;

/**
 * SPI entry point for the ADBC connector.
 * Discovered via META-INF/services/org.apache.doris.connector.spi.ConnectorProvider.
 *
 * <p><b>Class-loading invariant (guarded by {@code AdbcConnectorProviderIsolationTest}): neither this
 * class's static initializer nor its no-arg constructor may touch {@code org.apache.arrow.adbc.*},
 * directly or transitively.</b> The plugin loader builds the plugin classloader and instantiates every
 * discovered factory BEFORE it checks for a duplicate type name
 * ({@code DirectoryPluginRuntimeManager#loadAll}), so when a classpath built-in and a directory plugin
 * both claim "adbc" the directory one is still constructed once and only then discarded. Touching an ADBC
 * class there would trigger a second {@code System.load} of the JNI shim from a different classloader and
 * throw {@code UnsatisfiedLinkError}. Production never hits that shape; test and embedded setups do.
 *
 * <p>Practically: keep the fields, the constructor and {@link #getType()} free of ADBC types, and let the
 * ADBC classes first appear inside {@link #create}, whose body is resolved only when it runs.
 */
public class AdbcConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "adbc";
    }

    /**
     * Cheap presence checks only. Resolving {@code driver_url} to a path needs {@code adbc_drivers_dir} and
     * {@code adbc_driver_secure_path}, which arrive through the connector context rather than the property
     * map, so that half runs in {@code AdbcConnector#preCreateValidation}.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        AdbcConnectorProperties.require(properties, AdbcConnectorProperties.DRIVER_URL);
        AdbcConnectorProperties.require(properties, AdbcConnectorProperties.URI);
        // Parsed for its exceptions: an unreadable value has to fail here, at CREATE CATALOG, and not on
        // the first query -- these two decide how a scan is planned, and a typo in either would otherwise
        // change that silently.
        AdbcConnectorProperties.partitionedReadMode(properties);
        AdbcConnectorProperties.maxPartitions(properties);
        checkMetaCacheProperties(properties);
    }

    /**
     * The cache knobs, which {@code CacheSpec} otherwise reads leniently -- an unparseable ttl or capacity
     * falls back to the default instead of failing. That is the wrong shape for a value an operator typed:
     * the catalog would come up caching for a duration nobody chose, and nothing would say so. Bounds match
     * the other connectors': ttl {@code >= -1} (-1 is "never expire"), capacity {@code >= 0} (0 disables).
     */
    private static void checkMetaCacheProperties(Map<String, String> properties) {
        CacheSpec.PropertySpec spec = AdbcMetadataCache.propertySpec();
        CacheSpec.checkBooleanProperty(properties.get(spec.getEnableKey()), spec.getEnableKey());
        CacheSpec.checkLongProperty(properties.get(spec.getTtlKey()), -1L, spec.getTtlKey());
        CacheSpec.checkLongProperty(properties.get(spec.getCapacityKey()), 0L, spec.getCapacityKey());
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new AdbcConnector(properties, context);
    }
}
