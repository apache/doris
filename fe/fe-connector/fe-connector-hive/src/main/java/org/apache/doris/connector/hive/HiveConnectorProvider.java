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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;

/**
 * SPI entry point for the Hive (HMS) connector plugin.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * The type is {@code "hms"} to match the existing catalog type in CatalogFactory.</p>
 */
public class HiveConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "hms";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new HiveConnector(properties, context);
    }

    @Override
    public void validateProperties(Map<String, String> properties) {
        // Reject removed metastore types at CREATE/ALTER CATALOG. This runs only for a user-issued statement,
        // never during edit-log replay, so an already-created glue catalog cannot block FE startup here; it is
        // rejected later, at HiveConnector.createClient(). IllegalArgumentException is required: it is the only
        // type PluginDrivenExternalCatalog.checkProperties unwraps, preserving the message verbatim.
        String removedType = HmsClientConfig.removedMetastoreTypeError(properties);
        if (removedType != null) {
            throw new IllegalArgumentException(removedType);
        }

        // Restore the legacy HMSExternalCatalog.checkProperties fail-fast for the two meta-cache TTL knobs:
        // after the hms cutover an "hms" catalog is created via this SPI provider (not HMSExternalCatalog), so
        // the old per-property validation no longer runs and an invalid ttl (e.g. -2) was silently accepted.
        // Legacy semantics: the value, when present, must be a long >= 0 (CACHE_TTL_DISABLE_CACHE); < 0 is
        // rejected. checkLongProperty emits the identical "The parameter ... is wrong, value is ..." message.
        CacheSpec.checkLongProperty(properties.get("file.meta.cache.ttl-second"),
                0L, "file.meta.cache.ttl-second");
        CacheSpec.checkLongProperty(properties.get("partition.cache.ttl-second"),
                0L, "partition.cache.ttl-second");
    }
}
