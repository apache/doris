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
import org.apache.doris.connector.api.cache.CacheSpec;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Collections;
import java.util.Map;

/**
 * SPI entry point for the Iceberg connector plugin.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * The type is {@code "iceberg"} to match the existing catalog type in CatalogFactory.
 * Internally dispatches to all Iceberg catalog backends (REST, HMS, Glue, DLF,
 * JDBC, Hadoop, S3Tables) via the Iceberg SDK's {@code CatalogUtil}.</p>
 */
public class IcebergConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "iceberg";
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new IcebergConnector(properties, context);
    }

    /**
     * Validates catalog properties at CREATE CATALOG time via the shared metastore parsers (P6-T10): the
     * flavor is resolved from {@code iceberg.catalog.type} ({@link IcebergCatalogFactory#resolveFlavor}) and
     * {@link MetaStoreProviders#bindForType} selects the iceberg backend, whose {@code validate()} enforces
     * the per-flavor fail-fast rules — REST (security/creds enums, OAuth2, signing, AK/SK), Glue
     * (AK/SK-together, endpoint https, at-least-one-credential), JDBC (uri/catalog_name/warehouse), and the
     * shared HMS/DLF connection checks; hadoop/s3tables are no-op (their storage is validated upstream at
     * fe-filesystem bind). Storage is not needed for validation, so an empty storage map is passed. A blank
     * or unknown {@code iceberg.catalog.type} makes {@code bindForType} throw (no provider supports it).
     * Throws {@link IllegalArgumentException}, which {@code PluginDrivenExternalCatalog.checkProperties}
     * wraps into a DdlException.
     *
     * <p>The meta-cache knobs are validated first (restoring the legacy
     * {@code IcebergExternalCatalog.checkProperties} fail-fast that was dropped at the SPI cutover), so a
     * bad {@code meta.cache.iceberg.*} value is rejected at CREATE/ALTER instead of being silently
     * coerced to a cache-disabling default.
     */
    @Override
    public void validateProperties(Map<String, String> properties) {
        checkMetaCacheProperties(properties);
        String flavor = IcebergCatalogFactory.resolveFlavor(properties);
        MetaStoreProviders.bindForType(flavor, properties, Collections.emptyMap()).validate();
    }

    /**
     * Byte-for-byte parity with the legacy {@code IcebergExternalCatalog.checkProperties}: table/manifest
     * {@code enable} must be boolean, {@code ttl-second} must be a long &ge; -1 (the "no expiration"
     * sentinel), {@code capacity} must be a long &ge; 0. Absent keys are skipped.
     */
    private static void checkMetaCacheProperties(Map<String, String> properties) {
        CacheSpec.checkBooleanProperty(properties.get(IcebergConnectorProperties.TABLE_CACHE_ENABLE),
                IcebergConnectorProperties.TABLE_CACHE_ENABLE);
        CacheSpec.checkLongProperty(properties.get(IcebergConnectorProperties.TABLE_CACHE_TTL),
                -1L, IcebergConnectorProperties.TABLE_CACHE_TTL);
        CacheSpec.checkLongProperty(properties.get(IcebergConnectorProperties.TABLE_CACHE_CAPACITY),
                0L, IcebergConnectorProperties.TABLE_CACHE_CAPACITY);

        CacheSpec.checkBooleanProperty(properties.get(IcebergConnectorProperties.MANIFEST_CACHE_ENABLE),
                IcebergConnectorProperties.MANIFEST_CACHE_ENABLE);
        CacheSpec.checkLongProperty(properties.get(IcebergConnectorProperties.MANIFEST_CACHE_TTL),
                -1L, IcebergConnectorProperties.MANIFEST_CACHE_TTL);
        CacheSpec.checkLongProperty(properties.get(IcebergConnectorProperties.MANIFEST_CACHE_CAPACITY),
                0L, IcebergConnectorProperties.MANIFEST_CACHE_CAPACITY);
    }
}
