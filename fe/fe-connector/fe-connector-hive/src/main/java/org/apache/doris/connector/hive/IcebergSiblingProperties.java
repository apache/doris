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

import java.util.HashMap;
import java.util.Map;

/**
 * Synthesizes the catalog-property map for the embedded Iceberg <em>sibling</em> connector that a flipped HMS
 * gateway delegates its iceberg-on-HMS tables to.
 *
 * <p>The sibling is built once per gateway catalog (not per table) via
 * {@code ConnectorContext.createSiblingConnector("iceberg", synthesize(catalogProps))}, sharing the gateway's
 * context (metastore auth + storage). The only synthesis needed is to declare the iceberg catalog <b>flavor</b>
 * as {@code hms}: the Iceberg connector then reads {@code hive.metastore.uris}/{@code uri},
 * {@code hive.conf.resources}, and the raw {@code hive.*}/{@code fs.*}/{@code dfs.*}/{@code hadoop.*} storage +
 * kerberos passthrough straight from this map. That is the SAME map shape a native
 * {@code type=iceberg, iceberg.catalog.type=hms} catalog already hands the connector — fe-core builds that
 * connector from the <em>full</em> catalog property map ({@code PluginDrivenExternalCatalog
 * .createConnectorFromProperties}) — so carrying the gateway catalog's whole property map verbatim and injecting
 * the flavor is both sufficient and exactly what the connector expects. Carrying the whole map (rather than a
 * hand-picked subset) is also the robust choice: it cannot silently drop a connectivity key (the {@code uri}
 * short form, an HDFS-HA {@code dfs.*} set, an S3 endpoint override, a kerberos variant, …). The connector
 * ignores keys it does not recognize; its {@code create()} path does no property validation.
 *
 * <p>The flavor key/value are hardcoded literals on purpose: the Iceberg connector's
 * {@code IcebergConnectorProperties} constants live in the iceberg plugin's child-first classloader and are not
 * visible from the hive loader.
 */
final class IcebergSiblingProperties {

    // Literals of the iceberg-plugin IcebergConnectorProperties.ICEBERG_CATALOG_TYPE / TYPE_HMS: those constants
    // live in the iceberg plugin's child-first classloader and are not visible from the hive loader.
    static final String ICEBERG_CATALOG_TYPE_KEY = "iceberg.catalog.type";
    static final String ICEBERG_CATALOG_TYPE_HMS = "hms";

    private IcebergSiblingProperties() {
    }

    /**
     * Returns a NEW property map = the gateway catalog's properties verbatim with the iceberg catalog flavor
     * forced to {@code hms}. The input is never mutated (the gateway holds it unmodifiable and shared). An
     * existing {@code iceberg.catalog.type} is overridden unconditionally — an iceberg-on-HMS sibling is always
     * the hms flavor.
     */
    static Map<String, String> synthesize(Map<String, String> gatewayCatalogProperties) {
        Map<String, String> siblingProperties = new HashMap<>(gatewayCatalogProperties);
        siblingProperties.put(ICEBERG_CATALOG_TYPE_KEY, ICEBERG_CATALOG_TYPE_HMS);
        return siblingProperties;
    }
}
