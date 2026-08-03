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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.DorisConnectorException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Synthesizes the catalog-property map for the embedded paimon <em>sibling</em> connector that serves a
 * fluss lake table ({@code tbl$lake}). Mirrors the hive gateway's {@code HudiSiblingProperties} /
 * {@code IcebergSiblingProperties}: the sibling is built through
 * {@code ConnectorContext.createSiblingConnector("paimon", synthesize(...))} so its classes come from the
 * paimon plugin's own loader — the fluss plugin bundles no paimon at all.
 *
 * <p>The one structural difference from hive: hive's lake configuration is part of the CATALOG properties
 * the user typed, so its synthesis is a verbatim copy. Fluss's lives in the TABLE properties instead — the
 * fluss coordinator merges the cluster-level {@code datalake.paimon.*} settings into every datalake table's
 * properties under a {@code table.} prefix, and only while {@code table.datalake.enabled} is true
 * ({@code TableRegistration#toTableInfo}). So the input here is a table's property map and the mapping has
 * to strip that prefix:
 *
 * <pre>
 *   fluss cluster config      datalake.paimon.warehouse = /lake
 *   fluss table property      table.datalake.paimon.warehouse = /lake
 *   paimon catalog property   warehouse = /lake
 * </pre>
 *
 * <p>Two keys are renamed because Doris's paimon connector spells them differently from paimon itself;
 * everything else is forwarded with only the prefix removed. That verbatim tail is what carries a real
 * deployment's storage keys ({@code fs.*} / {@code dfs.*} / {@code hadoop.*}), which the paimon connector
 * reads under exactly those names.
 */
final class PaimonSiblingProperties {

    /**
     * The prefix the fluss coordinator gives its injected paimon lake settings inside a table's property
     * map.
     */
    private static final String LAKE_OPTION_PREFIX = "table.datalake.paimon.";

    /** Paimon's own name for the metastore flavor; Doris's paimon connector calls it paimon.catalog.type. */
    private static final String FLUSS_METASTORE = "metastore";
    private static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";

    /** Same name on both sides, but required, so it is handled explicitly rather than by the tail. */
    private static final String WAREHOUSE = "warehouse";

    /** The only metastore flavor this connector serves today (see the class comment of the gate). */
    private static final String FILESYSTEM = "filesystem";

    private PaimonSiblingProperties() {
    }

    /**
     * Returns a NEW paimon catalog-property map derived from one fluss table's properties. The input is
     * never mutated and is not aliased into the result.
     *
     * <p>Fails loud on a lake configuration this connector cannot serve, rather than handing the paimon
     * connector a half-built map and letting it fail with a message about a catalog nobody created.
     */
    static Map<String, String> synthesize(Map<String, String> flussTableProperties) {
        Map<String, String> lakeOptions = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : flussTableProperties.entrySet()) {
            if (entry.getKey().startsWith(LAKE_OPTION_PREFIX)) {
                lakeOptions.put(entry.getKey().substring(LAKE_OPTION_PREFIX.length()), entry.getValue());
            }
        }

        String metastore = lakeOptions.remove(FLUSS_METASTORE);
        // Absent means paimon's own default, which is filesystem — the same thing this connector supports.
        if (metastore != null && !FILESYSTEM.equalsIgnoreCase(metastore)) {
            throw new DorisConnectorException(
                    "Cannot read the lake table: its fluss cluster configures a '" + metastore
                            + "' paimon catalog (datalake.paimon." + FLUSS_METASTORE
                            + "), and the fluss connector currently supports only '" + FILESYSTEM + "'");
        }

        String warehouse = lakeOptions.remove(WAREHOUSE);
        if (warehouse == null || warehouse.isEmpty()) {
            throw new DorisConnectorException(
                    "Cannot read the lake table: the fluss table carries no '"
                            + LAKE_OPTION_PREFIX + WAREHOUSE
                            + "'. The fluss cluster must configure datalake.paimon." + WAREHOUSE
                            + " for its lake tables to be readable");
        }

        // LinkedHashMap so a failure message or a log line renders the same order every time.
        Map<String, String> siblingProperties = new LinkedHashMap<>();
        siblingProperties.put(PAIMON_CATALOG_TYPE, FILESYSTEM);
        siblingProperties.put(WAREHOUSE, warehouse);
        siblingProperties.putAll(lakeOptions);
        return siblingProperties;
    }
}
