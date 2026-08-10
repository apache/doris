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

import org.apache.doris.connector.spi.DorisConnectorException;

import java.util.LinkedHashMap;
import java.util.Locale;
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
 *
 * <p><b>What the table properties cannot contain is credentials.</b> The fluss cluster removes every lake
 * option whose name contains {@code key}, {@code secret} or {@code password} before it answers a metadata
 * request, so {@code s3.access-key} and its equivalents never leave the cluster. This is not a Doris-side
 * omission and there is no alias to fix: a lake on authenticated storage is readable only if the CATALOG
 * supplies those settings, which is what {@link FlussCatalogProperties#LAKE_OPTION_PREFIX} is for.
 *
 * <p><b>Storage settings do not appear in the result at all</b>, whichever side they came from: they
 * configure the catalog's storage rather than its lake catalog, and go to the engine's storage layer
 * instead ({@link LakeStorageOptions}).
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

    // The metastore flavors, in paimon's spelling (left) and Doris's paimon connector's (right). Only the
    // names differ; paimon's "hive" and Doris's "hms" are the same metastore. Paimon also has "jdbc",
    // deliberately not served here: nothing has read a lake through it, and an untried flavor is better
    // refused by name than half-supported.
    private static final String PAIMON_FILESYSTEM = "filesystem";
    private static final String PAIMON_HIVE = "hive";
    private static final String PAIMON_REST = "rest";
    private static final String DORIS_FILESYSTEM = "filesystem";
    private static final String DORIS_HMS = "hms";
    private static final String DORIS_REST = "rest";

    private PaimonSiblingProperties() {
    }

    /**
     * Returns a NEW paimon catalog-property map derived from one fluss table's properties, with the
     * catalog's own lake settings applied over them. Neither input is mutated nor aliased into the result.
     *
     * <p>The catalog wins, key by key: a catalog that states only what its cluster cannot report keeps
     * everything else the cluster does report. Both arguments are required — there is no single-argument
     * form — because the overrides have to be applied at every place a sibling is configured, and one call
     * site that forgot them would build a second, differently configured sibling for the same catalog.
     *
     * <p>The override happens BEFORE the checks below, not after: "the cluster reports no warehouse and the
     * catalog supplies one" is exactly the configuration this is for, and checking first would reject it.
     *
     * <p>Fails loud on a lake configuration this connector cannot serve, rather than handing the paimon
     * connector a half-built map and letting it fail with a message about a catalog nobody created.
     */
    static Map<String, String> synthesize(Map<String, String> flussTableProperties,
            Map<String, String> catalogLakeOverrides) {
        Map<String, String> lakeOptions = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : flussTableProperties.entrySet()) {
            if (entry.getKey().startsWith(LAKE_OPTION_PREFIX)) {
                lakeOptions.put(entry.getKey().substring(LAKE_OPTION_PREFIX.length()), entry.getValue());
            }
        }
        lakeOptions.putAll(catalogLakeOverrides);
        // Storage is configured once per catalog and read by both the FE and the BE; the sibling gets the
        // lake catalog's settings only. See LakeStorageOptions for why leaving these in would be worse
        // than dropping them.
        lakeOptions.keySet().removeIf(LakeStorageOptions::isStorageOption);

        String catalogType = dorisCatalogType(lakeOptions.remove(FLUSS_METASTORE));

        String warehouse = lakeOptions.remove(WAREHOUSE);
        if (warehouse == null || warehouse.isEmpty()) {
            throw new DorisConnectorException(
                    "Cannot read the lake table: the fluss table carries no '"
                            + LAKE_OPTION_PREFIX + WAREHOUSE
                            + "'. Either the fluss cluster configures datalake.paimon." + WAREHOUSE
                            + ", or the catalog states it as '"
                            + FlussCatalogProperties.LAKE_OPTION_PREFIX + WAREHOUSE + "'");
        }

        // LinkedHashMap so a failure message or a log line renders the same order every time.
        Map<String, String> siblingProperties = new LinkedHashMap<>();
        siblingProperties.put(PAIMON_CATALOG_TYPE, catalogType);
        siblingProperties.put(WAREHOUSE, warehouse);
        siblingProperties.putAll(lakeOptions);
        return siblingProperties;
    }

    /**
     * Doris's name for the paimon metastore flavor {@code flussMetastore} names, which is the same
     * metastore under a different spelling.
     *
     * <p>An absent flavor is paimon's own default, filesystem. The catalog type is then stated explicitly
     * anyway: the paimon connector defaults on its own, and leaving it out would make what this lake is
     * read as depend on that default staying put.
     *
     * <p>The rest of a flavor's configuration is not checked here. Each one has its own required and
     * mutually exclusive settings (a metastore uri, kerberos, REST tokens), and the paimon connector
     * enforces them when it binds this map — a second copy of those rules would drift from the first and
     * reject configurations paimon accepts. Only the flavor itself is checked, because it is the one thing
     * this side chooses.
     */
    private static String dorisCatalogType(String flussMetastore) {
        if (flussMetastore == null || flussMetastore.trim().isEmpty()) {
            return DORIS_FILESYSTEM;
        }
        switch (flussMetastore.trim().toLowerCase(Locale.ROOT)) {
            case PAIMON_FILESYSTEM:
                return DORIS_FILESYSTEM;
            case PAIMON_HIVE:
                return DORIS_HMS;
            case PAIMON_REST:
                return DORIS_REST;
            default:
                // Exhaustive on purpose: a flavor added to the left has to be given a Doris name here,
                // and until it is, a lake behind it is refused by name rather than read as a filesystem.
                throw new DorisConnectorException(
                        "Cannot read the lake table: its paimon catalog is a '" + flussMetastore
                                + "' one (datalake.paimon." + FLUSS_METASTORE
                                + "), and the fluss connector supports " + PAIMON_FILESYSTEM + ", "
                                + PAIMON_HIVE + " and " + PAIMON_REST);
        }
    }
}
