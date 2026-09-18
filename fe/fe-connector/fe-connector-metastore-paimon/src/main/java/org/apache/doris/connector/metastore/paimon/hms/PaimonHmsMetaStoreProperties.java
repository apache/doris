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

package org.apache.doris.connector.metastore.paimon.hms;

import org.apache.doris.connector.metastore.spi.AbstractHmsMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Paimon's Hive Metastore (HMS) backend. The fields, {@code toHiveConfOverrides}, and the shared
 * connection rules live in {@link AbstractHmsMetaStoreProperties}; paimon's {@link #validate()} adds the
 * {@code requireWarehouse()} that every paimon flavor enforces (legacy {@code AbstractPaimonProperties}),
 * then the shared connection check. Fire order (warehouse → uri → simple/kerberos auth) is byte-identical
 * to the pre-split {@code HmsMetaStorePropertiesImpl}.
 *
 * <p>The two fields below are paimon's own hms-flavor catalog options, so they live here rather than on
 * the engine-neutral base. They are declared as Strings, not as a long/boolean: paimon parses them
 * itself, and binding them to a number would turn a value paimon tolerates today into a catalog that
 * cannot be created.
 */
public final class PaimonHmsMetaStoreProperties extends AbstractHmsMetaStoreProperties {

    public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS = "client-pool-cache.eviction-interval-ms";
    public static final String LOCATION_IN_PROPERTIES = "location-in-properties";

    /** Legacy default: 5 minutes. */
    private static final String DEFAULT_CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS = "300000";
    private static final String DEFAULT_LOCATION_IN_PROPERTIES = "false";

    @ConnectorProperty(names = {CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS}, required = false,
            description = "How long paimon caches an idle hive client pool, in milliseconds.")
    private String clientPoolCacheEvictionIntervalMs = DEFAULT_CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;

    @ConnectorProperty(names = {LOCATION_IN_PROPERTIES}, required = false,
            description = "Whether paimon stores the table location in the hive table properties.")
    private String locationInProperties = DEFAULT_LOCATION_IN_PROPERTIES;

    private PaimonHmsMetaStoreProperties(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        super(raw, storageHadoopConfig);
    }

    public static PaimonHmsMetaStoreProperties of(Map<String, String> raw, Map<String, String> storageHadoopConfig) {
        PaimonHmsMetaStoreProperties props = new PaimonHmsMetaStoreProperties(raw, storageHadoopConfig);
        ConnectorPropertiesUtils.bindConnectorProperties(props, raw);
        return props;
    }

    @Override
    public void validate() {
        requireWarehouse();
        validateConnection();
    }

    /**
     * The hms-flavor catalog options, as neutral keys the connector turns into paimon {@code Options}
     * (this module stays free of the paimon SDK). Mirrors the legacy {@code appendHmsOptions}: the
     * metastore uri plus the two knobs above, all three emitted unconditionally.
     *
     * <p>The uri is the bound one, so it is alias-resolved and trimmed exactly like the value that
     * reaches the HiveConf through {@code toHiveConfOverrides} — the point of reading it from here
     * rather than re-scanning the raw map. A catalog with no uri at all cannot pass {@link #validate()},
     * so the empty string this would emit is only reachable on an image edited by hand.
     */
    public Map<String, String> toCatalogOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("uri", getUri());
        options.put(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS, clientPoolCacheEvictionIntervalMs);
        options.put(LOCATION_IN_PROPERTIES, locationInProperties);
        return options;
    }
}
