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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The catalog properties this connector interprets, bound and checked.
 *
 * <p><b>None of these keys belong to hudi.</b> There is no {@code type=hudi} catalog (see
 * {@link HudiConnectorProvider}): this connector is always an embedded sibling of an HMS gateway and
 * receives that gateway catalog's whole property map verbatim, so what it reads are the <i>hive</i>
 * catalog's keys. Cross-plugin keys can only be referenced by copying the literal with a comment
 * pointing at the owner -- the hive plugin's classes are not visible from here -- which is what the
 * constants below do.
 *
 * <p>{@link #of(Map)} binds, derives and validates in one step, so an instance that exists has valid
 * properties and every reader downstream uses a getter instead of re-parsing the map. It performs no
 * I/O and is idempotent: the connector is rebuilt lazily on every catalog refresh and again on an FE
 * replaying the edit log.
 *
 * <p><b>Unknown keys are accepted, always.</b> The map is the gateway catalog's, so it carries hive's
 * own keys, the engine's ({@code type}, {@code meta.cache.*}) and storage's ({@code s3.*},
 * {@code dfs.*}, ...); and {@code ALTER CATALOG} merges properties -- it can overwrite a key but never
 * remove one, so a key refused here would leave a catalog no statement could repair. Bad <i>values</i>
 * are refused; unrecognized <i>names</i> are not.
 */
public final class HudiCatalogProperties {

    /**
     * The Hive Metastore thrift URI. Owned by the hive connector
     * ({@code HiveCatalogProperties.HIVE_METASTORE_URIS}); the literal is copied here because the
     * gateway's plugin classes are not visible from this one.
     */
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    /** The short form of {@link #HIVE_METASTORE_URIS}, accepted as a fallback exactly as hive accepts it. */
    public static final String URI = "uri";

    /**
     * Size of the metastore client pool. Owned by the hive connector
     * ({@code HiveCatalogProperties.HMS_CLIENT_POOL_SIZE}) -- see {@link #HIVE_METASTORE_URIS} for why
     * the literal is copied.
     */
    public static final String HMS_CLIENT_POOL_SIZE = "hive.metastore.client.pool.size";

    /**
     * Take partition names from the metastore (hive-sync) rather than from the table's own filesystem
     * layout. Owned by the hive catalog (legacy {@code HMSExternalTable.USE_HIVE_SYNC_PARTITION}).
     */
    public static final String USE_HIVE_SYNC_PARTITION = "use_hive_sync_partition";

    private static final int DEFAULT_HMS_CLIENT_POOL_SIZE = 8;

    @ConnectorProperty(names = {HIVE_METASTORE_URIS, URI},
            description = "Hive Metastore thrift URI; 'uri' is accepted as the short form")
    private String metastoreUri;

    @ConnectorProperty(names = {HMS_CLIENT_POOL_SIZE}, required = false,
            description = "size of the metastore client pool")
    private int hmsClientPoolSize = DEFAULT_HMS_CLIENT_POOL_SIZE;

    @ConnectorProperty(names = {HmsClientConfig.PARTITION_BATCH_SIZE_KEY}, required = false,
            description = "maximum partition names sent in one Hive Metastore RPC")
    private int hmsPartitionsBatchSizePerRpc = HmsClientConfig.DEFAULT_PARTITION_BATCH_SIZE;

    @ConnectorProperty(names = {USE_HIVE_SYNC_PARTITION}, required = false,
            description = "read partition names from the metastore instead of the table's file layout")
    private boolean useHiveSyncPartition;

    private final Map<String, String> raw;

    private HudiCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    public static HudiCatalogProperties of(Map<String, String> properties) {
        HudiCatalogProperties p = new HudiCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        new ParamRules()
                .require(p.metastoreUri,
                        "HMS URI ('" + HIVE_METASTORE_URIS + "') is required for Hudi connector")
                .validate();
        new HmsClientConfig(p.raw, p.hmsClientPoolSize);
        return p;
    }

    public String getMetastoreUri() {
        return metastoreUri;
    }

    /**
     * A malformed value fails here rather than falling back to the default, per the migration rule for
     * a typed holder. Note the asymmetry this creates while the hive connector still parses the same key
     * leniently: on a catalog whose pool size is misspelled, hive tables keep working and hudi tables do
     * not, until an {@code ALTER CATALOG} overwrites the value.
     */
    public int getHmsClientPoolSize() {
        return hmsClientPoolSize;
    }

    public boolean isUseHiveSyncPartition() {
        return useHiveSyncPartition;
    }

    /**
     * The gateway catalog's properties as written, unmodifiable. It is the storage/kerberos passthrough
     * the Hadoop {@code Configuration} and the metastore client are built from, so it goes on being
     * handed around whole; the keys above are the ones this connector itself interprets.
     */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
