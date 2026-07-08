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
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.hms.ThriftHmsClient;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.KerberosAuthSpec;
import org.apache.doris.kerberos.KerberosAuthenticationConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Hive connector implementation. Manages the lifecycle of
 * an {@link HmsClient} for HMS operations and provides scan planning.
 */
public class HiveConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(HiveConnector.class);

    // Catalog property key gating the plugin-side Kerberos authenticator (value matches AuthType.KERBEROS).
    private static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    // The sibling connector type a flipped hms gateway delegates iceberg-on-HMS tables to. A string literal
    // (not the iceberg plugin's own type constant, which is child-first and invisible from the hive loader);
    // matches the "iceberg" entry in CatalogFactory.SPI_READY_TYPES.
    private static final String ICEBERG_CONNECTOR_TYPE = "iceberg";

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile HmsClient hmsClient;

    // Lazily-built plugin-side Kerberos authenticator (single-owner auth), null for a non-Kerberos catalog.
    // Its doAs acts on the PLUGIN's UserGroupInformation copy — the one the plugin's ThriftHmsClient RPC reads
    // (hadoop + fe-kerberos bundled child-first) — not the app-loader copy the FE-injected context would use.
    private volatile HadoopAuthenticator pluginAuth;
    private volatile boolean pluginAuthComputed;

    // Read-transaction manager for transactional (ACID) Hive scans. One per connector, keyed by query.
    // Plugin-owned and dormant until the read cutover wires its query-finish commit (see the manager).
    private final HiveReadTransactionManager readTxnManager = new HiveReadTransactionManager();

    // Embedded iceberg SIBLING connector: a flipped hms gateway delegates its iceberg-on-HMS tables to it. Built
    // once per gateway connector (lazily) in the iceberg plugin's OWN child-first classloader via
    // context.createSiblingConnector — never co-packaged into the hive zip (a second AWS SDK would poison S3
    // JVM-wide). Held ONLY as the parent-first Connector interface and NEVER cast: its concrete type is invisible
    // to the hive loader, so a cast would CCE across the loader split. Dormant until hms enters SPI_READY_TYPES —
    // nothing builds it today.
    private volatile Connector icebergSibling;

    public HiveConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        // Pass the sibling supplier (not the built sibling): a pure-hive query never invokes it, so a hive-only
        // deployment without the iceberg plugin never builds/throws. The metadata diverts a foreign iceberg
        // handle through it (per-handle guard-and-forward).
        return new HiveConnectorMetadata(getOrCreateClient(), properties, context, this::getOrCreateIcebergSibling);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new HiveScanPlanProvider(getOrCreateClient(), properties, readTxnManager);
    }

    /**
     * Selects the scan provider for a given table handle — the gateway seam a flipped hms catalog uses to serve
     * its iceberg-on-HMS tables from the embedded iceberg sibling. A hive handle (the gateway's OWN hive-loader
     * type) runs the hive scan provider; any foreign handle (the raw iceberg handle the sibling's getTableHandle
     * produced) is delegated to the sibling's per-handle scan provider. Because the returned sibling provider is
     * built in the iceberg plugin's classloader, {@code PluginDrivenScanNode.onPluginClassLoader} auto-pins the
     * scan-thread TCCL to the iceberg loader for free (it keys off {@code provider.getClass().getClassLoader()}),
     * so no pinning is needed here. The foreign handle is passed through UNMODIFIED and NEVER cast (its concrete
     * iceberg type is invisible across the loader split — a cast would CCE). A HUDI table keeps a HiveTableHandle,
     * so it stays on the hive scan path (its delegation is a later substep). Pairs with the getTableHandle iceberg
     * divert; dormant until hms enters SPI_READY_TYPES (nothing selects a scan provider for this connector today).
     */
    @Override
    public ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
        if (handle instanceof HiveTableHandle) {
            return getScanPlanProvider();
        }
        return getOrCreateIcebergSibling().getScanPlanProvider(handle);
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        return new HiveWritePlanProvider(getOrCreateClient(), properties, context);
    }

    @Override
    public Set<ConnectorCapability> getCapabilities() {
        // Connector-wide capabilities for the flipped hms catalog, each a faithful port of a legacy
        // HMSExternalTable/HMS admission. Inert until hms enters SPI_READY_TYPES.
        //  - SUPPORTS_VIEW: legacy resolves isView() from the remote table's view text; the plugin path then
        //    consults viewExists, routes a view DROP to dropView, and merges listViewNames into SHOW TABLES —
        //    hive returns an EMPTY listViewNames (its listTableNames already includes views), so the merge is a
        //    no-op and each view is listed once (legacy parity).
        //  - SUPPORTS_COLUMN_AUTO_ANALYZE: legacy StatisticsUtil.supportAutoAnalyze admitted HMS tables of
        //    dlaType HIVE and ICEBERG (NOT hudi) into the background per-column FULL auto-analyze (sample is
        //    unimplemented for external SQL-driven tables); this capability replaces that instanceof-
        //    HMSExternalTable arm. It is connector-wide, so at the flip it also admits hudi-on-HMS tables that
        //    legacy excluded — a residual for the iceberg/hudi delegation substep to gate per-handle or
        //    explicitly accept (there is no per-table escape for it today, unlike Top-N).
        //  - SUPPORTS_METADATA_PRELOAD: legacy HMSExternalTable.supportsExternalMetadataPreload() returned true;
        //    the capability replaces the legacy engine-name "jdbc" gate. Opt-in via enable_preload_external_
        //    metadata (default off), a pure lock-latency optimization with no correctness effect.
        //  - SUPPORTS_MVCC_SNAPSHOT: the heterogeneous hms catalog needs it (its iceberg/hudi-on-HMS tables are
        //    MvccTable, and the GSON single-row maps "HMSExternalTable" -> PluginDrivenMvccExternalTable, so
        //    buildTableInternal selects the Mvcc subclass from this catalog-level capability). Declared HERE
        //    together with its MTMV freshness machinery: HiveConnectorMetadata.getTableFreshness /
        //    getPartitionFreshnessMillis surface hive's last-modified freshness (transient_lastDdlTime), which
        //    PluginDrivenMvccExternalTable wraps into MTMVMaxTimestampSnapshot / MTMVTimestampSnapshot on the
        //    MTMV refresh path (byte-parity with legacy HiveDlaTable) — so a plain-hive base table's MV detects
        //    change instead of pinning a constant. Plain-hive stays non-MVCC per table (beginQuerySnapshot
        //    default-empty -> empty pin -> scan reads current); iceberg/hudi-on-HMS keep their snapshot-id
        //    freshness through the sibling delegation substep. (Fixes the earlier "hive is non-MVCC" note: hive
        //    is non-MVCC per table, but the catalog-level flag is required for the mixed catalog.)
        //
        // Deliberately NOT declared here:
        //  - SUPPORTS_SHOW_CREATE_DDL: the connector must first emit the table location (show.location) and a
        //    generic-vs-hive-specific SHOW CREATE rendering must be decided — its own substep.
        //  - SUPPORTS_PASSTHROUGH_QUERY / SUPPORTS_PARTITION_STATS: hive exposes no query() TVF, and legacy SHOW
        //    PARTITIONS lists names only.
        //  - SUPPORTS_TOPN_LAZY_MATERIALIZE: a per-table marker emitted in getTableSchema (orc/parquet only),
        //    never a connector-wide flag.
        //  - SUPPORTS_NESTED_COLUMN_PRUNE: NOT emitted yet — a genuine deferral like MVCC. Legacy parquet/orc
        //    pruned nested columns, but the connector must first carry stable nested field-ids down its column
        //    tree (SlotTypeReplacer rewrites nested access to field-ids; without them BE reads NULL leaves).
        //    Restore it as a per-table marker once hive field-ids are verified.
        return EnumSet.of(
                ConnectorCapability.SUPPORTS_VIEW,
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE,
                ConnectorCapability.SUPPORTS_METADATA_PRELOAD,
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT);
    }

    private HmsClient getOrCreateClient() {
        if (hmsClient == null) {
            synchronized (this) {
                if (hmsClient == null) {
                    hmsClient = createClient();
                }
            }
        }
        return hmsClient;
    }

    /**
     * Lazily builds and memoizes the embedded iceberg <em>sibling</em> connector this hive gateway delegates its
     * iceberg-on-HMS tables to. There is exactly ONE sibling per gateway connector (not per table): the iceberg
     * connector holds per-catalog caches (latest-snapshot, manifest, scan&rarr;write delete stash) shared across
     * its tables, which a per-op sibling would fragment. The sibling is built through
     * {@link ConnectorContext#createSiblingConnector} so its concrete class is loaded by the iceberg plugin's own
     * child-first classloader, sharing this gateway catalog's id/auth/storage; it is therefore held ONLY as the
     * parent-first {@link Connector} interface and MUST NOT be cast (a cast would CCE across the loader split).
     *
     * <p>Fails loud when no iceberg provider is available (e.g. the plugin is not installed). The failure is NOT
     * memoized (a null sibling leaves the field unset), so a later-available plugin recovers on the next access.
     */
    Connector getOrCreateIcebergSibling() {
        if (icebergSibling == null) {
            synchronized (this) {
                if (icebergSibling == null) {
                    Connector sibling = context.createSiblingConnector(
                            ICEBERG_CONNECTOR_TYPE, IcebergSiblingProperties.synthesize(properties));
                    if (sibling == null) {
                        throw new DorisConnectorException(
                                "Cannot serve iceberg-on-HMS tables in catalog '" + context.getCatalogName()
                                        + "': the iceberg connector plugin is not available");
                    }
                    icebergSibling = sibling;
                }
            }
        }
        return icebergSibling;
    }

    private HmsClient createClient() {
        String metastoreUri = properties.get(HiveConnectorProperties.HIVE_METASTORE_URIS);
        if (metastoreUri == null || metastoreUri.isEmpty()) {
            // Also check the "uri" short form
            metastoreUri = properties.get("uri");
        }
        if (metastoreUri == null || metastoreUri.isEmpty()) {
            throw new DorisConnectorException(
                    "HMS URI ('" + HiveConnectorProperties.HIVE_METASTORE_URIS + "') is required");
        }

        int poolSize = HiveConnectorProperties.getInt(
                properties, HiveConnectorProperties.HMS_CLIENT_POOL_SIZE,
                HiveConnectorProperties.DEFAULT_HMS_CLIENT_POOL_SIZE);

        HmsClientConfig config = new HmsClientConfig(properties, poolSize);
        LOG.info("Creating Hive connector client for catalog='{}', uri={}, type={}, poolSize={}",
                context.getCatalogName(), config.getMetastoreUri(),
                config.getMetastoreType(), poolSize);

        // For a Kerberos catalog run the metastore RPC under the PLUGIN's UGI doAs (buildPluginAuthenticator),
        // NOT the FE-injected context: after the catalog flip that context resolves to NOOP (SIMPLE) auth, which
        // would silently downgrade a Kerberos HMS. AuthAction.execute is a generic method (<T> T execute(...)),
        // so it cannot be a lambda — use an anonymous class. ThriftHmsClient.doAs already pins the RPC's TCCL to
        // the system classloader; the plugin's HadoopAuthenticator only wraps it in a UGI doAs (no TCCL change).
        HadoopAuthenticator auth = pluginAuthenticator();
        ThriftHmsClient.AuthAction authAction;
        if (auth != null) {
            authAction = new ThriftHmsClient.AuthAction() {
                @Override
                public <T> T execute(Callable<T> callable) throws Exception {
                    return auth.doAs(callable::call);
                }
            };
        } else {
            authAction = context::executeAuthenticated;
        }
        return new ThriftHmsClient(config, authAction);
    }

    /**
     * Lazily builds and memoizes the plugin-side Kerberos authenticator that {@link #createClient()} wraps the
     * metastore RPC under, so the RPC uses the PLUGIN's own {@code UserGroupInformation} copy (hadoop +
     * fe-kerberos are bundled child-first in the hive plugin). Returns {@code null} for a non-Kerberos catalog
     * so the FE-injected auth path is preserved unchanged. Construction is cheap — the keytab login is lazy in
     * {@code getUGI()} on the first {@code doAs}.
     */
    private HadoopAuthenticator pluginAuthenticator() {
        if (!pluginAuthComputed) {
            synchronized (this) {
                if (!pluginAuthComputed) {
                    pluginAuth = buildPluginAuthenticator(properties);
                    pluginAuthComputed = true;
                }
            }
        }
        return pluginAuth;
    }

    /**
     * Resolves the plugin-side Kerberos authenticator for the catalog, or {@code null} for a non-Kerberos
     * catalog. Two Kerberos sources are covered, in precedence order (mirroring the legacy
     * {@code HMSBaseProperties.initHadoopAuthenticator}):
     * <ol>
     *   <li><b>Storage</b> Kerberos — the raw {@code hadoop.security.authentication=kerberos} passthrough
     *       (HDFS login), built from the catalog Hadoop configuration. When storage is Kerberos this single
     *       login also carries the HMS metastore RPC (same UGI).</li>
     *   <li><b>HMS-metastore</b> Kerberos with non-Kerberos storage — a secured Hive Metastore whose data
     *       storage is simple (e.g. a Kerberized HMS over S3). The HMS client principal/keytab facts
     *       ({@link HmsMetaStoreProperties#kerberos()}, resolved through the shared metastore-spi parser) feed a
     *       {@link KerberosAuthenticationConfig}, so the {@code doAs} logs in the same client identity fe-core
     *       used. The HMS <em>service</em> principal / SASL settings ride the catalog's own HiveConf, not the
     *       login.</li>
     * </ol>
     * Package-visible + static for KDC-free unit testing.
     */
    static HadoopAuthenticator buildPluginAuthenticator(Map<String, String> properties) {
        if ("kerberos".equalsIgnoreCase(properties.get(HADOOP_SECURITY_AUTHENTICATION))) {
            return HadoopAuthenticator.getHadoopAuthenticator(buildHadoopConf(properties));
        }
        HmsMetaStoreProperties hms = (HmsMetaStoreProperties) MetaStoreProviders.bindForType(
                HmsClientConfig.METASTORE_TYPE_HMS, properties, Collections.emptyMap());
        Optional<KerberosAuthSpec> spec = hms.kerberos();
        if (spec.isPresent() && spec.get().hasCredentials()) {
            Configuration conf = buildHadoopConf(properties);
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("hive.metastore.sasl.enabled", "true");
            return HadoopAuthenticator.getHadoopAuthenticator(
                    new KerberosAuthenticationConfig(spec.get().getPrincipal(), spec.get().getKeytab(), conf));
        }
        return null;
    }

    /**
     * Builds a plain Hadoop {@link Configuration} from the catalog properties for the authenticator. A plain
     * {@code new Configuration()} (NOT {@code HiveConf}) is used deliberately: HiveConf static-init would drag
     * hadoop-mapreduce onto the unit-test classpath. The classloader is pinned to the plugin loader so the
     * child-first (plugin) copy of the auth classes is resolved.
     */
    private static Configuration buildHadoopConf(Map<String, String> properties) {
        Configuration conf = new Configuration();
        conf.setClassLoader(HiveConnector.class.getClassLoader());
        properties.forEach(conf::set);
        return conf;
    }

    @Override
    public void close() throws IOException {
        HmsClient c = hmsClient;
        if (c != null) {
            c.close();
            hmsClient = null;
        }
        // Forward close to the embedded iceberg sibling: the engine closes only a catalog's PRIMARY connector,
        // so the gateway owns the sibling's lifecycle. No-op when the sibling was never built (dormant path).
        Connector sibling = icebergSibling;
        if (sibling != null) {
            sibling.close();
            icebergSibling = null;
        }
    }
}
