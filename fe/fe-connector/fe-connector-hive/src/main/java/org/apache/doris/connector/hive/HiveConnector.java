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
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.connector.hms.CachingHmsClient;
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

    // The sibling connector type a flipped hms gateway delegates hudi-on-HMS tables to. A string literal (hudi
    // has NO user-facing catalog type — it is served only via createSiblingConnector); matches the "hudi" type
    // string HudiConnectorProvider registers. NEVER add "hudi" to CatalogFactory.SPI_READY_TYPES.
    private static final String HUDI_CONNECTOR_TYPE = "hudi";

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

    // Connector-owned directory-listing cache, shared by the (per-call) scan provider and metadata. One per
    // connector (like readTxnManager above) — the scan provider / metadata are rebuilt per query, so the cache
    // must live on the long-lived connector to survive across scans. Built from catalog props; dormant until hms
    // enters SPI_READY_TYPES. Its metastore-metadata sibling is the CachingHmsClient wrapping the HmsClient.
    private final HiveFileListingCache fileListingCache;

    // Embedded iceberg SIBLING connector: a flipped hms gateway delegates its iceberg-on-HMS tables to it. Built
    // once per gateway connector (lazily) in the iceberg plugin's OWN child-first classloader via
    // context.createSiblingConnector — never co-packaged into the hive zip (a second AWS SDK would poison S3
    // JVM-wide). Held ONLY as the parent-first Connector interface and NEVER cast: its concrete type is invisible
    // to the hive loader, so a cast would CCE across the loader split. Dormant until hms enters SPI_READY_TYPES —
    // nothing builds it today.
    private volatile Connector icebergSibling;

    // Embedded hudi SIBLING connector: a flipped hms gateway delegates its hudi-on-HMS tables to it. Same
    // lifecycle/classloader contract as icebergSibling above — built once per gateway (lazily) in the hudi
    // plugin's OWN child-first classloader via context.createSiblingConnector, never co-packaged into the hive
    // zip (a second AWS SDK would poison S3 JVM-wide). Held ONLY as the parent-first Connector interface and
    // NEVER cast (a cast would CCE across the loader split). Dormant until hms enters SPI_READY_TYPES AND the
    // getTableHandle HUDI divert is wired (a later substep) — nothing references it today.
    private volatile Connector hudiSibling;

    public HiveConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
        this.fileListingCache = new HiveFileListingCache(this.properties);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return newMetadata(getOrCreateClient());
    }

    /**
     * Builds the connector's metadata with its sibling seams wired in. Extracted (package-private) from
     * {@link #getMetadata} so a unit test can assert the wiring WITHOUT {@link #getOrCreateClient()} building a
     * real ThriftHmsClient (whose Hadoop stack is absent from connector unit tests). The seams:
     * <ul>
     *   <li>getOrCreateIcebergSibling / getOrCreateHudiSibling (by-TYPE, force-build): only the getTableHandle
     *       ICEBERG / HUDI diverts use them, to build+ask the matching sibling for a table detected as iceberg/hudi
     *       (no handle exists yet to route by). These two args share the static type {@code Supplier<Connector>},
     *       so a transposition would compile clean &mdash; HiveConnectorThreeWayRoutingTest pins the pairing.</li>
     *   <li>resolveSiblingOwner (by-HANDLE, peek): every per-handle site routes a foreign handle to whichever
     *       ALREADY-BUILT sibling owns it. Passing the resolver (not a built sibling) keeps a pure-hive query from
     *       ever building/throwing a sibling.</li>
     * </ul>
     */
    HiveConnectorMetadata newMetadata(HmsClient client) {
        return new HiveConnectorMetadata(client, properties, context,
                this::getOrCreateIcebergSibling, this::getOrCreateHudiSibling, this::resolveSiblingOwner,
                fileListingCache);
    }

    /**
     * Resolves which embedded sibling connector OWNS a foreign (non-hive) table handle, for the per-handle
     * gateway seams (the connector-level {@code get*Provider(handle)} below and the ~34 guard-and-forward
     * methods in {@link HiveConnectorMetadata}). Asks each sibling's {@link Connector#ownsHandle} — the sibling
     * tests its OWN in-loader handle type, which is invisible to the hive loader across the plugin split, so the
     * gateway can never {@code instanceof} the foreign handle itself.
     *
     * <p>Consults only ALREADY-BUILT siblings (a plain field read, never {@code getOrCreate*}). The owning
     * sibling is always already built: a foreign handle can only originate from {@code getTableHandle}'s divert,
     * which force-builds that sibling before producing the handle. Reading the field (not force-building) avoids
     * demanding an UNRELATED plugin merely to classify a handle — e.g. a hudi-only hms catalog with no iceberg
     * plugin must still route its hudi handles without building an iceberg sibling. Fails loud when no built
     * sibling owns the handle (an orphan handle is a bug, not a route), naming the catalog.
     */
    private Connector resolveSiblingOwner(ConnectorTableHandle handle) {
        Connector iceberg = icebergSibling;
        if (iceberg != null && iceberg.ownsHandle(handle)) {
            return iceberg;
        }
        Connector hudi = hudiSibling;
        if (hudi != null && hudi.ownsHandle(handle)) {
            return hudi;
        }
        throw new DorisConnectorException("Cannot route a foreign table handle in catalog '"
                + context.getCatalogName() + "': no embedded sibling connector owns it");
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new HiveScanPlanProvider(getOrCreateClient(), properties, readTxnManager, fileListingCache);
    }

    /**
     * Selects the scan provider for a given table handle — the gateway seam a flipped hms catalog uses to serve
     * its iceberg-on-HMS tables from the embedded iceberg sibling. A hive handle (the gateway's OWN hive-loader
     * type) runs the hive scan provider; any foreign handle (the raw iceberg handle the sibling's getTableHandle
     * produced) is delegated to the sibling's per-handle scan provider. Because the returned sibling provider is
     * built in the iceberg plugin's classloader, {@code PluginDrivenScanNode.onPluginClassLoader} auto-pins the
     * scan-thread TCCL to the iceberg loader for free (it keys off {@code provider.getClass().getClassLoader()}),
     * so no pinning is needed here. The foreign handle is passed through UNMODIFIED and NEVER cast (its concrete
     * sibling type is invisible across the loader split — a cast would CCE). A HUDI table (once its divert lands)
     * routes to the hudi sibling by the 3-way {@link #resolveSiblingOwner} — a HUDI-stamped HiveTableHandle stays
     * hive. Pairs with the getTableHandle diverts; dormant until hms enters SPI_READY_TYPES (nothing selects a
     * scan provider for this connector today).
     */
    @Override
    public ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
        if (handle instanceof HiveTableHandle) {
            return getScanPlanProvider();
        }
        return resolveSiblingOwner(handle).getScanPlanProvider(handle);
    }

    @Override
    public ConnectorWritePlanProvider getWritePlanProvider() {
        return new HiveWritePlanProvider(getOrCreateClient(), properties, context);
    }

    /**
     * Per-table write provider: a hive handle uses the hive write provider; a foreign handle is delegated to the
     * OWNING sibling's per-handle write provider (resolved 3-way by {@link #resolveSiblingOwner}). The foreign
     * handle is passed through UNMODIFIED and NEVER cast (its concrete sibling type is invisible across the loader
     * split — a cast would CCE). A HUDI-stamped HiveTableHandle stays on the hive write path. Mirrors {@link
     * #getScanPlanProvider(ConnectorTableHandle)}; dormant until hms enters SPI_READY_TYPES. The returned sibling
     * provider runs its planWrite on fe-core threads — the write-path TCCL pin is a separate flip-time concern.
     */
    @Override
    public ConnectorWritePlanProvider getWritePlanProvider(ConnectorTableHandle handle) {
        if (handle instanceof HiveTableHandle) {
            return getWritePlanProvider();
        }
        return resolveSiblingOwner(handle).getWritePlanProvider(handle);
    }

    /**
     * Per-table procedure ops for {@code ALTER TABLE ... EXECUTE}: a hive handle has NO procedures — it inherits
     * the connector-level {@code null} (plain-hive exposes none) — while a foreign handle is delegated to the
     * OWNING sibling's per-handle procedure ops (resolved 3-way by {@link #resolveSiblingOwner}), so an
     * iceberg-on-HMS table gains the native iceberg procedures (rollback_to_snapshot, rewrite_data_files, ...).
     * The foreign handle is passed through UNMODIFIED and NEVER cast (its concrete sibling type is invisible
     * across the loader split — a cast would CCE). A HUDI-stamped HiveTableHandle stays hive and inherits the
     * null (no procedures), same as plain-hive. Mirrors {@link #getWritePlanProvider(ConnectorTableHandle)};
     * dormant until hms enters SPI_READY_TYPES (nothing selects procedure ops for this connector today).
     */
    @Override
    public ConnectorProcedureOps getProcedureOps(ConnectorTableHandle handle) {
        if (handle instanceof HiveTableHandle) {
            return getProcedureOps();
        }
        return resolveSiblingOwner(handle).getProcedureOps(handle);
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

    /**
     * Lazily builds and memoizes the embedded hudi <em>sibling</em> connector this hive gateway delegates its
     * hudi-on-HMS tables to. Mirrors {@link #getOrCreateIcebergSibling()}: exactly ONE sibling per gateway
     * connector (not per table), built through {@link ConnectorContext#createSiblingConnector} so its concrete
     * class is loaded by the hudi plugin's own child-first classloader, sharing this gateway catalog's
     * id/auth/storage; it is therefore held ONLY as the parent-first {@link Connector} interface and MUST NOT be
     * cast (a cast would CCE across the loader split).
     *
     * <p>Fails loud when no hudi provider is available (e.g. the plugin is not installed). The failure is NOT
     * memoized (a null sibling leaves the field unset), so a later-available plugin recovers on the next access.
     *
     * <p>Dormant: no production path references it until the getTableHandle HUDI divert lands (a later substep).
     */
    Connector getOrCreateHudiSibling() {
        if (hudiSibling == null) {
            synchronized (this) {
                if (hudiSibling == null) {
                    Connector sibling = context.createSiblingConnector(
                            HUDI_CONNECTOR_TYPE, HudiSiblingProperties.synthesize(properties));
                    if (sibling == null) {
                        throw new DorisConnectorException(
                                "Cannot serve hudi-on-HMS tables in catalog '" + context.getCatalogName()
                                        + "': the hudi connector plugin is not available");
                    }
                    hudiSibling = sibling;
                }
            }
        }
        return hudiSibling;
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
        return wrapWithCache(new ThriftHmsClient(config, authAction));
    }

    /**
     * Wraps the raw pooled metastore client in the connector-owned {@link CachingHmsClient} so this connector
     * keeps caching its scan-side metastore reads AFTER the hms cutover. Once a hive catalog becomes plugin-driven
     * the fe-core engine-side {@code HiveExternalMetaCache} stops routing to it, so without this wrap every
     * {@code getTable} / {@code listPartitionNames} / {@code getPartitions} / column-stats read would regress to a
     * fresh Thrift RPC on every scan. This single wrap makes ALL {@code hmsClient.*} reads in
     * {@link HiveConnectorMetadata} cache-backed transparently — including the
     * {@link HiveConnectorMetadata#getTableFreshness}/{@link HiveConnectorMetadata#getPartitionFreshnessMillis}
     * MTMV probes (both read {@code hmsClient.getPartitions}), so the periodic SQL-dictionary / MV freshness poll
     * stays cheap instead of hitting the metastore every tick.
     *
     * <p>The decorator reads its per-entry knobs from this catalog's own {@code meta.cache.hive.*} properties; a
     * disabled entry (or {@code ttl-second}/{@code capacity} &lt;= 0) makes it a transparent pass-through. The
     * cache lives on the {@code HmsClient} (rebuilt on connector init / {@code ADD}/{@code MODIFY CATALOG}), never
     * on a handle or the GSON edit log.
     *
     * <p>Extracted (package-private) so a unit test can verify the wrap + caching WITHOUT {@link #createClient()}
     * building a real {@code ThriftHmsClient} (whose Hadoop stack is absent from connector unit tests) — mirrors
     * {@link #newMetadata(HmsClient)}. Dormant: {@code "hms"} is not in {@code SPI_READY_TYPES}, so no live catalog
     * builds a {@code HiveConnector} — this wrap only runs in unit tests until the flip.
     */
    HmsClient wrapWithCache(HmsClient raw) {
        return new CachingHmsClient(raw, properties);
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
        // Same for the embedded hudi sibling — the gateway owns its lifecycle too. No-op when never built.
        Connector hudi = hudiSibling;
        if (hudi != null) {
            hudi.close();
            hudiSibling = null;
        }
    }
}
