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

import org.apache.doris.connector.cache.ConnectorMetadataCache;
import org.apache.doris.connector.hms.CachingHmsClient;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientConfig;
import org.apache.doris.connector.hms.ThriftHmsClient;
import org.apache.doris.connector.hms.event.HmsEventSource;
import org.apache.doris.connector.metastore.HmsMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTestResult;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.event.ConnectorEventSource;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.KerberosAuthSpec;
import org.apache.doris.kerberos.KerberosAuthenticationConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

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
    // matches the type name IcebergConnectorProvider registers.
    private static final String ICEBERG_CONNECTOR_TYPE = "iceberg";

    // The sibling connector type a flipped hms gateway delegates hudi-on-HMS tables to. A string literal (hudi
    // has NO user-facing catalog type — it is served only via createSiblingConnector); matches the "hudi" type
    // string HudiConnectorProvider registers, which declares isStandaloneCatalogType() == false so that the
    // engine can never build a hudi catalog. Sibling lookup does not consult that flag, so this stays valid.
    private static final String HUDI_CONNECTOR_TYPE = "hudi";

    private final Map<String, String> properties;
    // Bound + validated once here: an existing HiveConnector always has usable catalog properties. Built on
    // every (re)build of the connector, including the lazy one an FE does after replaying the edit log.
    private final HiveCatalogProperties props;
    private final ConnectorContext context;
    private volatile HmsClient hmsClient;

    // Lazily-built plugin-side Kerberos authenticator (single-owner auth), null for a non-Kerberos catalog.
    // Its doAs acts on the PLUGIN's UserGroupInformation copy — the one the plugin's ThriftHmsClient RPC reads
    // (hadoop + fe-kerberos bundled child-first) — not the app-loader copy the FE-injected context would use.
    private volatile HadoopAuthenticator pluginAuth;
    private volatile boolean pluginAuthComputed;

    // Read-transaction manager for transactional (ACID) Hive scans. One per connector, keyed by query.
    // Plugin-owned; live since the read cutover wired its query-finish commit (see the manager).
    private final HiveReadTransactionManager readTxnManager = new HiveReadTransactionManager();

    // Connector-owned directory-listing cache, shared by the (per-call) scan provider and metadata. One per
    // connector (like readTxnManager above) — the scan provider / metadata are rebuilt per query, so the cache
    // must live on the long-lived connector to survive across scans. Built from catalog props. Its
    // metastore-metadata sibling is the CachingHmsClient wrapping the HmsClient.
    private final HiveFileListingCache fileListingCache;

    // PERF-06 (S6): cross-query DERIVED partition-view cache ("cache A", the generic ConnectorMetadataCache
    // from fe-connector-cache), layered ABOVE the raw per-name HMS listing served by CachingHmsClient: it
    // memoizes the BUILT List<ConnectorPartitionInfo> (HiveConnectorMetadata#listPartitionsUncached's per-name
    // HiveWriteUtils.toPartitionValues parse + ConnectorPartitionInfo construction), keyed by
    // (db, table, -1, -1) — hive is snapshot-less (beginQuerySnapshot always pins -1) and its handle carries no
    // schema version, so every query for a table shares ONE entry, invalidated by the hooks below + TTL. ONE
    // typed field (like paimon): hive's getMvccPartitionView returns the SPI default (Optional.empty()) for a
    // real hive handle, so listPartitions is the only enumeration hook to wrap. Unlike iceberg, hive has NO
    // session=user / per-user credential-isolation cache-disabling convention (its per-catalog caches —
    // CachingHmsClient, HiveFileListingCache — are already built unconditionally below), so this is constructed
    // unconditionally too.
    private final ConnectorMetadataCache<List<ConnectorPartitionInfo>> partitionViewCache;

    // Embedded iceberg SIBLING connector: a flipped hms gateway delegates its iceberg-on-HMS tables to it. Built
    // once per gateway connector (lazily) in the iceberg plugin's OWN child-first classloader via
    // context.createSiblingConnector — never co-packaged into the hive zip (a second AWS SDK would poison S3
    // JVM-wide). Held ONLY as the parent-first Connector interface and NEVER cast: its concrete type is invisible
    // to the hive loader, so a cast would CCE across the loader split.
    private volatile Connector icebergSibling;

    // Embedded hudi SIBLING connector: a flipped hms gateway delegates its hudi-on-HMS tables to it. Same
    // lifecycle/classloader contract as icebergSibling above — built once per gateway (lazily) in the hudi
    // plugin's OWN child-first classloader via context.createSiblingConnector, never co-packaged into the hive
    // zip (a second AWS SDK would poison S3 JVM-wide). Held ONLY as the parent-first Connector interface and
    // NEVER cast (a cast would CCE across the loader split).
    private volatile Connector hudiSibling;

    // Set FIRST in close(), under the same monitor the getOrCreate* holders below take, and never cleared.
    // Every one of those holders is a plain double-checked lazy build over a field close() nulls out, so
    // without this flag a call that arrives AFTER close() does not fail and is not a no-op: it rebuilds. A
    // rebuilt hudi sibling takes a hold on HudiConnector's static FS_SCOPES entry that no later close() can
    // reach (this gateway is already detached from the catalog and PluginDrivenExternalCatalog has already
    // nulled its own connector field), so the UGI, every FileSystem Hadoop cached under it, and those
    // filesystems' SDK executor threads live to the end of the FE process. A rebuilt iceberg sibling or
    // HmsClient leaks its own resources the same way. It takes no error to get here: a statement that
    // resolved this catalog before an ALTER CATALOG and touches an iceberg/hudi-on-HMS table after it reaches
    // the holders through the metadata instance PluginDrivenMetadata cached for the statement. Mirrors
    // JdbcDorisConnector, which guards its data source the same way.
    private volatile boolean closed;

    public HiveConnector(Map<String, String> properties, ConnectorContext context) {
        this.props = HiveCatalogProperties.of(properties);
        this.properties = props.getRaw();
        this.context = context;
        this.fileListingCache = new HiveFileListingCache(props);
        // Reads its own meta.cache.hive.partition_view.(enable|ttl-second|capacity) from the catalog properties
        // via the framework's CacheSpec (default ON / 24h / 1000).
        this.partitionViewCache = new ConnectorMetadataCache<>("hive", "partition_view", this.properties);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return newMetadata(getOrCreateClient());
    }

    /**
     * Eagerly validates metastore connectivity during CREATE CATALOG (when {@code test_connection=true}),
     * so a wrong {@code hive.metastore.uris} or a bad Kerberos setup fails the DDL instead of surfacing at
     * the first query. Listing databases forces a real HMS round-trip through the authenticated client,
     * which is what the legacy fe-core {@code HMSBaseConnectivityTester} did with {@code getAllDatabases()}.
     *
     * <p>The message deliberately carries both the {@code "HMS"} tag and the phrase
     * {@code "connectivity test failed"} — the CREATE CATALOG regression asserts on both.</p>
     */
    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            getMetadata(session).listDatabaseNames(session);
            return ConnectorTestResult.success();
        } catch (Exception e) {
            LOG.warn("HMS connectivity test failed for catalog '{}'", context.getCatalogName(), e);
            return ConnectorTestResult.failure("HMS connectivity test failed: " + rootCauseMessage(e));
        }
    }

    /** Unwraps to the deepest cause so the DDL error names the actual failure (e.g. a refused connection). */
    private static String rootCauseMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message == null ? cause.toString() : message;
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
        return new HiveConnectorMetadata(client, props, context,
                this::getOrCreateIcebergSibling, this::getOrCreateHudiSibling, this::resolveSiblingOwnerLabeled,
                fileListingCache, partitionViewCache);
    }

    /**
     * Resolves which embedded sibling connector OWNS a foreign (non-hive) table handle AND its stable owner label,
     * for the per-handle gateway seams (the connector-level {@code get*Provider(handle)} below and the
     * guard-and-forward methods in {@link HiveConnectorMetadata}). Asks each sibling's {@link Connector#ownsHandle}
     * — the sibling tests its OWN in-loader handle type, which is invisible to the hive loader across the plugin
     * split, so the gateway can never {@code instanceof} the foreign handle itself. The MATCHED ARM supplies the
     * label ({@link SiblingOwner#ICEBERG_LABEL} / {@link SiblingOwner#HUDI_LABEL}), so a per-handle forward keys the
     * per-statement metadata funnel by owner without a force-build or an identity comparison against the suppliers.
     *
     * <p>Consults only ALREADY-BUILT siblings (a plain field read, never {@code getOrCreate*}). The owning
     * sibling is always already built: a foreign handle can only originate from {@code getTableHandle}'s divert,
     * which force-builds that sibling before producing the handle. Reading the field (not force-building) avoids
     * demanding an UNRELATED plugin merely to classify a handle — e.g. a hudi-only hms catalog with no iceberg
     * plugin must still route its hudi handles without building an iceberg sibling. Fails loud when no built
     * sibling owns the handle (an orphan handle is a bug, not a route), naming the catalog.
     */
    SiblingOwner resolveSiblingOwnerLabeled(ConnectorTableHandle handle) {
        Connector iceberg = icebergSibling;
        if (iceberg != null && iceberg.ownsHandle(handle)) {
            return new SiblingOwner(iceberg, SiblingOwner.ICEBERG_LABEL);
        }
        Connector hudi = hudiSibling;
        if (hudi != null && hudi.ownsHandle(handle)) {
            return new SiblingOwner(hudi, SiblingOwner.HUDI_LABEL);
        }
        throw new DorisConnectorException("Cannot route a foreign table handle in catalog '"
                + context.getCatalogName() + "': no embedded sibling connector owns it");
    }

    /**
     * The owning sibling {@link Connector} alone (label dropped) for the connector-level per-handle provider seams
     * ({@code get*Provider(handle)} below), which route by owner but never touch the metadata funnel. Delegates to
     * {@link #resolveSiblingOwnerLabeled} so the 3-way ownsHandle dispatch has a single implementation.
     */
    private Connector resolveSiblingOwner(ConnectorTableHandle handle) {
        return resolveSiblingOwnerLabeled(handle).connector();
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new HiveScanPlanProvider(getOrCreateClient(), props, context, readTxnManager, fileListingCache);
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
     * hive. Pairs with the getTableHandle diverts.
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
        return new HiveWritePlanProvider(getOrCreateClient(), props, context);
    }

    /**
     * Per-table write provider: a hive handle uses the hive write provider; a foreign handle is delegated to the
     * OWNING sibling's per-handle write provider (resolved 3-way by {@link #resolveSiblingOwner}). The foreign
     * handle is passed through UNMODIFIED and NEVER cast (its concrete sibling type is invisible across the loader
     * split — a cast would CCE). A HUDI-stamped HiveTableHandle stays on the hive write path. Mirrors {@link
     * #getScanPlanProvider(ConnectorTableHandle)}. The returned sibling
     * provider's planWrite runs on fe-core threads, but the write-path TCCL pin is already carried by the sibling's
     * own {@code TcclPinningConnectorContext} (e.g. {@code IcebergConnector} wraps {@code executeAuthenticated},
     * classloader-thread-independent), so no additional pin is needed here — verified, not an open flip-time gap.
     * The iceberg-on-HMS write path is e2e-owed on a heterogeneous HMS catalog.
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
     * null (no procedures), same as plain-hive. Mirrors {@link #getWritePlanProvider(ConnectorTableHandle)}.
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
        // HMSExternalTable/HMS admission.
        //  - SUPPORTS_VIEW: legacy resolves isView() from the remote table's view text; the plugin path then
        //    consults viewExists, routes a view DROP to dropView, and merges listViewNames into SHOW TABLES —
        //    hive returns an EMPTY listViewNames (its listTableNames already includes views), so the merge is a
        //    no-op and each view is listed once (legacy parity).
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
        //  - SUPPORTS_PARTITION_STATS: legacy SHOW PARTITIONS lists names only. (Passthrough SQL is not a
        //    capability at all: hive simply does not implement ConnectorPassthroughSqlOps.)
        //  - SUPPORTS_TOPN_LAZY_MATERIALIZE: a per-table marker emitted in getTableSchema (orc/parquet only),
        //    never a connector-wide flag.
        //  - SUPPORTS_COLUMN_AUTO_ANALYZE: legacy StatisticsUtil.supportAutoAnalyze admitted HMS tables of dlaType
        //    HIVE and ICEBERG (NOT hudi) into background per-column auto-analyze. A connector-wide flag here would
        //    over-admit hudi-on-HMS (which legacy excluded), so it is NOT declared connector-wide: getTableSchema
        //    emits it per-table for every plain-hive table, and the sibling-delegation branch reflects the iceberg
        //    sibling's connector-wide capability set onto an iceberg-on-HMS table's schema (so it survives the
        //    delegation) — hudi-on-HMS, whose connector declares neither, is thereby correctly withheld.
        //  - SUPPORTS_NESTED_COLUMN_PRUNE: NOT emitted for plain-hive yet — a genuine deferral like MVCC. Legacy
        //    parquet/orc pruned nested columns, but the connector must first carry stable nested field-ids down its
        //    column tree (SlotTypeReplacer rewrites nested access to field-ids; without them BE reads NULL leaves).
        //    Restore it as a per-table marker once hive field-ids are verified. (An iceberg-on-HMS table DOES get
        //    it, via the sibling-capability reflection above, because the iceberg sibling carries field-ids.)
        return EnumSet.of(
                ConnectorCapability.SUPPORTS_VIEW,
                ConnectorCapability.SUPPORTS_METADATA_PRELOAD,
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT);
    }

    /**
     * REFRESH TABLE hook: drop this table's connector-owned scan caches. Clears BOTH cache layers for the table —
     * the metastore-metadata entries ({@link CachingHmsClient#flush}: table meta, partition names, partition
     * objects, column stats) AND the {@link HiveFileListingCache} directory listings — because a hive table's
     * schema, partitions AND files are all mutable, so a REFRESH must re-read all of them (unlike iceberg, whose
     * manifests are immutable and kept across REFRESH TABLE). fe-core already routes {@code REFRESH TABLE} to
     * {@code connector.invalidateTable} for a plugin-driven catalog.
     * Also forwarded to the already-built embedded siblings — see {@link #forEachBuiltSibling}.
     */
    @Override
    public void invalidateTable(String dbName, String tableName) {
        // Read the client field WITHOUT building it (getOrCreateClient would force a real ThriftHmsClient just to
        // flush an empty cache): a never-built client means no metastore cache exists to flush. The file cache is
        // a final field and always present.
        invalidateTable(hmsClient, dbName, tableName);
        forEachBuiltSibling(sibling -> sibling.invalidateTable(dbName, tableName));
    }

    // Package-private seam: the metastore half needs an observable CachingHmsClient, which a unit test can build
    // via wrapWithCache (the hmsClient field is otherwise only set by getOrCreateClient building a real client).
    void invalidateTable(HmsClient client, String dbName, String tableName) {
        if (client instanceof CachingHmsClient) {
            ((CachingHmsClient) client).flush(dbName, tableName);
        }
        fileListingCache.invalidateTable(dbName, tableName);
        // PERF-06: also drop this table's cached derived partition-view entry, so the next listPartitions
        // re-derives live.
        partitionViewCache.invalidateTable(dbName, tableName);
    }

    /**
     * REFRESH DATABASE hook: drop the connector-owned scan caches for EVERY table in this database — both cache
     * layers ({@link CachingHmsClient#flushDb} metastore-metadata + {@link HiveFileListingCache#invalidateDb}
     * directory listings). Same no-force-build read of the client as {@link #invalidateTable(String, String)}.
     * fe-core routes {@code REFRESH DATABASE} to {@code connector.invalidateDb} for a plugin-driven catalog.
     * Also forwarded to the already-built embedded siblings — see {@link #forEachBuiltSibling}.
     */
    @Override
    public void invalidateDb(String dbName) {
        invalidateDb(hmsClient, dbName);
        forEachBuiltSibling(sibling -> sibling.invalidateDb(dbName));
    }

    // Package-private seam (see invalidateTable above).
    void invalidateDb(HmsClient client, String dbName) {
        if (client instanceof CachingHmsClient) {
            ((CachingHmsClient) client).flushDb(dbName);
        }
        fileListingCache.invalidateDb(dbName);
        partitionViewCache.invalidateDb(dbName);
    }

    /**
     * REFRESH CATALOG hook: drop ALL of this catalog's connector-owned scan caches — every metastore-metadata
     * entry ({@link CachingHmsClient#flushAll}) and every directory listing. Same no-force-build read of the
     * client as {@link #invalidateTable(String, String)}.
     * Also forwarded to the already-built embedded siblings — see {@link #forEachBuiltSibling}.
     */
    @Override
    public void invalidateAll() {
        invalidateAll(hmsClient);
        forEachBuiltSibling(Connector::invalidateAll);
    }

    // Package-private seam (see invalidateTable above).
    void invalidateAll(HmsClient client) {
        if (client instanceof CachingHmsClient) {
            ((CachingHmsClient) client).flushAll();
        }
        fileListingCache.invalidateAll();
        partitionViewCache.invalidateAll();
    }

    /**
     * Invalidates exactly the named partitions on a partition add/drop/alter, mirroring legacy
     * {@code HiveExternalMetaCache}'s per-partition invalidation. The partition NAMES are parsed into VALUES
     * purely ({@link HiveWriteUtils#toPartitionValues}, no metastore round-trip), which key both connector
     * caches: the directory-listing cache ({@link HiveFileListingCache#invalidatePartitions}) and the metastore
     * partition-metadata cache ({@link CachingHmsClient#invalidatePartitions}). Deriving values from the name
     * (rather than looking the partition up) is exactly what stops an evicted partition-metadata entry from
     * leaving a stale file listing — the #65334 failure mode. Table-level column stats and the table object are
     * intentionally left intact (legacy did not drop them on a partition-level refresh). Also forwarded to the
     * already-built embedded siblings.
     */
    @Override
    public void invalidatePartition(String dbName, String tableName, List<String> partitionNames) {
        invalidatePartition(hmsClient, dbName, tableName, partitionNames);
        forEachBuiltSibling(sibling -> sibling.invalidatePartition(dbName, tableName, partitionNames));
    }

    // Package-private seam (mirrors invalidateTable): the metastore half needs an observable CachingHmsClient.
    void invalidatePartition(HmsClient client, String dbName, String tableName, List<String> partitionNames) {
        Set<List<String>> partitionValues = new HashSet<>();
        for (String name : partitionNames) {
            partitionValues.add(HiveWriteUtils.toPartitionValues(name));
        }
        if (client instanceof CachingHmsClient) {
            ((CachingHmsClient) client).invalidatePartitions(dbName, tableName, partitionValues);
        }
        fileListingCache.invalidatePartitions(dbName, tableName, partitionValues);
        // PERF-06: cache A's key carries no partition-name axis (only db/table/-1/-1), so a partition-level
        // change cannot be scoped finer than the whole table's single cached entry — invalidate it wholesale.
        partitionViewCache.invalidateTable(dbName, tableName);
    }

    /**
     * The catalog's incremental metadata-change source, or {@code null} when this catalog does not opt into
     * HMS notification-event sync. The per-catalog opt-in is the connector's concern (fe-core does not parse
     * hive properties): a source is returned only when the
     * {@code hive.enable_hms_events_incremental_sync} property is set, and the engine's role-aware event
     * driver skips connectors whose source is null. A value that is not {@code true} reads as disabled,
     * mirroring the legacy poller's skip-on-throw; a malformed <em>batch size</em>, in contrast, now fails
     * when the catalog properties are bound rather than being silently replaced by the default.
     */
    @Override
    public ConnectorEventSource getEventSource() {
        if (!props.isHmsEventsIncrementalSyncEnabled()) {
            return null;
        }
        return new HmsEventSource(getOrCreateClient(), props.getHmsEventsBatchSizePerRpc());
    }

    /**
     * Runs one invalidation action on each ALREADY-BUILT embedded sibling (iceberg, hudi). fe-core routes
     * {@code REFRESH TABLE/DATABASE/CATALOG} only to a catalog's PRIMARY connector, so the gateway must
     * propagate invalidation to the sibling-owned caches — e.g. the iceberg sibling's latest-snapshot pin,
     * whose ACCESS-based expiry keeps a continuously-queried stale entry alive indefinitely, making an explicit
     * REFRESH the only way to unpin an iceberg-on-HMS table's staleness. Mirrors {@link #close()}: plain
     * volatile reads, never force-builds (a never-built sibling has no cache to drop, and building one just to
     * flush it could fail-loud spuriously on a catalog whose sibling plugin is absent).
     */
    private void forEachBuiltSibling(Consumer<Connector> action) {
        Connector iceberg = icebergSibling;
        if (iceberg != null) {
            action.accept(iceberg);
        }
        Connector hudi = hudiSibling;
        if (hudi != null) {
            action.accept(hudi);
        }
    }

    /** The connector-owned directory-listing cache, exposed for unit tests (mirrors iceberg manifestCacheForTest). */
    HiveFileListingCache fileListingCacheForTest() {
        return fileListingCache;
    }

    /** Test-only: the derived listPartitions view cache (PERF-06). Never null (hive has no session=user gate). */
    ConnectorMetadataCache<List<ConnectorPartitionInfo>> partitionViewCacheForTest() {
        return partitionViewCache;
    }

    private HmsClient getOrCreateClient() {
        if (hmsClient == null) {
            synchronized (this) {
                // Re-checked INSIDE the monitor, which is what makes it exclusive with close(): either this
                // build completes and close() (which takes the same monitor before it detaches anything)
                // finds the client and closes it, or close() wins and this throws. See the `closed` field.
                throwIfClosed();
                if (hmsClient == null) {
                    hmsClient = createClient();
                }
            }
        }
        return hmsClient;
    }

    /**
     * Fails a lazy build that arrived after {@link #close()}. Called under {@code this} by every holder, so
     * the check cannot pass while close() is detaching. The message is deliberately actionable: the only way
     * to reach it is a statement that outlived the catalog definition it resolved against, and re-running it
     * resolves the replacement connector.
     */
    private void throwIfClosed() {
        if (closed) {
            throw new DorisConnectorException("catalog '" + context.getCatalogName()
                    + "' was closed (dropped or altered) while this statement was running; retry the statement");
        }
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
                throwIfClosed();
                if (icebergSibling == null) {
                    Connector sibling = context.createSiblingConnector(
                            ICEBERG_CONNECTOR_TYPE, IcebergSiblingProperties.synthesize(properties));
                    if (sibling == null) {
                        throw new DorisConnectorException(
                                "Cannot serve iceberg-on-HMS tables in catalog '" + context.getCatalogName()
                                        + "': the iceberg connector plugin is not available");
                    }
                    // Fail-loud invariant guard for the cache-isolation security track: the hive gateway FRONT
                    // DOOR never declares SUPPORTS_USER_SESSION, so fe-core keys its per-user schema/name cache
                    // bypass off THIS (front-door) connector's capabilities and would NOT bypass for a delegated
                    // sibling. The iceberg sibling is forced iceberg.catalog.type=hms (IcebergSiblingProperties
                    // .synthesize) and can never be REST session=user, so this must hold today. If a future change
                    // ever let the sibling be session=user, the front-door-only bypass would silently leak
                    // cross-user metadata — fail here instead.
                    if (sibling.getCapabilities().contains(ConnectorCapability.SUPPORTS_USER_SESSION)) {
                        throw new DorisConnectorException(
                                "iceberg-on-HMS sibling in catalog '" + context.getCatalogName()
                                        + "' unexpectedly declares SUPPORTS_USER_SESSION: the hive gateway front "
                                        + "door is not session=user, so fe-core's per-user schema/name cache bypass "
                                        + "would not trigger and cross-user metadata would leak. The sibling must "
                                        + "stay iceberg.catalog.type=hms (never REST session=user).");
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
     */
    Connector getOrCreateHudiSibling() {
        if (hudiSibling == null) {
            synchronized (this) {
                throwIfClosed();
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
        // No removed-type / required-URI check here: both are invariants of HiveCatalogProperties, which this
        // connector's constructor built. A catalog created before the type was removed, or one carrying no
        // metastore URI, fails when the connector is (re)built — including on the lazy build an FE does for a
        // catalog it loaded from the image — with the same messages this method used to produce.
        int poolSize = props.getHmsClientPoolSize();

        // getHmsClientProperties(), not the raw map: the metastore URI must reach HiveConf under its canonical
        // key even when the catalog spells it with the "uri" short form.
        HmsClientConfig config = new HmsClientConfig(props.getHmsClientProperties(), poolSize);
        LOG.info("Creating Hive connector client for catalog='{}', uri={}, type={}, poolSize={}",
                context.getCatalogName(), config.getMetastoreUri(),
                config.getMetastoreType(), poolSize);

        // For a Kerberos catalog run the metastore RPC under the PLUGIN's UGI doAs (buildPluginAuthenticator),
        // NOT the FE-injected context: after the catalog flip that context resolves to NOOP (SIMPLE) auth, which
        // would silently downgrade a Kerberos HMS. AuthAction.execute is a generic method (<T> T execute(...)),
        // so it cannot be a lambda — use an anonymous class. ThriftHmsClient.doAs pins the RPC's TCCL to the
        // plugin (child-first) classloader (so SecurityUtil.<clinit> resolves hadoop from the plugin copy, not a
        // split-brain against fe-core's copy); the plugin's HadoopAuthenticator only wraps it in a UGI doAs.
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
        // Feed the catalog's type-mapping options (enable.mapping.varbinary / enable.mapping.timestamp_tz) to
        // the LIVE client: ThriftHmsClient.convertFieldSchemas maps hive column types with the client's own
        // options, so the 2-arg ctor (HmsTypeMapping.Options.DEFAULT) would ignore the catalog toggles and
        // always map hive BINARY -> STRING / timestamp -> non-TZ. Commit 5672d7c0209 read the dot-keys but only
        // into a dead metadata field; the fix is to build the options here where the client is constructed.
        return wrapWithCache(new ThriftHmsClient(config, authAction,
                HiveConnectorMetadata.buildTypeMappingOptions(props)));
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
     * {@link #newMetadata(HmsClient)}. Live since the hms flip: every live hms catalog builds a
     * {@code HiveConnector}, and {@link #createClient()} wraps its {@code ThriftHmsClient} here.
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
        // Pin the TCCL to the plugin (child-first) classloader for the whole resolution. A catalog that declares
        // hadoop.security.authentication=kerberos WITHOUT a principal/keytab falls back to a
        // HadoopSimpleAuthenticator whose ctor EAGERLY calls UserGroupInformation.createRemoteUser ->
        // SecurityUtil.<clinit>, whose internal `new Configuration()` captures the current TCCL. This runs on the
        // (unpinned) createClient thread, so without the pin it would resolve hadoop's DNSDomainNameResolver from
        // fe-core's system-loader copy and split-brain-poison SecurityUtil against the plugin copy (the same
        // failure ThriftHmsClient.doAs guards). buildHadoopConf pins only the OUTER conf's loader, not the TCCL
        // that SecurityUtil's own Configuration reads — so the thread pin is required here too.
        // (HadoopKerberosAuthenticator's keytab login is lazy in getUGI, already under doAs's pin.)
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(HiveConnector.class.getClassLoader());
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
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
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

    /**
     * Releases everything this gateway owns: its HMS client, and the embedded iceberg / hudi sibling
     * connectors whose lifecycle it owns (the engine closes only a catalog's PRIMARY connector, so a sibling
     * is never reached from anywhere else). Idempotent, and a no-op for anything that was never built.
     *
     * <p><b>Every stage runs, even when an earlier one throws.</b> The iceberg stage tears down a live iceberg
     * {@code Catalog} and {@code BaseViewSessionCatalog} — a REST session or an HMS catalog pool, real network
     * teardown declared {@code throws IOException} — and the hudi stage is the ONLY release of that sibling's
     * shared {@code FS_SCOPES} reference count. Letting one dead HTTP connection skip that release would
     * strand the UGI, every FileSystem Hadoop cached under it, and those filesystems' AWS SDK executor
     * threads for the life of the FE — the "OutOfMemoryError: unable to create native thread" this feature
     * exists to prevent — with no second chance, because {@code PluginDrivenExternalCatalog.closeResources}
     * swallows the throw and then nulls its connector field. So the stages are independent: each failure is
     * named and folded into the first one as a suppressed exception (the {@code HiveConnectorTransaction}
     * convention), rather than a nested try/finally whose finally-throw would drop the primary failure.
     *
     * <p><b>Detach under the monitor, close outside it.</b> The three fields are read and nulled inside
     * {@code synchronized (this)} together with {@code closed}, so a concurrent lazy build either finishes
     * first (and is closed here) or fails fast: it can never hand back a half-torn-down client, nor build a
     * sibling that no later close() could reach. The closing itself is external IO and must NOT hold the
     * monitor {@code getOrCreateClient()} needs — on the ALTER CATALOG path this runs on the FE's journal
     * replay thread under the CatalogMgr write lock. Same shape, and the same reason, as
     * {@code PluginDrivenExternalCatalog.closeResources}, which detaches every stage before invoking
     * external code.
     */
    @Override
    public void close() throws IOException {
        HmsClient client;
        Connector iceberg;
        Connector hudi;
        synchronized (this) {
            closed = true;
            client = hmsClient;
            hmsClient = null;
            iceberg = icebergSibling;
            icebergSibling = null;
            hudi = hudiSibling;
            hudiSibling = null;
        }
        IOException failure = closeStage(client, "hive metastore client", null);
        failure = closeStage(iceberg, "iceberg-on-HMS sibling connector", failure);
        failure = closeStage(hudi, "hudi-on-HMS sibling connector", failure);
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Closes one stage of {@link #close()} and folds its failure into the running one, so that a failing
     * stage never costs the stages after it. Returns {@code running} unchanged when the stage was never built
     * or closed cleanly; otherwise the first failure, naming the stage, with every later one attached to it
     * as suppressed.
     *
     * <p>{@code LinkageError} is caught alongside {@code Exception} for the same reason
     * {@code HudiConnector.releaseFileSystemScope} catches it: a sibling closes classes loaded child-first by
     * its own plugin loader, so a loader split surfaces as an {@code Error} rather than an exception, and
     * that must not be the thing that strands a filesystem scope. Harder errors (OOM, StackOverflow) are left
     * to propagate — at that point the FE has bigger problems than an orderly catalog teardown.
     */
    private IOException closeStage(AutoCloseable stage, String what, IOException running) {
        if (stage == null) {
            return running;
        }
        try {
            stage.close();
            return running;
        } catch (Exception | LinkageError t) {
            IOException named = new IOException("failed to close the " + what + " of hive catalog '"
                    + context.getCatalogName() + "'", t);
            if (running == null) {
                return named;
            }
            running.addSuppressed(named);
            return running;
        }
    }
}
