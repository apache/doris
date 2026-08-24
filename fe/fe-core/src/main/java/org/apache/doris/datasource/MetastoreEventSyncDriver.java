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

package org.apache.doris.datasource;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.event.ConnectorEventSource;
import org.apache.doris.connector.spi.event.EventPollRequest;
import org.apache.doris.connector.spi.event.EventPollResult;
import org.apache.doris.connector.spi.event.MetastoreChangeDescriptor;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.log.MetaIdMappingsLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The connector-agnostic, role-aware driver of incremental metastore-event sync. It is the fe-core half
 * of the metastore-event relocation: it iterates catalogs, asks each connector that exposes a
 * {@link ConnectorEventSource} for a batch of neutral {@link MetastoreChangeDescriptor}s, and applies
 * them to the engine's catalog&#8594;db&#8594;table object graph and caches — the plugin never touches
 * {@code CatalogMgr}, {@code EditLog}, or the HA state.
 *
 * <p><b>Engine/connector split (Trino-aligned).</b> The engine owns everything stateful and replicated:
 * the per-catalog cursor, the master/follower role, the edit-log write of the synced cursor, and the
 * follower&#8594;master {@code REFRESH CATALOG} forward. The connector owns only the metastore fetch +
 * message parse behind {@code pollOnce}. This mirrors the legacy {@code MetastoreEventsProcessor} role
 * logic exactly, but the source-specific work is now behind the SPI and the type gate
 * ({@code instanceof HMSExternalCatalog}) is replaced by a capability probe ({@code getEventSource() != null}).
 *
 * <p><b>Dormant until the flip.</b> Only a {@link PluginDrivenExternalCatalog} whose connector exposes an
 * event source is driven; pre-flip no such catalog exists, so this daemon is inert. At the flip the legacy
 * poller's gate goes false and this driver takes over, and the {@code MetaIdMappingsLog} replay handler is
 * repointed to feed THIS driver's follower cursor (see {@link #updateMasterLastSyncedEventId}).
 *
 * <p><b>Classloader.</b> {@code pollOnce} runs under a context-classloader pin to the event source's own
 * plugin classloader (covering the notification RPC and the JSON/GZIP deserialization). Descriptor
 * application and master-side full refresh run under the connector's plugin classloader because they may
 * call connector invalidation and identifier hooks. The daemon thread does not inherit any pin.
 */
public class MetastoreEventSyncDriver extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(MetastoreEventSyncDriver.class);

    // This FE's per-catalog synced cursor. Not persisted (rebuilt as -1 on restart); single-threaded (the
    // daemon thread), so a plain HashMap is fine.
    private final Map<Long, Long> lastSyncedEventIdMap = Maps.newHashMap();
    // The master's committed high-water mark per catalog, learned on followers via edit-log replay. A
    // follower never fetches past it. Only meaningful on followers.
    private final Map<Long, Long> masterLastSyncedEventIdMap = Maps.newHashMap();

    private boolean isRunning;

    public MetastoreEventSyncDriver() {
        super(MetastoreEventSyncDriver.class.getName(), Config.hms_events_polling_interval_ms);
        this.isRunning = false;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (isRunning) {
            LOG.warn("Last metastore-event sync task not finished, ignore current task.");
            return;
        }
        isRunning = true;
        try {
            realRun();
        } catch (Exception ex) {
            LOG.warn("Metastore-event sync task failed", ex);
        }
        isRunning = false;
    }

    /**
     * One-shot force-initialization of a catalog nobody has queried on this FE, so it can obtain its event
     * source and seed its cursor.
     *
     * <p>Flip-time force-init parity: the legacy {@code MetastoreEventsProcessor} force-initialized EVERY hms
     * catalog every cycle on every FE (via {@code getHmsProperties() -> makeSureInitialized()}), so a flipped
     * hms catalog seeds its cursor even if it is never queried on this FE. That is required on followers too
     * (each FE runs its own driver with its own cursor, and a follower must have the catalog initialized to
     * obtain its event source, seed its cursor and forward {@code REFRESH CATALOG}) — hence no isMaster gate.
     *
     * <p>Only types that DECLARE an event source get this, so idle paimon/iceberg/jdbc catalogs stay
     * byte-inert. The declaration is read off the connector PROVIDER, keyed on the pre-init type string:
     * {@code getType()} reads catalogProperty and does NOT force-init, whereas asking the connector itself
     * would force-initialize exactly the idle catalogs this check exists to leave alone. The caller guards on
     * {@code !isInitialized()}, so this is one-shot per catalog — later cycles take the initialized path.
     *
     * @return whether the catalog is now initialized and can be polled this cycle
     */
    boolean seedCursorOfUninitializedCatalog(PluginDrivenExternalCatalog pluginCatalog) {
        boolean declaresEventSource = ConnectorFactory
                .findProvider(pluginCatalog.getType(), pluginCatalog.getProperties())
                .map(ConnectorProvider::providesEventSource)
                .orElse(false);
        if (!declaresEventSource) {
            return false;
        }
        try {
            pluginCatalog.makeSureInitialized();
        } catch (Exception e) {
            // Missing/invalid params this cycle -> skip (mirrors the legacy skip-on-throw around
            // getHmsProperties()); retried next cycle, the error is already surfaced via SHOW CATALOGS.
            return false;
        }
        return true;
    }

    private void realRun() {
        List<Long> catalogIds = Env.getCurrentEnv().getCatalogMgr().getCatalogIds();
        for (Long catalogId : catalogIds) {
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
            if (!(catalog instanceof PluginDrivenExternalCatalog)) {
                continue;
            }
            PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
            if (!pluginCatalog.isInitialized() && !seedCursorOfUninitializedCatalog(pluginCatalog)) {
                continue;
            }
            Connector connector;
            ConnectorEventSource eventSource;
            try {
                connector = pluginCatalog.getConnector();
                eventSource = onPluginClassLoader(
                        connector.getClass().getClassLoader(), connector::getEventSource);
            } catch (RuntimeException e) {
                // uninitialized / unavailable connector this cycle => skip (mirrors the legacy skip-on-throw)
                continue;
            }
            if (eventSource == null) {
                continue;
            }
            try {
                syncCatalog(pluginCatalog, connector, eventSource);
            } catch (Exception e) {
                // Self-heal (mirrors the legacy poller's onRefreshCache(true) + reset-to-(-1)): reset the
                // cursor so the next cycle first-pulls -> full refresh, jumping past a deterministically-failing
                // (poison) event/descriptor instead of retrying it forever and wedging the catalog's sync (and,
                // on the master, freezing every follower that waits on the replicated cursor). Transient FETCH
                // errors do not reach here — HmsEventSource retries them in place (ofNothing) — so this reset
                // fires only on a deterministic parse/apply failure.
                lastSyncedEventIdMap.put(catalogId, -1L);
                LOG.warn("Failed to sync metastore events for catalog [{}]; reset cursor for a full re-sync",
                        pluginCatalog.getName(), e);
            }
        }
    }

    private void syncCatalog(PluginDrivenExternalCatalog catalog, Connector connector,
            ConnectorEventSource eventSource) throws Exception {
        long catalogId = catalog.getId();
        boolean isMaster = Env.getCurrentEnv().isMaster();
        long lastSyncedEventId = lastSyncedEventIdMap.getOrDefault(catalogId, -1L);
        long masterUpperBound = masterLastSyncedEventIdMap.getOrDefault(catalogId, -1L);

        EventPollRequest request = new EventPollRequest(lastSyncedEventId, isMaster, masterUpperBound);
        EventPollResult result = onPluginClassLoader(
                eventSource.getClass().getClassLoader(), () -> eventSource.pollOnce(request));

        if (result.isNeedsFullRefresh()) {
            // first sync or an events-gap: the master invalidates the whole catalog locally; a follower
            // forwards REFRESH CATALOG to the master. Then seed the cursor to the connector's current id.
            if (isMaster) {
                onPluginClassLoader(connector.getClass().getClassLoader(), () -> {
                    refreshCatalogForMaster(catalog);
                    return null;
                });
            } else {
                refreshCatalogForSlave(catalog);
            }
            commitCursor(catalogId, result.getNewCursor(), isMaster);
            return;
        }

        List<MetastoreChangeDescriptor> descriptors = result.getDescriptors();
        if (descriptors.isEmpty()) {
            // nothing to apply; still advance the cursor if it moved (e.g. a batch of ignored events)
            if (result.getNewCursor() != lastSyncedEventId) {
                commitCursor(catalogId, result.getNewCursor(), isMaster);
            }
            return;
        }

        // Apply in order; on failure the exception propagates and realRun's catch resets the cursor to -1
        // (self-heal), so the edit-log cursor below is NOT written (followers do not jump past a failed apply)
        // and the next cycle first-pulls a clean full refresh instead of retrying the poison descriptor.
        onPluginClassLoader(connector.getClass().getClassLoader(), () -> {
            applyDescriptors(catalog, connector, descriptors);
            return null;
        });
        commitCursor(catalogId, result.getNewCursor(), isMaster);
    }

    // Stores the local cursor and, on the master, replicates it to followers via the edit-log.
    private void commitCursor(long catalogId, long newCursor, boolean isMaster) {
        lastSyncedEventIdMap.put(catalogId, newCursor);
        if (isMaster) {
            writeSyncedCursorLog(catalogId, newCursor);
        }
    }

    private void applyDescriptors(PluginDrivenExternalCatalog catalog, Connector connector,
            List<MetastoreChangeDescriptor> descriptors) {
        for (MetastoreChangeDescriptor descriptor : descriptors) {
            try {
                applyOne(catalog, connector, descriptor);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to apply metastore change " + descriptor + " on catalog "
                                + catalog.getName(), e);
            }
        }
    }

    // Applies one neutral descriptor via the engine's own (connector-agnostic) mutators — the same ones the
    // legacy event.process() bodies called, now generalized to work on a flipped catalog.
    private void applyOne(PluginDrivenExternalCatalog catalog, Connector connector,
            MetastoreChangeDescriptor descriptor) throws Exception {
        if (!affectsConstraintMetadata(descriptor)) {
            applyOneInternal(catalog, connector, descriptor);
            return;
        }
        try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                catalog.beginConstraintMetadataMutation()) {
            applyOneInternal(catalog, connector, descriptor);
        }
    }

    private void applyOneInternal(PluginDrivenExternalCatalog catalog, Connector connector,
            MetastoreChangeDescriptor descriptor) throws Exception {
        EventIdentity before = EventIdentity.from(
                catalog, descriptor.getDbName(), descriptor.getTableName());
        EventIdentity after = descriptor.getDbNameAfter() == null ? null : EventIdentity.from(
                catalog, descriptor.getDbNameAfter(), descriptor.getTableNameAfter());
        String catalogName = catalog.getName();
        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        invalidateStructuralEventCaches(connector, descriptor.getOp(), before, after);
        switch (descriptor.getOp()) {
            case REGISTER_DATABASE:
                catalogMgr.registerExternalDatabaseFromEvent(
                        before.remoteDbName, before.localDbName, catalogName);
                break;
            case UNREGISTER_DATABASE:
                catalogMgr.unregisterExternalDatabaseFromEvent(before.localDbName, catalogName);
                Env.getCurrentEnv().getConstraintManager()
                        .dropDatabaseConstraints(catalogName, before.localDbName);
                break;
            case RENAME_DATABASE:
                // Always converge to "old removed, new registered". A normal lookup may already have warmed the
                // target after the remote rename; treating that as a reason to skip would retain stale old state.
                catalogMgr.unregisterExternalDatabaseFromEvent(before.localDbName, catalogName);
                catalogMgr.registerExternalDatabaseFromEvent(
                        after.remoteDbName, after.localDbName, catalogName);
                Env.getCurrentEnv().getConstraintManager().renameDatabase(
                        catalogName, before.localDbName, after.localDbName);
                break;
            case REGISTER_TABLE:
                catalogMgr.registerExternalTableFromEvent(before.localDbName,
                        before.remoteTableName, before.localTableName, catalogName, descriptor.getUpdateTime());
                break;
            case UNREGISTER_TABLE:
                catalogMgr.unregisterExternalTableFromEvent(
                        before.localDbName, before.localTableName, catalogName);
                Env.getCurrentEnv().getConstraintManager().dropTableConstraints(
                        new TableNameInfo(catalogName, before.localDbName, before.localTableName));
                break;
            case RENAME_TABLE:
                // Always converge to "old removed, new registered". The target may already be hot because a normal
                // lookup raced with the event after the remote rename; it must not prevent cleanup of the old identity.
                catalogMgr.unregisterExternalTableFromEvent(
                        before.localDbName, before.localTableName, catalogName);
                catalogMgr.registerExternalTableFromEvent(after.localDbName,
                        after.remoteTableName, after.localTableName,
                        catalogName, descriptor.getUpdateTime());
                Env.getCurrentEnv().getConstraintManager().renameTable(
                        new TableNameInfo(catalogName, before.localDbName, before.localTableName),
                        new TableNameInfo(catalogName, after.localDbName, after.localTableName));
                break;
            case REFRESH_TABLE:
                Env.getCurrentEnv().getRefreshManager().refreshExternalTableFromEvent(
                        catalogName, before.localDbName, before.localTableName, descriptor.getUpdateTime());
                break;
            case ADD_PARTITIONS:
                catalogMgr.addExternalPartitionsFromEvent(catalogName,
                        before.localDbName, before.localTableName,
                        descriptor.getPartitionNames(), descriptor.getUpdateTime());
                break;
            case DROP_PARTITIONS:
                catalogMgr.dropExternalPartitionsFromEvent(catalogName,
                        before.localDbName, before.localTableName,
                        descriptor.getPartitionNames(), descriptor.getUpdateTime());
                break;
            case REFRESH_PARTITIONS:
                Env.getCurrentEnv().getRefreshManager().refreshPartitionsFromEvent(catalogName,
                        before.localDbName, before.localTableName,
                        descriptor.getPartitionNames(), descriptor.getUpdateTime());
                break;
            default:
                break;
        }
    }

    private boolean affectsConstraintMetadata(MetastoreChangeDescriptor descriptor) {
        switch (descriptor.getOp()) {
            case REGISTER_DATABASE:
            case UNREGISTER_DATABASE:
            case RENAME_DATABASE:
            case REGISTER_TABLE:
            case UNREGISTER_TABLE:
            case RENAME_TABLE:
            case REFRESH_TABLE:
                return true;
            case ADD_PARTITIONS:
            case DROP_PARTITIONS:
            case REFRESH_PARTITIONS:
                return false;
            default:
                throw new IllegalStateException(
                        "Unsupported metastore change op: " + descriptor.getOp());
        }
    }

    /**
     * Invalidates connector-owned caches for structural events before publishing the corresponding FE mutation.
     * Connector cache keys use remote identities; FE mutators below use the local identities from the same values.
     */
    private void invalidateStructuralEventCaches(Connector connector,
            MetastoreChangeDescriptor.Op op, EventIdentity before, EventIdentity after) {
        switch (op) {
            case REGISTER_DATABASE:
            case UNREGISTER_DATABASE:
                connector.invalidateDb(before.remoteDbName);
                break;
            case RENAME_DATABASE:
                connector.invalidateDb(before.remoteDbName);
                if (!Objects.equals(before.remoteDbName, after.remoteDbName)) {
                    connector.invalidateDb(after.remoteDbName);
                }
                break;
            case REGISTER_TABLE:
            case UNREGISTER_TABLE:
                connector.invalidateTable(before.remoteDbName, before.remoteTableName);
                break;
            case RENAME_TABLE:
                connector.invalidateTable(before.remoteDbName, before.remoteTableName);
                if (!Objects.equals(before.remoteDbName, after.remoteDbName)
                        || !Objects.equals(before.remoteTableName, after.remoteTableName)) {
                    connector.invalidateTable(after.remoteDbName, after.remoteTableName);
                }
                break;
            default:
                break;
        }
    }

    /** One remote connector identity and its canonical FE-local counterpart. */
    private static final class EventIdentity {
        private final String remoteDbName;
        private final String localDbName;
        private final String remoteTableName;
        private final String localTableName;

        private EventIdentity(String remoteDbName, String localDbName,
                String remoteTableName, String localTableName) {
            this.remoteDbName = remoteDbName;
            this.localDbName = localDbName;
            this.remoteTableName = remoteTableName;
            this.localTableName = localTableName;
        }

        private static EventIdentity from(PluginDrivenExternalCatalog catalog,
                String remoteDbName, String remoteTableName) {
            Objects.requireNonNull(remoteDbName, "remote database name");
            String localDbName = catalog.canonicalLocalDatabaseNameFromRemote(remoteDbName);
            String localTableName = remoteTableName == null
                    ? null : catalog.canonicalLocalTableNameFromRemote(remoteDbName, remoteTableName);
            return new EventIdentity(remoteDbName, localDbName, remoteTableName, localTableName);
        }
    }

    // Writes the synced-event-id cursor to the edit-log so followers advance to it (the log's only live
    // purpose; the id-mapping payload the legacy path also wrote is vestigial — its getters have no
    // production reader — so a cursor-only log is written). Opcode + neutral GSON format are unchanged.
    private void writeSyncedCursorLog(long catalogId, long cursor) {
        MetaIdMappingsLog log = new MetaIdMappingsLog();
        log.setCatalogId(catalogId);
        log.setFromHmsEvent(true);
        log.setLastSyncedEventId(cursor);
        Env.getCurrentEnv().getExternalMetaIdMgr().replayMetaIdMappingsLog(log);
        Env.getCurrentEnv().getEditLog().logMetaIdMappingsLog(log);
    }

    private void refreshCatalogForMaster(CatalogIf catalog) {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalog.getId());
        log.setInvalidCache(true);
        Env.getCurrentEnv().getRefreshManager().replayRefreshCatalog(log);
    }

    private void refreshCatalogForSlave(CatalogIf catalog) throws Exception {
        // A follower cannot refresh a catalog locally (that mutation must originate on the master); forward
        // REFRESH CATALOG to the master, which replicates the result back.
        String sql = "REFRESH CATALOG " + catalog.getName();
        OriginStatement originStmt = new OriginStatement(sql, 0);
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setEnv(Env.getCurrentEnv());
        MasterOpExecutor masterOpExecutor = new MasterOpExecutor(originStmt, ctx,
                RedirectStatus.FORWARD_WITH_SYNC, false);
        masterOpExecutor.execute();
    }

    /**
     * Advances a follower's known master-committed cursor for a catalog. Wired from the
     * {@code MetaIdMappingsLog} edit-log replay at the flip (mirrors the legacy
     * {@code MetastoreEventsProcessor.updateMasterLastSyncedEventId}); keyed by catalog id only, so it never
     * casts the catalog to a source-specific type.
     */
    public void updateMasterLastSyncedEventId(long catalogId, long eventId) {
        masterLastSyncedEventIdMap.put(catalogId, eventId);
    }

    private static <T> T onPluginClassLoader(ClassLoader pluginClassLoader, Supplier<T> body) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginClassLoader);
            return body.get();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
