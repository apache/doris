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
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.connector.api.event.ConnectorEventSource;
import org.apache.doris.connector.api.event.EventPollRequest;
import org.apache.doris.connector.api.event.EventPollResult;
import org.apache.doris.connector.api.event.MetastoreChangeDescriptor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
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
 * plugin classloader (covering the notification RPC and the JSON/GZIP deserialization), mirroring
 * {@code PluginDrivenScanNode.onPluginClassLoader}; the daemon thread does not inherit any pin.
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

    private void realRun() {
        List<Long> catalogIds = Env.getCurrentEnv().getCatalogMgr().getCatalogIds();
        for (Long catalogId : catalogIds) {
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
            if (!(catalog instanceof PluginDrivenExternalCatalog)) {
                continue;
            }
            PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
            // Only probe already-initialized catalogs — do not force-init an idle catalog just to ask
            // whether it has an event source (the legacy poller never touched non-HMS catalogs; an idle
            // catalog has no warm cache to keep fresh and re-reads on its first access anyway).
            if (!pluginCatalog.isInitialized()) {
                continue;
            }
            ConnectorEventSource eventSource;
            try {
                eventSource = pluginCatalog.getConnector().getEventSource();
            } catch (RuntimeException e) {
                // uninitialized / unavailable connector this cycle => skip (mirrors the legacy skip-on-throw)
                continue;
            }
            if (eventSource == null) {
                continue;
            }
            try {
                syncCatalog(pluginCatalog, eventSource);
            } catch (Exception e) {
                LOG.warn("Failed to sync metastore events for catalog [{}]", pluginCatalog.getName(), e);
            }
        }
    }

    private void syncCatalog(PluginDrivenExternalCatalog catalog, ConnectorEventSource eventSource)
            throws Exception {
        long catalogId = catalog.getId();
        boolean isMaster = Env.getCurrentEnv().isMaster();
        long lastSyncedEventId = lastSyncedEventIdMap.getOrDefault(catalogId, -1L);
        long masterUpperBound = masterLastSyncedEventIdMap.getOrDefault(catalogId, -1L);

        EventPollRequest request = new EventPollRequest(lastSyncedEventId, isMaster, masterUpperBound);
        EventPollResult result = onPluginClassLoader(eventSource, () -> eventSource.pollOnce(request));

        if (result.isNeedsFullRefresh()) {
            // first sync or an events-gap: the master invalidates the whole catalog locally; a follower
            // forwards REFRESH CATALOG to the master. Then seed the cursor to the connector's current id.
            if (isMaster) {
                refreshCatalogForMaster(catalog);
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

        // Apply in order; on failure the cursor is rolled back to just before the failing descriptor and the
        // exception propagates (caught + logged in realRun), so the next cycle re-fetches from there and the
        // edit-log cursor below is NOT written (followers do not jump past a failed apply).
        applyDescriptors(catalog, descriptors);
        commitCursor(catalogId, result.getNewCursor(), isMaster);
    }

    // Stores the local cursor and, on the master, replicates it to followers via the edit-log.
    private void commitCursor(long catalogId, long newCursor, boolean isMaster) {
        lastSyncedEventIdMap.put(catalogId, newCursor);
        if (isMaster) {
            writeSyncedCursorLog(catalogId, newCursor);
        }
    }

    private void applyDescriptors(PluginDrivenExternalCatalog catalog,
            List<MetastoreChangeDescriptor> descriptors) {
        for (MetastoreChangeDescriptor descriptor : descriptors) {
            try {
                applyOne(catalog, descriptor);
            } catch (Exception e) {
                lastSyncedEventIdMap.put(catalog.getId(), descriptor.getEventId() - 1);
                throw new RuntimeException(
                        "Failed to apply metastore change " + descriptor + " on catalog "
                                + catalog.getName(), e);
            }
        }
    }

    // Applies one neutral descriptor via the engine's own (connector-agnostic) mutators — the same ones the
    // legacy event.process() bodies called, now generalized to work on a flipped catalog.
    private void applyOne(PluginDrivenExternalCatalog catalog, MetastoreChangeDescriptor descriptor)
            throws Exception {
        String catalogName = catalog.getName();
        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        switch (descriptor.getOp()) {
            case REGISTER_DATABASE:
                catalogMgr.registerExternalDatabaseFromEvent(descriptor.getDbName(), catalogName);
                break;
            case UNREGISTER_DATABASE:
                catalogMgr.unregisterExternalDatabase(descriptor.getDbName(), catalogName);
                break;
            case RENAME_DATABASE:
                // legacy AlterDatabaseEvent.processRename: skip when the after-db already exists locally
                if (catalog.getDbNullable(descriptor.getDbNameAfter()) == null) {
                    catalogMgr.unregisterExternalDatabase(descriptor.getDbName(), catalogName);
                    catalogMgr.registerExternalDatabaseFromEvent(descriptor.getDbNameAfter(), catalogName);
                }
                break;
            case REGISTER_TABLE:
                catalogMgr.registerExternalTableFromEvent(descriptor.getDbName(), descriptor.getTableName(),
                        catalogName, descriptor.getUpdateTime(), true);
                break;
            case UNREGISTER_TABLE:
                catalogMgr.unregisterExternalTable(descriptor.getDbName(), descriptor.getTableName(),
                        catalogName, true);
                break;
            case RENAME_TABLE:
                applyRenameTable(catalog, descriptor);
                break;
            case REFRESH_TABLE:
                Env.getCurrentEnv().getRefreshManager().refreshExternalTableFromEvent(catalogName,
                        descriptor.getDbName(), descriptor.getTableName(), descriptor.getUpdateTime());
                break;
            case ADD_PARTITIONS:
                catalogMgr.addExternalPartitions(catalogName, descriptor.getDbName(),
                        descriptor.getTableName(), descriptor.getPartitionNames(),
                        descriptor.getUpdateTime(), true);
                break;
            case DROP_PARTITIONS:
                catalogMgr.dropExternalPartitions(catalogName, descriptor.getDbName(),
                        descriptor.getTableName(), descriptor.getPartitionNames(),
                        descriptor.getUpdateTime(), true);
                break;
            case REFRESH_PARTITIONS:
                Env.getCurrentEnv().getRefreshManager().refreshPartitions(catalogName,
                        descriptor.getDbName(), descriptor.getTableName(),
                        descriptor.getPartitionNames(), descriptor.getUpdateTime(), true);
                break;
            default:
                break;
        }
    }

    private void applyRenameTable(PluginDrivenExternalCatalog catalog, MetastoreChangeDescriptor descriptor)
            throws Exception {
        String catalogName = catalog.getName();
        CatalogMgr catalogMgr = Env.getCurrentEnv().getCatalogMgr();
        // legacy AlterTableEvent: a rename to a DIFFERENT key that already exists locally is skipped
        // (processRename guard); a view recreate (after == before) always proceeds (processRecreateTable).
        boolean sameKey = descriptor.getDbName().equalsIgnoreCase(descriptor.getDbNameAfter())
                && descriptor.getTableName().equalsIgnoreCase(descriptor.getTableNameAfter());
        if (!sameKey && catalogMgr.externalTableExistInLocal(descriptor.getDbNameAfter(),
                descriptor.getTableNameAfter(), catalogName)) {
            return;
        }
        catalogMgr.unregisterExternalTable(descriptor.getDbName(), descriptor.getTableName(),
                catalogName, true);
        catalogMgr.registerExternalTableFromEvent(descriptor.getDbNameAfter(),
                descriptor.getTableNameAfter(), catalogName, descriptor.getUpdateTime(), true);
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

    private static <T> T onPluginClassLoader(ConnectorEventSource eventSource, Supplier<T> body) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(eventSource.getClass().getClassLoader());
            return body.get();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
