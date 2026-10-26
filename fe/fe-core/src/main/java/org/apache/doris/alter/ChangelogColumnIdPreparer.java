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

package org.apache.doris.alter;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.ChangelogColumnIdentityInfo;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService.PFetchColIdsRequest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/** Resolves persisted legacy identities from storage before planning a changelog write or DDL. */
public final class ChangelogColumnIdPreparer {
    private static final ConcurrentHashMap<OlapTable, CompletableFuture<Void>> IN_FLIGHT = new ConcurrentHashMap<>();

    private ChangelogColumnIdPreparer() {
    }

    /** Must be called outside catalog locks. All concurrent callers wait for journal durability. */
    public static void ensureReady(Database db, OlapTable table) throws UserException {
        // Changelog enablement cannot be altered after creation. Preserve ordinary writers' lock timeouts.
        if (!table.hasChangelog()) {
            return;
        }
        CompletableFuture<Void> completion = new CompletableFuture<>();
        CompletableFuture<Void> existing = IN_FLIGHT.putIfAbsent(table, completion);
        if (existing != null) {
            try {
                existing.get();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UserException("Interrupted preparing changelog column identities", e);
            } catch (ExecutionException e) {
                throw new UserException("Failed to prepare changelog column identities", e.getCause());
            }
        }
        try {
            prepare(db, table);
            completion.complete(null);
        } catch (UserException | RuntimeException | Error e) {
            completion.completeExceptionally(e);
            throw e;
        } finally {
            IN_FLIGHT.remove(table, completion);
        }
    }

    private static void prepare(Database db, OlapTable table) throws UserException {
        AlterLightSchChangeHelper helper = new AlterLightSchChangeHelper(db, table);
        Map<Long, PFetchColIdsRequest> requests;
        String captured;
        table.readLock();
        try {
            if (!needsPreparation(table)) {
                return;
            }
            if (Config.isCloudMode()) {
                throw new UserException("Legacy changelog column identity alignment is not supported in cloud mode");
            }
            if (!Env.getCurrentEnv().isMaster()) {
                throw new UserException("Changelog column identities must be prepared on the master");
            }
            captured = fingerprint(table);
            requests = helper.initParams();
        } finally {
            table.readUnlock();
        }
        Map<Long, Map<String, Integer>> fetched;
        try {
            fetched = helper.callForColumnsInfo(requests).getIndexIdToColumnInfo();
        } catch (IllegalStateException e) {
            throw new UserException("Failed to fetch changelog column identities", e);
        }
        EditLogItem journal;
        table.writeLockOrDdlException();
        try {
            if (!captured.equals(fingerprint(table))) {
                throw new UserException("Table schema or replicas changed while preparing column identities; retry");
            }
            ChangelogColumnIdentityInfo info = prepareInfo(db.getId(), table, fetched);
            info.apply(table);
            // Preserve metadata journal ordering. Non-batch EditLog retains its synchronous write discipline.
            journal = Env.getCurrentEnv().getEditLog().logChangelogColumnIdentity(info);
        } finally {
            table.writeUnlock();
        }
        journal.await();
    }

    private static boolean needsPreparation(OlapTable table) {
        return table.hasChangelog() && table.getIndexIdToMeta(true).values().stream()
                .flatMap(meta -> meta.getSchema(true).stream()).anyMatch(column -> column.getUniqueId() < 0);
    }

    private static String fingerprint(OlapTable table) throws UserException {
        if (table.getState() != OlapTableState.NORMAL || table.isDropped) {
            throw new UserException("Changelog column identities require a normal table");
        }
        Map<Long, Map<Long, Map<Long, Map<Long, String>>>> topology = new TreeMap<>();
        for (Partition partition : table.getAllPartitions()) {
            Map<Long, Map<Long, Map<Long, String>>> indexes = new TreeMap<>();
            topology.put(partition.getId(), indexes);
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL, true)) {
                Map<Long, Map<Long, String>> tablets = new TreeMap<>();
                indexes.put(index.getId(), tablets);
                for (Tablet tablet : index.getTablets()) {
                    if (tablet.getNormalReplicaBackendIds().isEmpty()) {
                        throw new UserException("No readable replica for tablet " + tablet.getId());
                    }
                    Map<Long, String> replicas = new TreeMap<>();
                    tablets.put(tablet.getId(), replicas);
                    for (Replica replica : tablet.getReplicas()) {
                        replicas.put(replica.getId(), replica.getBackendIdWithoutException() + ":" + replica.getState()
                                + ":" + replica.isBad() + ":" + replica.isChangelogMissing()
                                + ":" + new TreeSet<>(tablet.getNormalReplicaBackendIds()));
                    }
                }
            }
        }
        return GsonUtils.GSON.toJson(new TreeMap<>(table.getIndexIdToMeta(true)))
                + GsonUtils.GSON.toJson(topology) + table.getBaseIndexId()
                + table.getEnableLightSchemaChange() + GsonUtils.GSON.toJson(table.getChangelogConfig());
    }

    static ChangelogColumnIdentityInfo prepareInfo(long dbId, OlapTable table,
            Map<Long, Map<String, Integer>> fetched) throws UserException {
        if (!fetched.keySet().equals(table.getIndexIdToMeta(true).keySet())) {
            throw new UserException("Incomplete index coverage in column identity response");
        }
        Map<Long, Map<String, Integer>> ids = new HashMap<>();
        Map<Long, Integer> previousVersions = new HashMap<>();
        Map<Long, Integer> versions = new HashMap<>();
        Map<Long, Integer> maximums = new HashMap<>();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : table.getIndexIdToMeta(true).entrySet()) {
            MaterializedIndexMeta meta = entry.getValue();
            Map<String, Integer> actual = fetched.get(entry.getKey());
            Set<Integer> unique = new HashSet<>();
            boolean changed = false;
            int max = meta.getMaxColUniqueId();
            if (actual.size() != meta.getSchema(true).size()) {
                throw new UserException("Column identity response does not cover index " + entry.getKey());
            }
            for (Column column : meta.getSchema(true)) {
                Integer id = actual.get(column.getName());
                if (id == null || id < 0 || !unique.add(id)) {
                    throw new UserException("Invalid column identity for " + column.getName());
                }
                if (column.getUniqueId() >= 0 && column.getUniqueId() != id) {
                    throw new UserException("Storage column identity differs for " + column.getName());
                }
                changed |= column.getUniqueId() != id;
                max = Math.max(max, id);
            }
            ids.put(entry.getKey(), new HashMap<>(actual));
            previousVersions.put(entry.getKey(), meta.getSchemaVersion());
            versions.put(entry.getKey(), meta.getSchemaVersion() + (changed ? 1 : 0));
            maximums.put(entry.getKey(), max);
        }
        return new ChangelogColumnIdentityInfo(dbId, table.getId(), ids, previousVersions, versions, maximums);
    }
}
