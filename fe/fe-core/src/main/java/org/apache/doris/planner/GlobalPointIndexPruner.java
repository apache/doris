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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Plan-time tablet pruning with GLOBAL_POINT indexes.
 *
 * <p>A GLOBAL_POINT index keeps one bloom filter per rowset for one column. For an equality or IN
 * predicate on that column, FE sends the encoded probe values to every BE that holds a selected
 * tablet (one RPC per BE), and each BE answers which of its tablets may still contain a match at
 * the query's snapshot version. Tablets that are a definite miss are dropped from the scan.
 *
 * <p>Correctness rule: a tablet is dropped only on a definite miss. Every other outcome (no local
 * tablet on the BE, version not synced, missing or unreadable bloom, unreachable BE, RPC error or
 * timeout, unsupported predicate or type) keeps the tablet. So a broken or incomplete index can make
 * a query slower, but never changes its result.
 */
final class GlobalPointIndexPruner {
    private static final Logger LOG = LogManager.getLogger(GlobalPointIndexPruner.class);

    /** Outcome of one probe round, also used for the EXPLAIN line. */
    static final class Result {
        // partitionId -> tablets to keep. Contains every probed partition.
        final Map<Long, Set<Long>> candidatesByPartition = new HashMap<>();
        final String columnName;
        final long probeValueCount;
        long tabletsBefore = 0;
        long tabletsAfter = 0;
        // Tablets kept only because they could not be checked.
        long degradedTablets = 0;

        Result(String columnName, long probeValueCount) {
            this.columnName = columnName;
            this.probeValueCount = probeValueCount;
        }
    }

    private GlobalPointIndexPruner() {
    }

    /**
     * Returns the first GLOBAL_POINT-indexed column that has a filter in {@code columnFilters}, or
     * null. First match rather than most selective: picking the most selective column would need
     * NDV statistics, and the target workload has a single near-unique indexed column.
     */
    static Column findTargetColumn(OlapTable table, Map<String, PartitionColumnFilter> columnFilters) {
        if (table.getIndexes() == null) {
            return null;
        }
        for (Index index : table.getIndexes()) {
            if (index.getIndexType() != IndexType.GLOBAL_POINT
                    || index.getColumns() == null || index.getColumns().isEmpty()) {
                continue;
            }
            String columnName = index.getColumns().get(0);
            if (!columnFilters.containsKey(columnName)) {
                continue;
            }
            Column column = table.getColumn(columnName);
            if (column != null) {
                return column;
            }
        }
        return null;
    }

    /**
     * Returns the encoded probe values of an EQ or IN filter on {@code column}, or null if the filter
     * is anything else, has more than {@code Config.global_point_index_max_probe_values} values, or
     * has any value that cannot be encoded. Encoding only some of the values is not allowed: a value
     * that is left out could be the one that matches.
     */
    static List<ByteString> extractProbeValues(PartitionColumnFilter filter, Column column) {
        if (filter == null) {
            return null;
        }
        List<LiteralExpr> literals = new ArrayList<>();
        if (filter.getInPredicate() != null) {
            InPredicate inPredicate = filter.getInPredicate();
            if (inPredicate.isNotIn()) {
                return null;
            }
            for (Expr child : inPredicate.getListChildren()) {
                if (!(child instanceof LiteralExpr)) {
                    return null;
                }
                literals.add((LiteralExpr) child);
            }
        } else if (filter.lowerBound != null && filter.upperBound != null
                && filter.lowerBoundInclusive && filter.upperBoundInclusive
                && filter.lowerBound.compareLiteral(filter.upperBound) == 0) {
            literals.add(filter.lowerBound);
        } else {
            return null;
        }

        if (literals.isEmpty() || literals.size() > Config.global_point_index_max_probe_values) {
            return null;
        }
        List<ByteString> probeValues = new ArrayList<>(literals.size());
        for (LiteralExpr literal : literals) {
            byte[] encoded = encodeProbeValue(literal, column);
            if (encoded == null) {
                return null;
            }
            probeValues.add(ByteString.copyFrom(encoded));
        }
        return probeValues;
    }

    /**
     * Encodes one literal to the exact bytes the BE write path inserts into the bloom filter for
     * that value, or returns null if the type is not supported here.
     *
     * <p>The BE inserts the column's in-memory representation: the raw bytes of a string, and the
     * little-endian two's-complement value of a fixed-width integer. Only these two families are
     * encoded on FE. CHAR (padding), LARGEINT and the date types (packed internal formats) are
     * indexed on BE but not probed at planning time: a wrong encoding would be a false negative and
     * drop a tablet that has the value, so they are left to the scan-time check instead.
     */
    @VisibleForTesting
    static byte[] encodeProbeValue(LiteralExpr literal, Column column) {
        PrimitiveType colType = column.getDataType();
        switch (colType) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT: {
                if (!(literal instanceof IntLiteral)) {
                    return null;
                }
                long value = literal.getLongValue();
                switch (colType) {
                    case TINYINT:
                        return ByteBuffer.allocate(1).put((byte) value).array();
                    case SMALLINT:
                        return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) value).array();
                    case INT:
                        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt((int) value).array();
                    default:
                        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
                }
            }
            case VARCHAR:
            case STRING: {
                if (!(literal instanceof StringLiteral)) {
                    return null;
                }
                return literal.getStringValue().getBytes(StandardCharsets.UTF_8);
            }
            default:
                return null;
        }
    }

    /**
     * Probes every tablet of {@code partitionIds} and returns the tablets to keep, per partition.
     *
     * <p>Tablets are grouped by BE across all partitions, so each BE gets exactly one RPC and the
     * number of RPCs grows with the number of BEs, not tablets.
     *
     * <p>{@code visibleVersionMap} must be the same snapshot versions the query will scan. Probing an
     * older version could miss rowsets the query reads, and drop a tablet that has a match.
     */
    static Result probe(OlapTable table, long selectedIndexId, Collection<Long> partitionIds, Column column,
            List<ByteString> probeValues, Map<Long, Long> visibleVersionMap) {
        Result result = new Result(column.getName(), probeValues.size());
        Map<Long, Long> tabletIdToPartitionId = new HashMap<>();
        Map<Long, List<InternalService.PGpPruneTablet>> beIdToTablets = new HashMap<>();

        for (Long partitionId : partitionIds) {
            Partition partition = table.getPartition(partitionId);
            MaterializedIndex selectedIndex = table.getPartitionIndex(partition, selectedIndexId);
            List<Long> allTabletIds = selectedIndex.getTabletIdsInOrder();
            result.tabletsBefore += allTabletIds.size();
            Set<Long> candidates = new HashSet<>();
            result.candidatesByPartition.put(partitionId, candidates);

            Long snapshotVersion = visibleVersionMap.get(partitionId);
            if (!(partition instanceof CloudPartition) || snapshotVersion == null || snapshotVersion <= 0) {
                // Local mode, or the snapshot version of this partition is unknown: keep all tablets.
                candidates.addAll(allTabletIds);
                result.degradedTablets += allTabletIds.size();
                continue;
            }
            for (Long tabletId : allTabletIds) {
                Tablet tablet = selectedIndex.getTablet(tabletId);
                long beId;
                try {
                    if (tablet == null || tablet.getReplicas().isEmpty()) {
                        throw new UserException("no replica");
                    }
                    beId = tablet.getReplicas().get(0).getBackendId();
                } catch (UserException e) {
                    candidates.add(tabletId);
                    result.degradedTablets++;
                    continue;
                }
                tabletIdToPartitionId.put(tabletId, partitionId);
                beIdToTablets.computeIfAbsent(beId, k -> new ArrayList<>())
                        .add(InternalService.PGpPruneTablet.newBuilder()
                                .setTabletId(tabletId)
                                .setSnapshotVersion(snapshotVersion)
                                .build());
            }
        }

        Map<Long, Future<InternalService.PGlobalPointIndexPruneResponse>> futures = new HashMap<>();
        for (Map.Entry<Long, List<InternalService.PGpPruneTablet>> entry : beIdToTablets.entrySet()) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(entry.getKey());
            Future<InternalService.PGlobalPointIndexPruneResponse> future = null;
            if (backend != null && backend.isAlive()) {
                InternalService.PGlobalPointIndexPruneRequest request =
                        InternalService.PGlobalPointIndexPruneRequest.newBuilder()
                                .setTableId(table.getId())
                                .setColumnUniqueId(column.getUniqueId())
                                .addAllProbeValues(probeValues)
                                .addAllTablets(entry.getValue())
                                .build();
                future = BackendServiceProxy.getInstance()
                        .pruneGlobalPointIndexAsync(backend.getBrpcAddress(), request);
            }
            if (future == null) {
                keepAll(result, tabletIdToPartitionId, entry.getValue());
            } else {
                futures.put(entry.getKey(), future);
            }
        }

        // A small margin over the RPC deadline, so the RPC's own timeout fires first.
        long waitMs = Config.global_point_index_prune_timeout_ms + 50L;
        for (Map.Entry<Long, Future<InternalService.PGlobalPointIndexPruneResponse>> entry : futures.entrySet()) {
            List<InternalService.PGpPruneTablet> sent = beIdToTablets.get(entry.getKey());
            try {
                InternalService.PGlobalPointIndexPruneResponse response =
                        entry.getValue().get(waitMs, TimeUnit.MILLISECONDS);
                if (response == null || response.getStatus().getStatusCode() != 0) {
                    keepAll(result, tabletIdToPartitionId, sent);
                    continue;
                }
                for (Long tabletId : response.getCandidateTabletIdsList()) {
                    Long partitionId = tabletIdToPartitionId.get(tabletId);
                    if (partitionId != null) {
                        result.candidatesByPartition.get(partitionId).add(tabletId);
                    }
                }
                result.degradedTablets += response.getDegradedTablets();
            } catch (Exception e) {
                LOG.warn("global point index prune: RPC to BE {} failed or timed out", entry.getKey(), e);
                keepAll(result, tabletIdToPartitionId, sent);
            }
        }

        for (Set<Long> candidates : result.candidatesByPartition.values()) {
            result.tabletsAfter += candidates.size();
        }
        return result;
    }

    // Keeps every tablet of a BE whose answer is unusable.
    private static void keepAll(Result result, Map<Long, Long> tabletIdToPartitionId,
            List<InternalService.PGpPruneTablet> tablets) {
        for (InternalService.PGpPruneTablet tablet : tablets) {
            Long partitionId = tabletIdToPartitionId.get(tablet.getTabletId());
            if (partitionId != null && result.candidatesByPartition.get(partitionId).add(tablet.getTabletId())) {
                result.degradedTablets++;
            }
        }
    }
}
