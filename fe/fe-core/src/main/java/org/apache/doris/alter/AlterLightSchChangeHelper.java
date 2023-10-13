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
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Pair;
import org.apache.doris.persist.AlterLightSchemaChangeInfo;
import org.apache.doris.proto.InternalService.PFetchColIdsRequest;
import org.apache.doris.proto.InternalService.PFetchColIdsRequest.Builder;
import org.apache.doris.proto.InternalService.PFetchColIdsRequest.PFetchColIdParam;
import org.apache.doris.proto.InternalService.PFetchColIdsResponse;
import org.apache.doris.proto.InternalService.PFetchColIdsResponse.PFetchColIdsResultEntry;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * For alter light_schema_change table property
 */
public class AlterLightSchChangeHelper {

    private static final Logger LOG = LogManager.getLogger(AlterLightSchChangeHelper.class);

    private  static final long DEFAULT_RPC_TIMEOUT = 900;

    private final Database db;

    private final OlapTable olapTable;

    private final long rpcTimoutMs;

    public AlterLightSchChangeHelper(Database db, OlapTable olapTable) {
        this.db = db;
        this.olapTable = olapTable;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            rpcTimoutMs = DEFAULT_RPC_TIMEOUT;
        } else {
            rpcTimoutMs = connectContext.getExecTimeout();
        }
    }

    /**
     * 1. rpc read columnUniqueIds from BE
     * 2. refresh table metadata
     * 3. write edit log
     */
    public void enableLightSchemaChange() throws IllegalStateException {
        final AlterLightSchemaChangeInfo info = callForColumnsInfo();
        updateTableMeta(info);
        Env.getCurrentEnv().getEditLog().logAlterLightSchemaChange(info);
        LOG.info("successfully enable `light_schema_change`, db={}, tbl={}", db.getFullName(), olapTable.getName());
    }

    /**
     * This method creates RPC params for several BEs which target tablets lie on.
     * Each param contains several target indexIds and each indexId is mapped to all the tablet ids on it.
     * We should pass all tablet id for a consistency check of index schema.
     *
     * @return beId -> set(indexId -> tabletIds)
     */
    private Map<Long, PFetchColIdsRequest> initParams() {
        // params: indexId -> tabletIds
        Map<Long, Map<Long, Set<Long>>> beIdToRequestInfo = new HashMap<>();
        final Collection<Partition> partitions = olapTable.getAllPartitions();
        for (Partition partition : partitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    buildParams(index.getId(), tablet, beIdToRequestInfo);
                }
            }
        }
        // transfer to rpc params
        final Map<Long, PFetchColIdsRequest> beIdToRequest = new HashMap<>();
        beIdToRequestInfo.keySet().forEach(beId -> {
            final Map<Long, Set<Long>> indexIdToTabletIds = beIdToRequestInfo.get(beId);
            final Builder requestBuilder = PFetchColIdsRequest.newBuilder();
            for (Long indexId : indexIdToTabletIds.keySet()) {
                final PFetchColIdParam.Builder paramBuilder = PFetchColIdParam.newBuilder();
                final PFetchColIdParam param = paramBuilder
                        .setIndexId(indexId)
                        .addAllTabletIds(indexIdToTabletIds.get(indexId))
                        .build();
                requestBuilder.addParams(param);
            }
            beIdToRequest.put(beId, requestBuilder.build());
        });
        return beIdToRequest;
    }

    private void buildParams(Long indexId, Tablet tablet, Map<Long, Map<Long, Set<Long>>> beIdToRequestInfo) {
        final List<Long> backendIds = tablet.getNormalReplicaBackendIds();
        for (Long backendId : backendIds) {
            beIdToRequestInfo.putIfAbsent(backendId, new HashMap<>());
            final Map<Long, Set<Long>> indexIdToTabletId = beIdToRequestInfo.get(backendId);
            indexIdToTabletId.putIfAbsent(indexId, new HashSet<>());
            indexIdToTabletId.computeIfPresent(indexId, (idxId, set) -> {
                set.add(tablet.getId());
                return set;
            });
        }
    }

    /**
     * @return contains indexIds to each tablet schema info which consists of columnName to corresponding
     * column unique id pairs
     * @throws IllegalStateException as a wrapper for rpc failures
     */
    public AlterLightSchemaChangeInfo callForColumnsInfo()
            throws IllegalStateException {
        Map<Long, PFetchColIdsRequest> beIdToRequest = initParams();
        Map<Long, Future<PFetchColIdsResponse>> beIdToRespFuture = new HashMap<>();
        try {
            for (Long beId : beIdToRequest.keySet()) {
                final Backend backend = Env.getCurrentSystemInfo().getIdToBackend().get(beId);
                final TNetworkAddress address =
                        new TNetworkAddress(Objects.requireNonNull(backend).getHost(), backend.getBrpcPort());
                final Future<PFetchColIdsResponse> responseFuture = BackendServiceProxy.getInstance()
                        .getColumnIdsByTabletIds(address, beIdToRequest.get(beId));
                beIdToRespFuture.put(beId, responseFuture);
            }
        } catch (RpcException e) {
            throw new IllegalStateException("fetch columnIds RPC failed", e);
        }
        // wait for and get results
        final long start = System.currentTimeMillis();
        long timeoutMs = rpcTimoutMs;
        final List<PFetchColIdsResponse> resultList = new ArrayList<>();
        try {
            for (Map.Entry<Long, Future<PFetchColIdsResponse>> entry : beIdToRespFuture.entrySet()) {
                final PFetchColIdsResponse response = entry.getValue().get(timeoutMs, TimeUnit.MILLISECONDS);
                if (response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
                    throw new IllegalStateException(String.format("fail to get column info from be: %s, msg:%s",
                            entry.getKey(), response.getStatus().getErrorMsgs(0)));
                }
                resultList.add(response);
                // refresh the timeout
                final long now = System.currentTimeMillis();
                final long deltaMs = now - start;
                timeoutMs -= deltaMs;
                Preconditions.checkState(timeoutMs >= 0,
                        "impossible state, timeout should happened");
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("fetch columnIds RPC result failed: ", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("fetch columnIds RPC result timeout", e);
        }
        return compactToAlterLscInfo(resultList);
    }

    /**
     * Since the result collected from several BEs may contain repeated indexes in distributed storage scenarios,
     * we should do consistency check for the result for the same index, and get the unique result.
     */
    private AlterLightSchemaChangeInfo compactToAlterLscInfo(List<PFetchColIdsResponse> resultList) {
        final PFetchColIdsResponse.Builder builder = PFetchColIdsResponse.newBuilder();
        Map<Long, Map<String, Integer>> indexIdToTabletInfo = new HashMap<>();
        resultList.forEach(response -> {
            for (PFetchColIdsResultEntry entry : response.getEntriesList()) {
                final long indexId = entry.getIndexId();
                if (!indexIdToTabletInfo.containsKey(indexId)) {
                    indexIdToTabletInfo.put(indexId, entry.getColNameToIdMap());
                    builder.addEntries(entry);
                    continue;
                }
                // check tablet schema info consistency
                final Map<String, Integer> colNameToId = indexIdToTabletInfo.get(indexId);
                Preconditions.checkState(colNameToId.equals(entry.getColNameToIdMap()),
                        "index: " + indexId + "got inconsistent schema in storage");
            }
        });
        return new AlterLightSchemaChangeInfo(db.getId(), olapTable.getId(), indexIdToTabletInfo);
    }

    public void updateTableMeta(AlterLightSchemaChangeInfo info) throws IllegalStateException {
        Preconditions.checkNotNull(info, "passed in info should be not null");
        // update index-meta once and for all
        // schema pair: <maxColId, columns>
        final List<Pair<Integer, List<Column>>> schemaPairs = new ArrayList<>();
        final List<Long> indexIds = new ArrayList<>();
        info.getIndexIdToColumnInfo().forEach((indexId, colNameToId) -> {
            final List<Column> columns = olapTable.getSchemaByIndexId(indexId, true);
            Preconditions.checkState(columns.size() == colNameToId.size(),
                    "size mismatch for original columns meta and that in change info");
            int maxColId = Column.COLUMN_UNIQUE_ID_INIT_VALUE;
            final List<Column> newSchema = new ArrayList<>();
            for (Column column : columns) {
                final String columnName = column.getName();
                final int columnId = Preconditions.checkNotNull(colNameToId.get(columnName),
                        "failed to fetch column id of column:{" + columnName + "}");
                final Column newColumn = new Column(column);
                newColumn.setUniqueId(columnId);
                newSchema.add(newColumn);
                maxColId = Math.max(columnId, maxColId);
            }
            schemaPairs.add(Pair.of(maxColId, newSchema));
            indexIds.add(indexId);
        });
        Preconditions.checkState(schemaPairs.size() == indexIds.size(),
                "impossible state, size of schemaPairs and indexIds should be the same");
        // update index-meta once and for all
        try {
            for (int i = 0; i < indexIds.size(); i++) {
                final MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexIds.get(i));
                final Pair<Integer, List<Column>> schemaPair = schemaPairs.get(i);
                indexMeta.setMaxColUniqueId(schemaPair.first);
                indexMeta.setSchema(schemaPair.second);
            }
        } catch (IOException e) {
            throw new IllegalStateException("fail to reset index schema", e);
        }
        // write table property
        olapTable.setEnableLightSchemaChange(true);
    }
}
