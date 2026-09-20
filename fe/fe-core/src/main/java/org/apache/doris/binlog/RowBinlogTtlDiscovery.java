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

package org.apache.doris.binlog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Bounded catalog discovery for tablets absent from a BE's metadata cache. */
public final class RowBinlogTtlDiscovery {
    private static final Logger LOG = LogManager.getLogger(RowBinlogTtlDiscovery.class);
    private Iterator<Database> databases = Collections.emptyIterator();
    private Iterator<Table> tables = Collections.emptyIterator();
    private Iterator<Partition> partitions = Collections.emptyIterator();
    private Iterator<Tablet> tablets = Collections.emptyIterator();
    private Iterator<MaterializedIndex> indexes = Collections.emptyIterator();
    private OlapTable currentTable;

    public void discover() {
        if (!Config.isCloudMode() || !Config.enable_feature_binlog) {
            return;
        }
        try {
            CloudSystemInfoService info = (CloudSystemInfoService) Env.getCurrentSystemInfo();
            Map<Long, List<Long>> targets = new HashMap<>();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
            if (!databases.hasNext() && !tables.hasNext() && !partitions.hasNext()
                    && !indexes.hasNext() && !tablets.hasNext()) {
                databases = Env.getCurrentInternalCatalog().getDbs().iterator();
            }
            for (int n = 0; n < 64 && System.nanoTime() < deadline; n++) {
                Tablet tablet = nextTablet(deadline);
                if (tablet == null) {
                    break;
                }
                CloudReplica replica = (CloudReplica) tablet.getReplicas().get(0);
                // One existing replica owner per compute group. BE read/write separation
                // decides which group may compact; Meta Service arbitrates competing jobs.
                for (String clusterId : info.getCloudClusterIds()) {
                    long backendId = replica.getBackendIdWithClusterId(clusterId);
                    Backend backend = info.getBackend(backendId);
                    if (backend != null && backend.isAlive()) {
                        targets.computeIfAbsent(backendId, ignored -> new ArrayList<>()).add(tablet.getId());
                    }
                }
            }
            for (Map.Entry<Long, List<Long>> target : targets.entrySet()) {
                Backend backend = info.getBackend(target.getKey());
                if (backend == null || !backend.isAlive()) {
                    continue;
                }
                InternalService.PSyncTabletMetaRequest request = InternalService.PSyncTabletMetaRequest.newBuilder()
                        .addAllTabletIds(target.getValue()).setDiscoverRowBinlogTtl(true).build();
                Futures.addCallback(BackendServiceProxy.getInstance().syncTabletMeta(backend.getBrpcAddress(), request),
                        new FutureCallback<InternalService.PSyncTabletMetaResponse>() {
                            @Override
                            public void onSuccess(InternalService.PSyncTabletMetaResponse response) {
                                if (!response.hasStatus() || response.getStatus().getStatusCode()
                                        != TStatusCode.OK.getValue() || response.getFailedTablets() > 0) {
                                    LOG.warn("ROW binlog TTL discovery deferred, backend={}, response={}",
                                            target.getKey(), response);
                                }
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                LOG.warn("ROW binlog TTL discovery failed, backend={}", target.getKey(), t);
                            }
                        }, MoreExecutors.directExecutor());
            }
        } catch (UserException | RpcException | RuntimeException e) {
            LOG.warn("ROW binlog TTL discovery deferred until next catalog sweep", e);
        }
    }

    private Tablet nextTablet(long deadline) {
        while (System.nanoTime() < deadline) {
            if (tablets.hasNext()) {
                return tablets.next();
            }
            if (indexes.hasNext()) {
                MaterializedIndex index = indexes.next();
                if (index.isRowBinlog()) {
                    // getTablets() is an immutable snapshot; no per-tablet copy or long table lock.
                    tablets = index.getTablets().iterator();
                }
            } else if (partitions.hasNext()) {
                Partition partition = partitions.next();
                currentTable.readLock();
                try {
                    indexes = partition.getMaterializedIndices(
                            MaterializedIndex.IndexExtState.VISIBLE, true).iterator();
                } finally {
                    currentTable.readUnlock();
                }
            } else if (tables.hasNext()) {
                Table table = tables.next();
                if (table instanceof OlapTable && ((OlapTable) table).hasRowBinlogTtl()) {
                    currentTable = (OlapTable) table;
                    currentTable.readLock();
                    try {
                        partitions = new ArrayList<>(currentTable.getPartitions()).iterator();
                    } finally {
                        currentTable.readUnlock();
                    }
                }
            } else if (databases.hasNext()) {
                tables = databases.next().getTables().iterator();
            } else {
                currentTable = null;
                return null;
            }
        }
        return null;
    }
}
