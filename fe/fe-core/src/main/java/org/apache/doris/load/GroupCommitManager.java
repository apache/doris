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

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.proto.InternalService.PGetWalQueueSizeRequest;
import org.apache.doris.proto.InternalService.PGetWalQueueSizeResponse;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

public class GroupCommitManager {

    private static final Logger LOG = LogManager.getLogger(GroupCommitManager.class);

    private Set<Long> blockedTableIds = new HashSet<>();

    public boolean isBlock(long tableId) {
        return blockedTableIds.contains(tableId);
    }

    public void blockTable(long tableId) {
        LOG.info("block group commit for table={} when schema change", tableId);
        blockedTableIds.add(tableId);
    }

    public void unblockTable(long tableId) {
        blockedTableIds.remove(tableId);
        LOG.info("unblock group commit for table={} when schema change", tableId);
    }

    /**
     * Waiting All WAL files to be deleted.
     */
    public void waitWalFinished(long tableId) {
        List<Long> aliveBeIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
        long expireTime = System.currentTimeMillis() + Config.check_wal_queue_timeout_threshold;
        while (true) {
            LOG.info("wait for wal queue size to be empty");
            boolean walFinished = Env.getCurrentEnv().getGroupCommitManager()
                    .isPreviousWalFinished(tableId, aliveBeIds);
            if (walFinished) {
                LOG.info("all wal is finished for table={}", tableId);
                break;
            } else if (System.currentTimeMillis() > expireTime) {
                LOG.warn("waitWalFinished time out for table={}", tableId);
                break;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.warn("failed to wait for wal for table={} when schema change", tableId, ie);
                }
            }
        }
    }

    /**
     * Check the wal before the endTransactionId is finished or not.
     */
    public boolean isPreviousWalFinished(long tableId, List<Long> aliveBeIds) {
        boolean empty = true;
        for (int i = 0; i < aliveBeIds.size(); i++) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(aliveBeIds.get(i));
            // in ut port is -1, skip checking
            if (backend.getBrpcPort() < 0) {
                return true;
            }
            PGetWalQueueSizeRequest request = PGetWalQueueSizeRequest.newBuilder()
                    .setTableId(tableId)
                    .build();
            long size = getWalQueueSize(backend, request);
            if (size > 0) {
                LOG.info("backend id:" + backend.getId() + ",wal size:" + size);
                empty = false;
            }
        }
        return empty;
    }

    public long getAllWalQueueSize(Backend backend) {
        PGetWalQueueSizeRequest request = PGetWalQueueSizeRequest.newBuilder()
                .setTableId(-1)
                .build();
        long size = getWalQueueSize(backend, request);
        if (size > 0) {
            LOG.info("backend id:" + backend.getId() + ",all wal size:" + size);
        }
        return size;
    }

    public long getWalQueueSize(Backend backend, PGetWalQueueSizeRequest request) {
        PGetWalQueueSizeResponse response = null;
        long expireTime = System.currentTimeMillis() + Config.check_wal_queue_timeout_threshold;
        long size = 0;
        while (System.currentTimeMillis() <= expireTime) {
            if (!backend.isAlive()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.info("group commit manager sleep wait InterruptedException: ", ie);
                }
                continue;
            }
            try {
                Future<PGetWalQueueSizeResponse> future = BackendServiceProxy.getInstance()
                        .getWalQueueSize(new TNetworkAddress(backend.getHost(), backend.getBrpcPort()), request);
                response = future.get();
            } catch (Exception e) {
                LOG.warn("encounter exception while getting wal queue size on backend id: " + backend.getId()
                        + ",exception:" + e);
                String msg = e.getMessage();
                if (msg.contains("Method") && msg.contains("unimplemented")) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.info("group commit manager sleep wait InterruptedException: ", ie);
                }
                continue;
            }
            TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                String msg = "get all queue size fail,backend id: " + backend.getId() + ", status: "
                        + response.getStatus();
                LOG.warn(msg);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.info("group commit manager sleep wait InterruptedException: ", ie);
                }
                continue;
            }
            size = response.getSize();
            break;
        }
        return size;
    }

}
