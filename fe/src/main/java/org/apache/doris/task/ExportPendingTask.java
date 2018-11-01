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

package org.apache.doris.task;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.load.ExportFailMsg;
import org.apache.doris.load.ExportJob;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TSnapshotRequest;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ExportPendingTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(ExportPendingTask.class);

    protected final ExportJob job;
    protected Database db;

    public ExportPendingTask(ExportJob job) {
        super();
        this.job = job;
        this.signature = job.getId();
    }

    @Override
    protected void exec() {
        if (job.getState() != ExportJob.JobState.PENDING) {
            return;
        }

        long dbId = job.getDbId();
        db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            String failMsg = "db is null.";
            job.cancel(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            return;
        }

        // Check exec fragments should already generated
        if (job.isReplayed()) {
            String failMsg = "do not have exec request.";
            job.cancel(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            return;
        }

        // make snapshots
        Status snapshotStatus = makeSnapshots();
        if (!snapshotStatus.ok()) {
            String failMsg = "make snapshot failed.";
            failMsg += snapshotStatus.getErrorMsg();
            job.cancel(ExportFailMsg.CancelType.RUN_FAIL, failMsg);
            LOG.warn("make snapshot fail. job:{}", job);
            return;
        }

        if (job.updateState(ExportJob.JobState.EXPORTING)) {
            LOG.info("submit pending export job success. job: {}", job);
            return;
        }
    }
    
    private Status makeSnapshots() {
        List<TScanRangeLocations> tabletLocations = job.getTabletLocations();
        if (tabletLocations == null) {
            return Status.OK;
        }
        for (TScanRangeLocations tablet : tabletLocations) {
            TScanRange scanRange = tablet.getScan_range();
            if (!scanRange.isSetPalo_scan_range()) {
                continue;
            }
            TPaloScanRange paloScanRange = scanRange.getPalo_scan_range();
            List<TScanRangeLocation> locations = tablet.getLocations();
            for (TScanRangeLocation location : locations) {
                TNetworkAddress address = location.getServer();
                String host = address.getHostname();
                int port = address.getPort();
                Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(host, port);
                if (backend == null) {
                    return Status.CANCELLED;
                }
                long backendId = backend.getId();
                if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                    return Status.CANCELLED;
                }
                TSnapshotRequest snapshotRequest = new TSnapshotRequest();
                snapshotRequest.setTablet_id(paloScanRange.getTablet_id());
                snapshotRequest.setSchema_hash(Integer.parseInt(paloScanRange.getSchema_hash()));
                snapshotRequest.setVersion(Long.parseLong(paloScanRange.getVersion()));
                snapshotRequest.setVersion_hash(Long.parseLong(paloScanRange.getVersion_hash()));

                AgentClient client = new AgentClient(host, port);
                TAgentResult result = client.makeSnapshot(snapshotRequest);
                if (result == null || result.getStatus().getStatus_code() != TStatusCode.OK) {
                    return Status.CANCELLED;
                }
                job.addSnapshotPath(new Pair<TNetworkAddress, String>(address, result.getSnapshot_path()));
                LOG.debug("snapshot address:{}, path:{}", address, result.getSnapshot_path());
            }
        }
        return Status.OK;
    }
}
