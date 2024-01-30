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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Internal representation of partition-related metadata.
 */
public class CloudPartition extends Partition {
    // Every partition starts from version 1, version 1 has no data
    public static long EMPTY_VERSION = 1;

    private static final Logger LOG = LogManager.getLogger(CloudPartition.class);

    // not Serialized
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;

    public CloudPartition(long id, String name, MaterializedIndex baseIndex,
                          DistributionInfo distributionInfo, long dbId, long tableId) {
        super(id, name, baseIndex, distributionInfo);
        super.nextVersion = -1;
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public CloudPartition() {
        super();
    }

    public long getDbId() {
        return this.dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getTableId() {
        return this.tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    protected void setVisibleVersion(long visibleVersion) {
        LOG.debug("setVisibleVersion use CloudPartition {}", super.getName());
        return;
    }

    protected void setCachedVisibleVersion(long version) {
        super.setVisibleVersion(version);
    }

    @Override
    public long getVisibleVersion() {
        LOG.debug("getVisibleVersion use CloudPartition {}", super.getName());

        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder()
                .setDbId(this.dbId)
                .setTableId(this.tableId)
                .setPartitionId(super.getId())
                .setBatchMode(false)
                .build();

        try {
            Cloud.GetVersionResponse resp = getVersionFromMeta(request);
            long version = -1;
            if (resp.getStatus().getCode() == MetaServiceCode.OK) {
                version = resp.getVersion();
            } else {
                assert resp.getStatus().getCode() == MetaServiceCode.VERSION_NOT_FOUND;
                version = 0;
            }
            LOG.debug("get version from meta service, version: {}, partition: {}", version, super.getId());
            // Cache visible version, see hasData() for details.
            super.setVisibleVersion(version);
            if (version == 0 && isEmptyPartitionPruneDisabled()) {
                version = 1;
            }
            return version;
        } catch (RpcException e) {
            throw new RuntimeException("get version from meta service failed");
        }
    }

    // Select the non-empty partitions and return the ids.
    public static List<Long> selectNonEmptyPartitionIds(List<CloudPartition> partitions) {
        List<Long> nonEmptyPartitionIds = partitions.stream()
                .filter(CloudPartition::hasDataCached)
                .map(CloudPartition::getId)
                .collect(Collectors.toList());
        if (nonEmptyPartitionIds.size() == partitions.size()) {
            return nonEmptyPartitionIds;
        }

        List<CloudPartition> unknowns = partitions.stream()
                .filter(p -> !p.hasDataCached())
                .collect(Collectors.toList());

        SummaryProfile profile = getSummaryProfile();
        if (profile != null) {
            profile.incGetPartitionVersionByHasDataCount();
        }

        try {
            List<Long> versions = CloudPartition.getSnapshotVisibleVersion(unknowns);

            int size = versions.size();
            for (int i = 0; i < size; i++) {
                if (versions.get(i) > CloudPartition.EMPTY_VERSION) {
                    nonEmptyPartitionIds.add(unknowns.get(i).getId());
                }
            }

            return nonEmptyPartitionIds;
        } catch (RpcException e) {
            throw new RuntimeException("get version from meta service failed");
        }
    }

    // Get visible version from the specified partitions;
    //
    // Return the visible version in order of the specified partition ids, -1 means version NOT FOUND.
    public static List<Long> getSnapshotVisibleVersion(List<CloudPartition> partitions) throws RpcException {
        if (partitions.isEmpty()) {
            return new ArrayList<>();
        }

        List<Long> dbIds = new ArrayList<>();
        List<Long> tableIds = new ArrayList<>();
        List<Long> partitionIds = new ArrayList<>();
        for (CloudPartition partition : partitions) {
            dbIds.add(partition.getDbId());
            tableIds.add(partition.getTableId());
            partitionIds.add(partition.getId());
        }

        List<Long> versions = getSnapshotVisibleVersion(dbIds, tableIds, partitionIds);

        // Cache visible version, see hasData() for details.
        int size = versions.size();
        for (int i = 0; i < size; ++i) {
            Long version = versions.get(i);
            if (version > EMPTY_VERSION) {
                partitions.get(i).setCachedVisibleVersion(versions.get(i));
            }
        }

        return versions;
    }

    // Get visible versions for the specified partitions.
    //
    // Return the visible version in order of the specified partition ids, -1 means version NOT FOUND.
    public static List<Long> getSnapshotVisibleVersion(List<Long> dbIds, List<Long> tableIds, List<Long> partitionIds)
            throws RpcException {
        assert dbIds.size() == partitionIds.size() :
                "partition ids size: " + partitionIds.size() + " should equals to db ids size: " + dbIds.size();
        assert tableIds.size() == partitionIds.size() :
                "partition ids size: " + partitionIds.size() + " should equals to tablet ids size: " + tableIds.size();

        Cloud.GetVersionRequest req = Cloud.GetVersionRequest.newBuilder()
                .setDbId(-1)
                .setTableId(-1)
                .setPartitionId(-1)
                .setBatchMode(true)
                .addAllDbIds(dbIds)
                .addAllTableIds(tableIds)
                .addAllPartitionIds(partitionIds)
                .build();

        LOG.debug("getVisibleVersion use CloudPartition {}", partitionIds.toString());
        Cloud.GetVersionResponse resp = getVersionFromMeta(req);
        if (resp.getStatus().getCode() != MetaServiceCode.OK) {
            throw new RpcException("get visible version", "unexpected status " + resp.getStatus());
        }

        List<Long> versions = resp.getVersionsList();
        if (versions.size() != partitionIds.size()) {
            throw new RpcException("get visible version",
                    "wrong number of versions, required " + partitionIds.size() + ", but got " + versions.size());
        }

        LOG.debug("get version from meta service, partitions: {}, versions: {}", partitionIds, versions);

        if (isEmptyPartitionPruneDisabled()) {
            ArrayList<Long> news = new ArrayList<>();
            for (Long v : versions) {
                news.add(v == -1 ? 1 : v);
            }
            return news;
        }

        return versions;
    }

    @Override
    public long getNextVersion() {
        // use meta service visibleVersion
        LOG.debug("getNextVersion use CloudPartition {}", super.getName());
        return -1;
    }

    @Override
    public void setNextVersion(long nextVersion) {
        // use meta service visibleVersion
        LOG.debug("setNextVersion use CloudPartition {} Version {}", super.getName(), nextVersion);
        return;
    }

    @Override
    public void updateVersionForRestore(long visibleVersion) {
        LOG.debug("updateVersionForRestore use CloudPartition {} version for restore: visible: {}",
                super.getName(), visibleVersion);
        return;
    }

    @Override
    public void updateVisibleVersion(long visibleVersion) {
        // use meta service visibleVersion
        LOG.debug("updateVisibleVersion use CloudPartition {} version for restore: visible: {}",
                super.getName(), visibleVersion);

        return;
    }

    @Override
    public void updateVisibleVersionAndTime(long visibleVersion, long visibleVersionTime) {
    }

    // Determine whether data this partition has, according to the cached visible version.
    public boolean hasDataCached() {
        // In order to determine whether a partition is empty, a get_version RPC is issued to
        // the meta service. The pruning process will be very slow when there are lots of empty
        // partitions. This option disables the empty partition prune optimization to speed SQL
        // analysis/plan phase.
        if (isEmptyPartitionPruneDisabled()) {
            return true;
        }

        // Every partition starts from version 1, version 1 has no data.
        // So as long as version is greater than 1, it can be determined that there is data here.
        return super.getVisibleVersion() > EMPTY_VERSION;
    }

    /**
     * CloudPartition always has data
     */
    @Override
    public boolean hasData() {
        // To avoid sending an RPC request, see the cached visible version here first.
        if (hasDataCached()) {
            return true;
        }

        SummaryProfile profile = getSummaryProfile();
        if (profile != null) {
            profile.incGetPartitionVersionByHasDataCount();
        }

        return getVisibleVersion() > EMPTY_VERSION;
    }

    private static Cloud.GetVersionResponse getVersionFromMeta(Cloud.GetVersionRequest req)
            throws RpcException {
        long startAt = System.nanoTime();
        try {
            return getVersionFromMetaInner(req);
        } finally {
            SummaryProfile profile = getSummaryProfile();
            if (profile != null) {
                profile.addGetPartitionVersionTime(System.nanoTime() - startAt);
            }
        }
    }

    private static Cloud.GetVersionResponse getVersionFromMetaInner(Cloud.GetVersionRequest req)
            throws RpcException {
        for (int retryTime = 0; retryTime < Config.cloud_meta_service_rpc_failed_retry_times; retryTime++) {
            try {
                long deadline = System.currentTimeMillis() + Config.default_get_version_from_ms_timeout_second * 1000L;
                Future<Cloud.GetVersionResponse> future =
                        MetaServiceProxy.getInstance().getVisibleVersionAsync(req);

                Cloud.GetVersionResponse resp = null;
                while (resp == null) {
                    try {
                        resp = future.get(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOG.warn("get version from meta service: future get interrupted exception");
                    }
                }

                if (resp.hasStatus() && (resp.getStatus().getCode() == MetaServiceCode.OK
                            || resp.getStatus().getCode() == MetaServiceCode.VERSION_NOT_FOUND)) {
                    LOG.debug("get version from meta service, code: {}", resp.getStatus().getCode());
                    return resp;
                }

                LOG.warn("get version from meta service failed, status: {}, retry time: {}",
                        resp.getStatus(), retryTime);
            } catch (RpcException | ExecutionException | TimeoutException | RuntimeException e) {
                LOG.warn("get version from meta service failed, retry times: {} exception: ", retryTime, e);
            }

            // sleep random millis [20, 200] ms, retry rpc failed
            int randomMillis = 20 + (int) (Math.random() * (200 - 20));
            if (retryTime > Config.cloud_meta_service_rpc_failed_retry_times / 2) {
                // sleep random millis [500, 1000] ms, retry rpc failed
                randomMillis = 500 + (int) (Math.random() * (1000 - 500));
            }
            try {
                Thread.sleep(randomMillis);
            } catch (InterruptedException e) {
                LOG.warn("get version from meta service: sleep get interrupted exception");
            }
        }

        LOG.warn("get version from meta service failed after retry {} times",
                Config.cloud_meta_service_rpc_failed_retry_times);
        throw new RpcException("get version from meta service", "failed after retry n times");
    }

    private static boolean isEmptyPartitionPruneDisabled() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().getDisableEmptyPartitionPrune()) {
            return true;
        }
        return false;
    }

    private static SummaryProfile getSummaryProfile() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            StmtExecutor executor = ctx.getExecutor();
            if (executor != null) {
                return executor.getSummaryProfile();
            }
        }
        return null;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.dbId = in.readLong();
        this.tableId = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(this.dbId);
        out.writeLong(this.tableId);
    }

    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        if (!(obj instanceof CloudPartition)) {
            return false;
        }
        CloudPartition cloudPartition = (CloudPartition) obj;
        return (dbId == cloudPartition.dbId) && (tableId == cloudPartition.tableId);
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(super.toString());
        buffer.append("dbId: ").append(this.dbId).append("; ");
        buffer.append("tableId: ").append(this.tableId).append("; ");
        return buffer.toString();
    }
}
