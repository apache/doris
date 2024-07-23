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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.common.Config;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Internal representation of partition-related metadata.
 */
public class CloudPartition extends Partition {
    private static final Logger LOG = LogManager.getLogger(CloudPartition.class);

    // not Serialized
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableId")
    private long tableId;

    private ReentrantLock lock = new ReentrantLock(true);

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("setVisibleVersion use CloudPartition {}", super.getName());
        }
        return;
    }

    public void setCachedVisibleVersion(long version, Long versionUpdateTimeMs) {
        // we only care the version should increase monotonically and ignore the readers
        LOG.debug("setCachedVisibleVersion use CloudPartition {}, version: {}, old version: {}",
                super.getId(), version, super.getVisibleVersion());
        lock.lock();
        if (version > super.getVisibleVersion()) {
            super.setVisibleVersionAndTime(version, versionUpdateTimeMs);
        }
        lock.unlock();
    }

    @Override
    public long getVisibleVersion(Boolean fromCache) {
        return super.getVisibleVersion();
    }

    @Override
    public long getVisibleVersion() {
        if (Env.isCheckpointThread() || Config.enable_check_compatibility_mode) {
            return super.getVisibleVersion();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("getVisibleVersion use CloudPartition {}", super.getName());
        }

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
                // Cache visible version, see hasData() for details.
                long mTime = resp.getVersionUpdateTimeMsList().size() == 1 ? resp.getVersionUpdateTimeMs(0) : 0;
                setCachedVisibleVersion(version, mTime);
            } else {
                assert resp.getStatus().getCode() == MetaServiceCode.VERSION_NOT_FOUND;
                version = Partition.PARTITION_INIT_VERSION;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get version from meta service, version: {}, partition: {}", version, super.getId());
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
                if (versions.get(i) > Partition.PARTITION_INIT_VERSION) {
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
        List<Long> versionUpdateTimesMs = new ArrayList<>();
        for (CloudPartition partition : partitions) {
            dbIds.add(partition.getDbId());
            tableIds.add(partition.getTableId());
            partitionIds.add(partition.getId());
        }

        List<Long> versions = getSnapshotVisibleVersion(dbIds, tableIds, partitionIds, versionUpdateTimesMs);

        // Cache visible version, see hasData() for details.
        int size = versions.size();
        for (int i = 0; i < size; ++i) {
            Long version = versions.get(i);
            if (version > Partition.PARTITION_INIT_VERSION) {
                // For compatibility, the existing partitions may not have mtime
                long mTime = versions.size() == versionUpdateTimesMs.size() ? versionUpdateTimesMs.get(i) : 0;
                partitions.get(i).setCachedVisibleVersion(versions.get(i), mTime);
            }
        }

        return versions;
    }

    // Get visible versions for the specified partitions.
    //
    // Return the visible version in order of the specified partition ids, -1 means version NOT FOUND.
    public static List<Long> getSnapshotVisibleVersion(List<Long> dbIds, List<Long> tableIds, List<Long> partitionIds,
            List<Long> versionUpdateTimesMs)
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("getVisibleVersion use CloudPartition {}", partitionIds.toString());
        }
        Cloud.GetVersionResponse resp = getVersionFromMeta(req);
        if (resp.getStatus().getCode() != MetaServiceCode.OK) {
            throw new RpcException("get visible version", "unexpected status " + resp.getStatus());
        }

        List<Long> versions = resp.getVersionsList();
        if (versions.size() != partitionIds.size()) {
            throw new RpcException("get visible version",
                    "wrong number of versions, required " + partitionIds.size() + ", but got " + versions.size());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("get version from meta service, partitions: {}, versions: {}", partitionIds, versions);
        }

        if (isEmptyPartitionPruneDisabled()) {
            ArrayList<Long> news = new ArrayList<>();
            for (Long v : versions) {
                news.add(v == -1 ? 1 : v);
            }
            return news;
        }

        if (versionUpdateTimesMs != null) {
            versionUpdateTimesMs.addAll(resp.getVersionUpdateTimeMsList());
        }

        return versions;
    }

    @Override
    public long getNextVersion() {
        // use meta service visibleVersion
        if (LOG.isDebugEnabled()) {
            LOG.debug("getNextVersion use CloudPartition {}", super.getName());
        }
        return -1;
    }

    @Override
    public void setNextVersion(long nextVersion) {
        // use meta service visibleVersion
        if (LOG.isDebugEnabled()) {
            LOG.debug("setNextVersion use CloudPartition {} Version {}", super.getName(), nextVersion);
        }
        return;
    }

    @Override
    public void updateVersionForRestore(long visibleVersion) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("updateVersionForRestore use CloudPartition {} version for restore: visible: {}",
                    super.getName(), visibleVersion);
        }
        return;
    }

    @Override
    public void updateVisibleVersion(long visibleVersion) {
        // use meta service visibleVersion
        if (LOG.isDebugEnabled()) {
            LOG.debug("updateVisibleVersion use CloudPartition {} version for restore: visible: {}",
                    super.getName(), visibleVersion);
        }

        return;
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
        return super.getVisibleVersion() > Partition.PARTITION_INIT_VERSION;
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

        return getVisibleVersion() > Partition.PARTITION_INIT_VERSION;
    }

    private static Cloud.GetVersionResponse getVersionFromMeta(Cloud.GetVersionRequest req)
            throws RpcException {
        long startAt = System.nanoTime();
        try {
            return VersionHelper.getVisibleVersion(req);
        } finally {
            SummaryProfile profile = getSummaryProfile();
            if (profile != null) {
                profile.addGetPartitionVersionTime(System.nanoTime() - startAt);
            }
        }
    }

    private static boolean isEmptyPartitionPruneDisabled() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && (ctx.getSessionVariable().getDisableNereidsRules().get(RuleType.valueOf(
                "PRUNE_EMPTY_PARTITION").type()) || ctx.getSessionVariable().getDisableEmptyPartitionPrune())) {
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

    @Deprecated
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.dbId = in.readLong();
        this.tableId = in.readLong();
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
