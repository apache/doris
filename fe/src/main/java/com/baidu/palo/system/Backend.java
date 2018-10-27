// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.system;

import com.baidu.palo.alter.DecommissionBackendJob.DecommissionType;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.DiskInfo;
import com.baidu.palo.catalog.DiskInfo.DiskState;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.metric.MetricRepo;
import com.baidu.palo.system.BackendEvent.BackendEventType;
import com.baidu.palo.thrift.TDisk;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class extends the primary identifier of a Backend with ephemeral state,
 * eg usage information, current administrative state etc.
 */
public class Backend implements Writable {

    public enum BackendState {
        using, /* backend is belong to a cluster*/
        offline,
        free /* backend is not belong to any clusters */
    }

    private static final Logger LOG = LogManager.getLogger(Backend.class);

    private long id;
    private String host;

    private int heartbeatPort; // heartbeat
    private AtomicInteger bePort; // be
    private AtomicInteger httpPort; // web service
    private AtomicInteger beRpcPort; // be rpc port
    private AtomicInteger brpcPort = new AtomicInteger(-1);

    private AtomicLong lastUpdateMs;
    private AtomicLong lastStartTime;
    private AtomicBoolean isAlive;

    private AtomicBoolean isDecommissioned;
    private AtomicInteger decommissionType;
    private AtomicReference<String> ownerClusterName;
    // to index the state in some cluster
    private AtomicInteger backendState;
    // private BackendState backendState;

    // rootPath -> DiskInfo
    private AtomicReference<ImmutableMap<String, DiskInfo>> disksRef;

    private String heartbeatErrMsg = "";

    public Backend() {
        this.host = "";
        this.lastUpdateMs = new AtomicLong();
        this.lastStartTime = new AtomicLong();
        this.isAlive = new AtomicBoolean();
        this.isDecommissioned = new AtomicBoolean(false);

        this.bePort = new AtomicInteger();
        this.httpPort = new AtomicInteger();
        this.beRpcPort = new AtomicInteger();
        this.disksRef = new AtomicReference<ImmutableMap<String, DiskInfo>>(ImmutableMap.<String, DiskInfo> of());

        this.ownerClusterName = new AtomicReference<String>("");
        this.backendState = new AtomicInteger(BackendState.free.ordinal());
        
        this.decommissionType = new AtomicInteger(DecommissionType.SystemDecommission.ordinal());
    }

    public Backend(long id, String host, int heartbeatPort) {
        this.id = id;
        this.host = host;
        this.heartbeatPort = heartbeatPort;
        this.bePort = new AtomicInteger(-1);
        this.httpPort = new AtomicInteger(-1);
        this.beRpcPort = new AtomicInteger(-1);
        this.lastUpdateMs = new AtomicLong(-1L);
        this.lastStartTime = new AtomicLong(-1L);
        this.disksRef = new AtomicReference<ImmutableMap<String, DiskInfo>>(ImmutableMap.<String, DiskInfo> of());

        this.isAlive = new AtomicBoolean(false);
        this.isDecommissioned = new AtomicBoolean(false);

        this.ownerClusterName = new AtomicReference<String>(""); 
        this.backendState = new AtomicInteger(BackendState.free.ordinal());
        this.decommissionType = new AtomicInteger(DecommissionType.SystemDecommission.ordinal());
    }

    public long getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getBePort() {
        return bePort.get();
    }

    public int getHeartbeatPort() {
        return heartbeatPort;
    }

    public int getHttpPort() {
        return httpPort.get();
    }

    public int getBeRpcPort() {
        return beRpcPort.get();
    }

    public int getBrpcPort() {
        return brpcPort.get();
    }

    public String getHeartbeatErrMsg() {
        return heartbeatErrMsg;
    }

    // back compatible with unit test
    public void updateOnce(int bePort, int httpPort, int beRpcPort) {
        updateOnce(bePort, httpPort, beRpcPort, -1);
    }

    public void updateOnce(int bePort, int httpPort, int beRpcPort, int brpcPort) {
        boolean isChanged = false;
        if (this.bePort.get() != bePort) {
            isChanged = true;
            this.bePort.set(bePort);
        }

        if (this.httpPort.get() != httpPort) {
            isChanged = true;
            this.httpPort.set(httpPort);
        }

        if (this.beRpcPort.get() != beRpcPort) {
            isChanged = true;
            this.beRpcPort.set(beRpcPort);
        }

        if (this.brpcPort.get() != brpcPort) {
            isChanged = true;
            this.brpcPort.set(brpcPort);
        }

        long currentTime = System.currentTimeMillis();
        this.lastUpdateMs.set(currentTime);
        if (!isAlive.get()) {
            isChanged = true;
            this.lastStartTime.set(currentTime);
            LOG.info("{} is alive,", this.toString());
            this.isAlive.set(true);
        }

        if (isChanged) {
            Catalog.getInstance().getEditLog().logBackendStateChange(this);
        }

        heartbeatErrMsg = "";
    }

    public boolean setDecommissioned(boolean isDecommissioned) {
        if (this.isDecommissioned.compareAndSet(!isDecommissioned, isDecommissioned)) {
            LOG.warn("{} set decommission: {}", this.toString(), isDecommissioned);
            return true;
        }
        return false;
    }

    public void setBad(EventBus eventBus, String errMsg) {
        if (isAlive.compareAndSet(true, false)) {
            Catalog.getInstance().getEditLog().logBackendStateChange(this);
            LOG.warn("{} is dead", this.toString());
        }

        eventBus.post(new BackendEvent(BackendEventType.BACKEND_DOWN, "missing heartbeat", Long.valueOf(id)));
        heartbeatErrMsg = errMsg;
    }

    public void setBackendState(BackendState state) {
        this.backendState.set(state.ordinal());
    }

    public void setAlive(boolean isAlive) {
        this.isAlive.set(isAlive);
    }

    public void setBePort(int agentPort) {
        this.bePort.set(agentPort);
    }

    public void setHttpPort(int httpPort) {
        this.httpPort.set(httpPort);
    }

    public void setBeRpcPort(int beRpcPort) {
        this.beRpcPort.set(beRpcPort);
    }

    public void setBrpcPort(int brpcPort) {
        this.brpcPort.set(brpcPort);
    }

    public long getLastUpdateMs() {
        return this.lastUpdateMs.get();
    }

    public void setLastUpdateMs(long currentTime) {
        this.lastUpdateMs.set(currentTime);
    }

    public long getLastStartTime() {
        return this.lastStartTime.get();
    }

    public void setLastStartTime(long currentTime) {
        this.lastStartTime.set(currentTime);
    }

    public boolean isAlive() {
        return this.isAlive.get();
    }

    public boolean isDecommissioned() {
        return this.isDecommissioned.get();
    }

    public boolean isAvailable() {
        return this.isAlive.get() && !this.isDecommissioned.get();
    }

    public void setDisks(ImmutableMap<String, DiskInfo> disks) {
        this.disksRef.set(disks);
    }

    /**
     * backend belong to some cluster
     * 
     * @return
     */
    public boolean isUsedByCluster() {
        return this.backendState.get() == BackendState.using.ordinal();
    }

    /**
     * backend is free, and it isn't belong to any cluster
     * 
     * @return
     */
    public boolean isFreeFromCluster() {
        return this.backendState.get() == BackendState.free.ordinal();
    }

    /**
     * backend execute discommission in cluster , and backendState will be free
     * finally
     * 
     * @return
     */
    public boolean isOffLineFromCluster() {
        return this.backendState.get() == BackendState.offline.ordinal();
    }

    public ImmutableMap<String, DiskInfo> getDisks() {
        return this.disksRef.get();
    }

    public List<String> getDiskInfosAsString() {
        ImmutableMap<String, DiskInfo> disks = disksRef.get();
        List<String> diskInfoStrings = new LinkedList<String>();
        for (DiskInfo diskInfo : disks.values()) {
            diskInfoStrings.add(diskInfo.getRootPath() + "|" + diskInfo.getTotalCapacityB() + "|"
                    + diskInfo.getDataUsedCapacityB() + "|" + diskInfo.getAvailableCapacityB() + "|"
                    + diskInfo.getState().name());
        }
        return diskInfoStrings;
    }

    public long getTotalCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef.get();
        long totalCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                totalCapacityB += diskInfo.getTotalCapacityB();
            }
        }
        return totalCapacityB;
    }

    public long getAvailableCapacityB() {
        // when cluster init, disks is empty, return 1L.
        ImmutableMap<String, DiskInfo> disks = disksRef.get();
        long availableCapacityB = 1L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                availableCapacityB += diskInfo.getAvailableCapacityB();
            }
        }
        return availableCapacityB;
    }

    public long getDataUsedCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef.get();
        long dataUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                dataUsedCapacityB += diskInfo.getDataUsedCapacityB();
            }
        }
        return dataUsedCapacityB;
    }

    public void updateDisks(Map<String, TDisk> backendDisks) {
        // update status or add new diskInfo
        ImmutableMap<String, DiskInfo> disks = disksRef.get();
        Map<String, DiskInfo> newDisks = Maps.newHashMap();
        for (TDisk tDisk : backendDisks.values()) {
            String rootPath = tDisk.getRoot_path();
            long totalCapacityB = tDisk.getDisk_total_capacity();
            long dataUsedCapacityB = tDisk.getData_used_capacity();
            long diskAvailableCapacityB = tDisk.getDisk_available_capacity();
            boolean isUsed = tDisk.isUsed();

            DiskInfo diskInfo = disks.get(rootPath);
            if (diskInfo == null) {
                diskInfo = new DiskInfo(rootPath);
                LOG.info("add new disk info. backendId: {}, rootPath: {}", id, rootPath);
            }
            newDisks.put(rootPath, diskInfo);

            diskInfo.setTotalCapacityB(totalCapacityB);
            diskInfo.setDataUsedCapacityB(dataUsedCapacityB);
            diskInfo.setAvailableCapacityB(diskAvailableCapacityB);
            if (isUsed) {
                diskInfo.setState(DiskState.ONLINE);
            } else {
                diskInfo.setState(DiskState.OFFLINE);
            }
            LOG.debug("update disk info. backendId: {}, diskInfo: {}", id, diskInfo.toString());
        }

        // remove not exist rootPath in backend
        // no remove op. just log
        for (DiskInfo diskInfo : disks.values()) {
            String rootPath = diskInfo.getRootPath();
            if (!backendDisks.containsKey(rootPath)) {
                LOG.warn("remove not exist rootPath. backendId: {}, rootPath: {}", id, rootPath);
            }
        }

        // update disksRef
        disksRef.set(ImmutableMap.copyOf(newDisks));

        // log disk changing
        Catalog.getInstance().getEditLog().logBackendStateChange(this);

        // disks is changed, regenerated capacity metrics
        MetricRepo.generateCapacityMetrics();
    }

    public static Backend read(DataInput in) throws IOException {
        Backend backend = new Backend();
        backend.readFields(in);
        return backend;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        Text.writeString(out, host);
        out.writeInt(heartbeatPort);
        out.writeInt(bePort.get());
        out.writeInt(httpPort.get());
        out.writeInt(beRpcPort.get());
        out.writeBoolean(isAlive.get());
        out.writeBoolean(isDecommissioned.get());
        out.writeLong(lastUpdateMs.get());

        out.writeLong(lastStartTime.get());

        ImmutableMap<String, DiskInfo> disks = disksRef.get();
        out.writeInt(disks.size());
        for (Map.Entry<String, DiskInfo> entry : disks.entrySet()) {
            Text.writeString(out, entry.getKey());
            entry.getValue().write(out);
        }

        Text.writeString(out, ownerClusterName.get());
        out.writeInt(backendState.get());
        out.writeInt(decommissionType.get());

        out.writeInt(brpcPort.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        host = Text.readString(in);
        heartbeatPort = in.readInt();
        bePort.set(in.readInt());
        httpPort.set(in.readInt());
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_31) {
            beRpcPort.set(in.readInt());
        }
        isAlive.set(in.readBoolean());

        if (Catalog.getCurrentCatalogJournalVersion() >= 5) {
            isDecommissioned.set(in.readBoolean());
        }

        lastUpdateMs.set(in.readLong());

        if (Catalog.getCurrentCatalogJournalVersion() >= 2) {
            lastStartTime.set(in.readLong());

            Map<String, DiskInfo> disks = Maps.newHashMap();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String rootPath = Text.readString(in);
                DiskInfo diskInfo = DiskInfo.read(in);
                disks.put(rootPath, diskInfo);
            }

            disksRef.set(ImmutableMap.copyOf(disks));
        }
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            ownerClusterName.set(Text.readString(in));
            backendState.set(in.readInt());
            decommissionType.set(in.readInt());
        } else {
            ownerClusterName.set(SystemInfoService.DEFAULT_CLUSTER);
            backendState.set(BackendState.using.ordinal());
            decommissionType.set(DecommissionType.SystemDecommission.ordinal());
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_40) {
            brpcPort.set(in.readInt());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Backend)) {
            return false;
        }

        Backend backend = (Backend) obj;

        return (id == backend.id) && (host.equals(backend.host)) && (heartbeatPort == backend.heartbeatPort)
                && (bePort.get() == backend.bePort.get()) && (isAlive.get() == backend.isAlive.get());
    }

    @Override
    public String toString() {
        return "Backend [id=" + id + ", host=" + host + ", heartbeatPort=" + heartbeatPort + ", alive=" + isAlive.get()
                + "]";
    }

    public String getOwnerClusterName() {
        return ownerClusterName.get();
    }

    public void setOwnerClusterName(String name) {
        ownerClusterName.set(name);
    }
    
    public void clearClusterName() {
        ownerClusterName.set("");
    }

    public BackendState getBackendState() {
        switch (backendState.get()) {
            case 0:
                return BackendState.using;
            case 1:
                return BackendState.offline;
            case 2:
                return BackendState.free;
            default:
                return BackendState.free;
        }
    }

    public void setDecommissionType(DecommissionType type) {
        decommissionType.set(type.ordinal());
    }
    
    public DecommissionType getDecommissionType() {
        if (decommissionType.get() == DecommissionType.ClusterDecommission.ordinal()) {
            return DecommissionType.ClusterDecommission;
        }
        return DecommissionType.SystemDecommission;
    }

}

