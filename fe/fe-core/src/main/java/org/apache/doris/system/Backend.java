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

package org.apache.doris.system;

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.HeartbeatResponse.HbStatus;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class extends the primary identifier of a Backend with ephemeral state,
 * eg usage information, current administrative state etc.
 */
public class Backend implements Writable {
    private static final Logger LOG = LogManager.getLogger(Backend.class);

    // Represent a meaningless IP
    public static final String DUMMY_IP = "0.0.0.0";

    @SerializedName("id")
    private long id;
    @SerializedName("host")
    private volatile String host;
    private String version;

    @SerializedName("heartbeatPort")
    private int heartbeatPort; // heartbeat
    @SerializedName("bePort")
    private volatile int bePort; // be
    @SerializedName("httpPort")
    private volatile int httpPort; // web service
    @SerializedName("beRpcPort")
    private volatile int beRpcPort; // be rpc port
    @SerializedName("brpcPort")
    private volatile int brpcPort = -1;
    @SerializedName("arrowFlightSqlPort")
    private volatile int arrowFlightSqlPort = -1;

    @SerializedName("lastUpdateMs")
    private volatile long lastUpdateMs;
    @SerializedName("lastStartTime")
    private volatile long lastStartTime;
    @SerializedName("isAlive")
    private AtomicBoolean isAlive;

    @SerializedName("isDecommissioned")
    private AtomicBoolean isDecommissioned;

    // rootPath -> DiskInfo
    @SerializedName("disksRef")
    private volatile ImmutableMap<String, DiskInfo> disksRef;

    private String heartbeatErrMsg = "";

    // This is used for the first time we init pathHashToDishInfo in SystemInfoService.
    // after init it, this variable is set to true.
    private boolean initPathInfo = false;

    private long lastMissingHeartbeatTime = -1;
    // the max tablet compaction score of this backend.
    // this field is set by tablet report, and just for metric monitor, no need to persist.
    private volatile long tabletMaxCompactionScore = 0;

    // additional backendStatus information for BE, display in JSON format
    @SerializedName("backendStatus")
    private BackendStatus backendStatus = new BackendStatus();
    // the locationTag is also saved in tagMap, use a single field here to avoid
    // creating this everytime we get it.
    @SerializedName(value = "locationTag", alternate = {"tag"})
    private Tag locationTag = Tag.DEFAULT_BACKEND_TAG;

    @SerializedName("nodeRole")
    private Tag nodeRoleTag = Tag.DEFAULT_NODE_ROLE_TAG;

    // tag type -> tag value.
    // A backend can only be assigned to one tag type, and each type can only have one value.
    @SerializedName("tagMap")
    private Map<String, String> tagMap = Maps.newHashMap();

    // cpu cores
    @SerializedName("cpuCores")
    private int cpuCores = 1;

    // from config::pipeline_executor_size , default equal cpuCores
    @SerializedName("pipelineExecutorSize")
    private int pipelineExecutorSize = 1;

    // Counter of heartbeat failure.
    // Once a heartbeat failed, increase this counter by one.
    // And if it reaches Config.max_backend_heartbeat_failure_tolerance_count, this backend
    // will be marked as dead.
    // And once it back to alive, reset this counter.
    // No need to persist, because only master FE handle heartbeat.
    private int heartbeatFailureCounter = 0;

    // Not need serialize this field. If fe restart the state is reset to false. Maybe fe will
    // send some queries to this BE, it is not an important problem.
    private AtomicBoolean isShutDown = new AtomicBoolean(false);

    public Backend() {
        this.host = "";
        this.version = "";
        this.lastUpdateMs = 0;
        this.lastStartTime = 0;
        this.isAlive = new AtomicBoolean();
        this.isDecommissioned = new AtomicBoolean(false);

        this.bePort = 0;
        this.httpPort = 0;
        this.beRpcPort = 0;
        this.disksRef = ImmutableMap.of();

        this.tagMap.put(locationTag.type, locationTag.value);
    }

    public Backend(long id, String host, int heartbeatPort) {
        this.id = id;
        this.host = host;
        this.version = "";
        this.heartbeatPort = heartbeatPort;
        this.bePort = -1;
        this.httpPort = -1;
        this.beRpcPort = -1;
        this.lastUpdateMs = -1L;
        this.lastStartTime = -1L;
        this.disksRef = ImmutableMap.of();

        this.isAlive = new AtomicBoolean(false);
        this.isDecommissioned = new AtomicBoolean(false);

        this.tagMap.put(locationTag.type, locationTag.value);
    }

    public long getId() {
        return id;
    }

    // Return ip:heartbeat port
    public String getAddress() {
        return host + ":" + heartbeatPort;
    }

    public String getHost() {
        return host;
    }

    public String getVersion() {
        return version;
    }

    public int getBePort() {
        return bePort;
    }

    public int getHeartbeatPort() {
        return heartbeatPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getBeRpcPort() {
        return beRpcPort;
    }

    public int getBrpcPort() {
        return brpcPort;
    }

    public int getArrowFlightSqlPort() {
        return arrowFlightSqlPort;
    }

    public String getHeartbeatErrMsg() {
        return heartbeatErrMsg;
    }

    public long getLastStreamLoadTime() {
        return this.backendStatus.lastStreamLoadTime;
    }

    public void setLastStreamLoadTime(long lastStreamLoadTime) {
        this.backendStatus.lastStreamLoadTime = lastStreamLoadTime;
    }

    public boolean isQueryDisabled() {
        return backendStatus.isQueryDisabled;
    }

    public void setQueryDisabled(boolean isQueryDisabled) {
        this.backendStatus.isQueryDisabled = isQueryDisabled;
    }

    public boolean isLoadDisabled() {
        return backendStatus.isLoadDisabled;
    }

    public void setLoadDisabled(boolean isLoadDisabled) {
        this.backendStatus.isLoadDisabled = isLoadDisabled;
    }

    public String getDetailsForCreateReplica() {
        int hddBad = 0;
        int hddExceedLimit = 0;
        int hddOk = 0;
        int ssdBad = 0;
        int ssdExceedLimit = 0;
        int ssdOk = 0;
        for (DiskInfo disk : disksRef.values()) {
            TStorageMedium storageMedium = disk.getStorageMedium();
            if (storageMedium == TStorageMedium.HDD) {
                if (!disk.isAlive()) {
                    hddBad++;
                } else if (disk.exceedLimit(true)) {
                    hddExceedLimit++;
                } else {
                    hddOk++;
                }
            } else if (storageMedium == TStorageMedium.SSD) {
                if (!disk.isAlive()) {
                    ssdBad++;
                } else if (disk.exceedLimit(true)) {
                    ssdExceedLimit++;
                } else {
                    ssdOk++;
                }
            }
        }

        StringBuilder sb = new StringBuilder("[");
        sb.append("backendId=").append(id);
        sb.append(", host=").append(host);
        if (!isAlive()) {
            sb.append(", isAlive=false, exclude it");
        } else if (isDecommissioned()) {
            sb.append(", isDecommissioned=true, exclude it");
        } else if (isComputeNode()) {
            sb.append(", isComputeNode=true, exclude it");
        } else if (!Config.disable_backend_black_list && !SimpleScheduler.isAvailable(this)) {
            sb.append(", is in black list, exclude it");
        } else {
            sb.append(", hdd disks count={");
            if (hddOk > 0) {
                sb.append("ok=").append(hddOk).append(",");
            }
            if (hddBad > 0) {
                sb.append("bad=").append(hddBad).append(",");
            }
            if (hddExceedLimit > 0) {
                sb.append("capExceedLimit=").append(hddExceedLimit).append(",");
            }
            sb.append("}, ssd disk count={");
            if (ssdOk > 0) {
                sb.append("ok=").append(ssdOk).append(",");
            }
            if (ssdBad > 0) {
                sb.append("bad=").append(ssdBad).append(",");
            }
            if (ssdExceedLimit > 0) {
                sb.append("capExceedLimit=").append(ssdExceedLimit).append(",");
            }
            sb.append("}");
        }
        sb.append("]");

        return sb.toString();
    }

    // for test only
    public void updateOnce(int bePort, int httpPort, int beRpcPort) {
        if (this.bePort != bePort) {
            this.bePort = bePort;
        }

        if (this.httpPort != httpPort) {
            this.httpPort = httpPort;
        }

        if (this.beRpcPort != beRpcPort) {
            this.beRpcPort = beRpcPort;
        }

        long currentTime = System.currentTimeMillis();
        this.lastUpdateMs = currentTime;
        if (!isAlive.get()) {
            this.lastStartTime = currentTime;
            LOG.info("{} is alive,", this.toString());
            this.isAlive.set(true);
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

    public void setHost(String host) {
        this.host = host;
    }

    public void setAlive(boolean isAlive) {
        this.isAlive.set(isAlive);
    }

    public void setBePort(int agentPort) {
        this.bePort = agentPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public void setBeRpcPort(int beRpcPort) {
        this.beRpcPort = beRpcPort;
    }

    public void setBrpcPort(int brpcPort) {
        this.brpcPort = brpcPort;
    }

    public void setArrowFlightSqlPort(int arrowFlightSqlPort) {
        this.arrowFlightSqlPort = arrowFlightSqlPort;
    }

    public void setCpuCores(int cpuCores) {
        this.cpuCores = cpuCores;
    }

    public void setPipelineExecutorSize(int pipelineExecutorSize) {
        this.pipelineExecutorSize = pipelineExecutorSize;
    }

    public long getLastUpdateMs() {
        return this.lastUpdateMs;
    }

    public void setLastUpdateMs(long currentTime) {
        this.lastUpdateMs = currentTime;
    }

    public long getLastStartTime() {
        return this.lastStartTime;
    }

    public void setLastStartTime(long currentTime) {
        this.lastStartTime = currentTime;
    }

    public int getCputCores() {
        return cpuCores;
    }

    public int getPipelineExecutorSize() {
        return pipelineExecutorSize;
    }

    public long getLastMissingHeartbeatTime() {
        return lastMissingHeartbeatTime;
    }

    // Backend process epoch, is uesd to tag a beckend process
    // Currently it is always equal to be start time, even during oplog replay.
    public long getProcessEpoch() {
        return lastStartTime;
    }

    public boolean isAlive() {
        return this.isAlive.get();
    }

    public boolean isDecommissioned() {
        return this.isDecommissioned.get();
    }

    public boolean isQueryAvailable() {
        return isAlive() && !isQueryDisabled() && !isShutDown.get();
    }

    public boolean isScheduleAvailable() {
        return isAlive() && !isDecommissioned();
    }

    public boolean isLoadAvailable() {
        return isAlive() && !isLoadDisabled();
    }

    public void setDisks(ImmutableMap<String, DiskInfo> disks) {
        this.disksRef = disks;
    }

    public BackendStatus getBackendStatus() {
        return backendStatus;
    }

    public int getHeartbeatFailureCounter() {
        return heartbeatFailureCounter;
    }

    public ImmutableMap<String, DiskInfo> getDisks() {
        return this.disksRef;
    }

    public boolean hasPathHash() {
        return disksRef.values().stream().allMatch(DiskInfo::hasPathHash);
    }

    public boolean hasSpecifiedStorageMedium(TStorageMedium storageMedium) {
        return disksRef.values().stream().anyMatch(d -> d.isStorageMediumMatch(storageMedium));
    }

    public long getTotalCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long totalCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                totalCapacityB += diskInfo.getTotalCapacityB();
            }
        }
        return totalCapacityB;
    }

    public long getAvailableCapacityB() {
        // when system init, disks is empty, return 1L.
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long availableCapacityB = 1L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                availableCapacityB += diskInfo.getAvailableCapacityB();
            }
        }
        return availableCapacityB;
    }

    public long getDataUsedCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long dataUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                dataUsedCapacityB += diskInfo.getDataUsedCapacityB();
            }
        }
        return dataUsedCapacityB;
    }

    public long getTrashUsedCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long trashUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                trashUsedCapacityB += diskInfo.getTrashUsedCapacityB();
            }
        }
        return trashUsedCapacityB;
    }

    public long getRemoteUsedCapacityB() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        long totalRemoteUsedCapacityB = 0L;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                totalRemoteUsedCapacityB += diskInfo.getRemoteUsedCapacity();
            }
        }
        return totalRemoteUsedCapacityB;
    }

    public double getMaxDiskUsedPct() {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        double maxPct = 0.0;
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                double percent = diskInfo.getUsedPct();
                if (percent > maxPct) {
                    maxPct = percent;
                }
            }
        }
        return maxPct;
    }

    public boolean diskExceedLimitByStorageMedium(TStorageMedium storageMedium) {
        if (getDiskNumByStorageMedium(storageMedium) <= 0) {
            return true;
        }
        ImmutableMap<String, DiskInfo> diskInfos = disksRef;
        boolean exceedLimit = true;
        for (DiskInfo diskInfo : diskInfos.values()) {
            if (diskInfo.getState() == DiskState.ONLINE && diskInfo.getStorageMedium()
                    == storageMedium && !diskInfo.exceedLimit(true)) {
                exceedLimit = false;
                break;
            }
        }
        return exceedLimit;
    }

    public boolean diskExceedLimit() {
        if (getDiskNum() <= 0) {
            return true;
        }
        ImmutableMap<String, DiskInfo> diskInfos = disksRef;
        boolean exceedLimit = true;
        for (DiskInfo diskInfo : diskInfos.values()) {
            if (diskInfo.getState() == DiskState.ONLINE && !diskInfo.exceedLimit(true)) {
                exceedLimit = false;
                break;
            }
        }
        return exceedLimit;
    }

    public void updateDisks(Map<String, TDisk> backendDisks) {
        ImmutableMap<String, DiskInfo> disks = disksRef;
        // The very first time to init the path info
        if (!initPathInfo) {
            boolean allPathHashUpdated = true;
            for (DiskInfo diskInfo : disks.values()) {
                if (diskInfo.getPathHash() == 0) {
                    allPathHashUpdated = false;
                    break;
                }
            }
            if (allPathHashUpdated) {
                initPathInfo = true;
                Env.getCurrentSystemInfo().updatePathInfo(new ArrayList<>(disks.values()), Lists.newArrayList());
            }
        }

        // update status or add new diskInfo
        Map<String, DiskInfo> newDiskInfos = Maps.newHashMap();
        List<DiskInfo> addedDisks = Lists.newArrayList();
        List<DiskInfo> removedDisks = Lists.newArrayList();
        /*
         * set isChanged to true only if new disk is added or old disk is dropped.
         * we ignore the change of capacity, because capacity info is only used in master FE.
         */
        boolean isChanged = false;
        for (TDisk tDisk : backendDisks.values()) {
            String rootPath = tDisk.getRootPath();
            long totalCapacityB = tDisk.getDiskTotalCapacity();
            long dataUsedCapacityB = tDisk.getDataUsedCapacity();
            long trashUsedCapacityB = tDisk.getTrashUsedCapacity();
            long diskAvailableCapacityB = tDisk.getDiskAvailableCapacity();
            boolean isUsed = tDisk.isUsed();
            DiskInfo diskInfo = disks.get(rootPath);
            if (diskInfo == null) {
                diskInfo = new DiskInfo(rootPath);
                addedDisks.add(diskInfo);
                isChanged = true;
                LOG.info("add new disk info. backendId: {}, rootPath: {}", id, rootPath);
            }
            newDiskInfos.put(rootPath, diskInfo);

            diskInfo.setTotalCapacityB(totalCapacityB);
            diskInfo.setDataUsedCapacityB(dataUsedCapacityB);
            diskInfo.setTrashUsedCapacityB(trashUsedCapacityB);
            diskInfo.setAvailableCapacityB(diskAvailableCapacityB);
            if (tDisk.isSetRemoteUsedCapacity()) {
                diskInfo.setRemoteUsedCapacity(tDisk.getRemoteUsedCapacity());
            }

            if (tDisk.isSetPathHash()) {
                diskInfo.setPathHash(tDisk.getPathHash());
            }

            if (tDisk.isSetStorageMedium()) {
                diskInfo.setStorageMedium(tDisk.getStorageMedium());
            }

            if (isUsed) {
                if (diskInfo.setState(DiskState.ONLINE)) {
                    isChanged = true;
                }
            } else {
                if (diskInfo.setState(DiskState.OFFLINE)) {
                    isChanged = true;
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("update disk info. backendId: {}, diskInfo: {}", id, diskInfo.toString());
            }
        }

        // remove not exist rootPath in backend
        for (DiskInfo diskInfo : disks.values()) {
            String rootPath = diskInfo.getRootPath();
            if (!backendDisks.containsKey(rootPath)) {
                removedDisks.add(diskInfo);
                isChanged = true;
                LOG.warn("remove not exist rootPath. backendId: {}, rootPath: {}", id, rootPath);
            }
        }

        if (isChanged) {
            // update disksRef
            disksRef = ImmutableMap.copyOf(newDiskInfos);
            Env.getCurrentSystemInfo().updatePathInfo(addedDisks, removedDisks);
            // log disk changing
            Env.getCurrentEnv().getEditLog().logBackendStateChange(this);
        }
    }

    public boolean updateCpuInfo(int cpuCores, int pipelineExecutorSize) {
        boolean isChanged = false;

        if (this.cpuCores != cpuCores) {
            this.cpuCores = cpuCores;
            isChanged = true;
        }
        if (this.pipelineExecutorSize != pipelineExecutorSize) {
            this.pipelineExecutorSize = pipelineExecutorSize;
            isChanged = true;
        }
        return isChanged;
    }

    /**
     * In old version, there is only one tag for a Backend, and it is a "location" type tag.
     * But in new version, a Backend can have multi tag, so we need to put locationTag to
     * the new tagMap
     */
    private void convertToTagMapAndSetLocationTag() {
        if (tagMap == null) {
            // When first upgrade from old version, tags may be null
            tagMap = Maps.newHashMap();
        }
        if (!locationTag.value.equals(tagMap.get(Tag.TYPE_LOCATION))) {
            // ATTN: here we use Tag.TYPE_LOCATION directly, not locationTag.type,
            // because we need to make sure the previous tag must be a location type tag,
            // and if not, convert it to location type.
            tagMap.put(Tag.TYPE_LOCATION, locationTag.value);
        }
    }

    public static Backend read(DataInput in) throws IOException {
        String json = Text.readString(in);
        Backend be = GsonUtils.GSON.fromJson(json, Backend.class);
        be.convertToTagMapAndSetLocationTag();
        return be;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, heartbeatPort, bePort, isAlive.get());
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

        return (id == backend.id) && (host.equals(backend.host)) && (heartbeatPort == backend.heartbeatPort) && (bePort
                == backend.bePort) && (isAlive.get() == backend.isAlive.get());
    }

    @Override
    public String toString() {
        return "Backend [id=" + id + ", host=" + host + ", heartbeatPort=" + heartbeatPort + ", alive=" + isAlive.get()
                + ", lastStartTime=" + TimeUtils.longToTimeString(lastStartTime) + ", process epoch=" + lastStartTime
                + ", tags: " + tagMap + "]";
    }

    public String getHealthyStatus() {
        return "Backend [id=" + id + ", isDecommission: " + isDecommissioned
                + ", backendStatus: " + backendStatus + ", isAlive: " + isAlive.get() + ", lastUpdateTime: "
                + TimeUtils.longToTimeString(lastUpdateMs);
    }

    /**
     * handle Backend's heartbeat response.
     * return true if any port changed, or alive state is changed.
     */
    public boolean handleHbResponse(BackendHbResponse hbResponse, boolean isReplay) {
        boolean isChanged = false;
        if (hbResponse.getStatus() == HbStatus.OK) {
            if (!this.version.equals(hbResponse.getVersion())) {
                isChanged = true;
                this.version = hbResponse.getVersion();
            }

            if (this.bePort != hbResponse.getBePort() && !FeConstants.runningUnitTest) {
                isChanged = true;
                this.bePort = hbResponse.getBePort();
            }

            if (this.httpPort != hbResponse.getHttpPort() && !FeConstants.runningUnitTest) {
                isChanged = true;
                this.httpPort = hbResponse.getHttpPort();
            }

            if (this.brpcPort != hbResponse.getBrpcPort() && !FeConstants.runningUnitTest) {
                isChanged = true;
                this.brpcPort = hbResponse.getBrpcPort();
            }

            if (this.arrowFlightSqlPort != hbResponse.getArrowFlightSqlPort() && !FeConstants.runningUnitTest) {
                isChanged = true;
                this.arrowFlightSqlPort = hbResponse.getArrowFlightSqlPort();
            }

            if (this.isShutDown.get() != hbResponse.isShutDown()) {
                isChanged = true;
                LOG.info("{} shutdown state is changed", this.toString());
                this.isShutDown.set(hbResponse.isShutDown());
            }

            if (!this.getNodeRoleTag().value.equals(hbResponse.getNodeRole()) && Tag.validNodeRoleTag(
                    hbResponse.getNodeRole())) {
                isChanged = true;
                this.nodeRoleTag = Tag.createNotCheck(Tag.TYPE_ROLE, hbResponse.getNodeRole());
            }

            this.lastUpdateMs = hbResponse.getHbTime();
            if (!isAlive.get()) {
                isChanged = true;
                LOG.info("{} is back to alive, update start time from {} to {}, "
                        + "update be epoch from {} to {}.", this.toString(),
                        TimeUtils.longToTimeString(lastStartTime),
                        TimeUtils.longToTimeString(hbResponse.getBeStartTime()),
                        lastStartTime, hbResponse.getBeStartTime());
                this.lastStartTime = hbResponse.getBeStartTime();
                this.isAlive.set(true);
            }

            if (this.lastStartTime != hbResponse.getBeStartTime() && hbResponse.getBeStartTime() > 0) {
                LOG.info("{} update last start time from {} to {}, "
                        + "update be epoch from {} to {}.", this.toString(),
                        TimeUtils.longToTimeString(lastStartTime),
                        TimeUtils.longToTimeString(hbResponse.getBeStartTime()),
                        lastStartTime, hbResponse.getBeStartTime());
                this.lastStartTime = hbResponse.getBeStartTime();
                isChanged = true;
            }
            heartbeatErrMsg = "";
            this.heartbeatFailureCounter = 0;
        } else {
            // Only set backend to dead if the heartbeat failure counter exceed threshold.
            // And if it is a replay process, must set backend to dead.
            if (isReplay || ++this.heartbeatFailureCounter >= Config.max_backend_heartbeat_failure_tolerance_count) {
                if (isAlive.compareAndSet(true, false)) {
                    isChanged = true;
                    LOG.warn("{} is dead,", this.toString());
                }
            }

            // still set error msg and missing time even if we may not mark this backend as dead,
            // for debug easily.
            // But notice that if isChanged = false, these msg will not sync to other FE.
            heartbeatErrMsg = hbResponse.getMsg() == null ? "Unknown error" : hbResponse.getMsg();
            lastMissingHeartbeatTime = System.currentTimeMillis();
        }

        return isChanged;
    }

    public void setTabletMaxCompactionScore(long compactionScore) {
        tabletMaxCompactionScore = compactionScore;
    }

    public long getTabletMaxCompactionScore() {
        return tabletMaxCompactionScore;
    }

    private long getDiskNumByStorageMedium(TStorageMedium storageMedium) {
        return disksRef.values().stream().filter(v -> v.getStorageMedium() == storageMedium).count();
    }

    private int getDiskNum() {
        return disksRef.size();
    }

    /**
     * Note: This class must be a POJO in order to display in JSON format
     * Add additional information in the class to show in `show backends`
     * if just change new added backendStatus, you can do like following
     *     BackendStatus status = Backend.getBackendStatus();
     *     status.newItem = xxx;
     */
    public class BackendStatus {
        // this will be output as json, so not using FeConstants.null_string;
        public volatile String lastSuccessReportTabletsTime = "N/A";
        @SerializedName("lastStreamLoadTime")
        // the last time when the stream load status was reported by backend
        public volatile long lastStreamLoadTime = -1;
        @SerializedName("isQueryDisabled")
        public volatile boolean isQueryDisabled = false;
        @SerializedName("isLoadDisabled")
        public volatile boolean isLoadDisabled = false;

        @Override
        public String toString() {
            return "[" + "lastSuccessReportTabletsTime='" + lastSuccessReportTabletsTime + '\''
                    + ", lastStreamLoadTime=" + lastStreamLoadTime + ", isQueryDisabled=" + isQueryDisabled
                    + ", isLoadDisabled=" + isLoadDisabled + "]";
        }
    }

    public Tag getLocationTag() {
        return locationTag;
    }

    public Tag getNodeRoleTag() {
        return nodeRoleTag;
    }

    public boolean isMixNode() {
        return nodeRoleTag.value.equals(Tag.VALUE_MIX);
    }

    public boolean isComputeNode() {
        return nodeRoleTag.value.equals(Tag.VALUE_COMPUTATION);
    }

    public void setTagMap(Map<String, String> tagMap) {
        Preconditions.checkState(tagMap.containsKey(Tag.TYPE_LOCATION));
        this.tagMap = tagMap;
        this.locationTag = Tag.createNotCheck(Tag.TYPE_LOCATION, tagMap.get(Tag.TYPE_LOCATION));
        if (tagMap.containsKey(Tag.TYPE_ROLE) && Tag.validNodeRoleTag(tagMap.get(Tag.TYPE_ROLE))) {
            this.nodeRoleTag = Tag.createNotCheck(Tag.TYPE_ROLE, tagMap.get(Tag.TYPE_ROLE));
        }
    }

    public Map<String, String> getTagMap() {
        return tagMap;
    }

    public TNetworkAddress getBrpcAddress() {
        return new TNetworkAddress(getHost(), getBrpcPort());
    }

    public TNetworkAddress getHeartbeatAddress() {
        return new TNetworkAddress(getHost(), getHeartbeatPort());
    }

    public TNetworkAddress getArrowFlightAddress() {
        return new TNetworkAddress(getHost(), getArrowFlightSqlPort());
    }

    public String getTagMapString() {
        return "{" + new PrintableMap<>(tagMap, ":", true, false).toString() + "}";
    }

    public Set<String> getBeWorkloadGroupTagSet() {
        Set<String> beTagSet = Sets.newHashSet();
        String beTagStr = this.tagMap.get(Tag.WORKLOAD_GROUP);
        if (StringUtils.isEmpty(beTagStr)) {
            return beTagSet;
        }

        String[] beTagArr = beTagStr.split(",");
        for (String beTag : beTagArr) {
            beTagSet.add(beTag.trim());
        }

        return beTagSet;
    }

    public static boolean isMatchWorkloadGroupTag(String wgTagStr, Set<String> beTagSet) {
        if (StringUtils.isEmpty(wgTagStr)) {
            return true;
        }
        if (beTagSet.isEmpty()) {
            return false;
        }

        String[] wgTagArr = wgTagStr.split(",");
        Set<String> wgTagSet = new HashSet<>();
        for (String wgTag : wgTagArr) {
            wgTagSet.add(wgTag.trim());
        }

        return !Collections.disjoint(wgTagSet, beTagSet);
    }

}
