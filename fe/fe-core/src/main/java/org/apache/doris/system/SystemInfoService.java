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

import org.apache.doris.analysis.ModifyBackendClause;
import org.apache.doris.analysis.ModifyBackendHostNameClause;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);

    public static final String DEFAULT_CLUSTER = "default_cluster";

    public static final String NO_BACKEND_LOAD_AVAILABLE_MSG = "No backend load available.";

    public static final String NO_SCAN_NODE_BACKEND_AVAILABLE_MSG = "There is no scanNode Backend available.";

    private volatile ImmutableMap<Long, Backend> idToBackendRef = ImmutableMap.of();
    private volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef = ImmutableMap.of();

    private volatile ImmutableMap<Long, DiskInfo> pathHashToDiskInfoRef = ImmutableMap.of();

    public static class HostInfo implements Comparable<HostInfo> {
        public String host;
        public int port;

        public HostInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getIdent() {
            return host + "_" + port;
        }

        @Override
        public int compareTo(@NotNull HostInfo o) {
            int res = host.compareTo(o.getHost());
            if (res == 0) {
                return Integer.compare(port, o.getPort());
            }
            return res;
        }

        public boolean isSame(HostInfo other) {
            if (other.getPort() != port) {
                return false;
            }
            return host.equals(other.getHost());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HostInfo that = (HostInfo) o;
            return Objects.equals(host, that.getHost())
                    && Objects.equals(port, that.getPort());
        }

        @Override
        public String toString() {
            return "HostInfo{"
                    + "host='" + host + '\''
                    + ", port=" + port
                    + '}';
        }
    }

    // sort host backends list by num of backends, descending
    private static final Comparator<List<Backend>> hostBackendsListComparator = new Comparator<List<Backend>>() {
        @Override
        public int compare(List<Backend> list1, List<Backend> list2) {
            if (list1.size() > list2.size()) {
                return -1;
            } else {
                return 1;
            }
        }
    };

    public static TPaloNodesInfo createAliveNodesInfo() {
        TPaloNodesInfo nodesInfo = new TPaloNodesInfo();
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long id : systemInfoService.getAllBackendIds(true /*need alive*/)) {
            Backend backend = systemInfoService.getBackend(id);
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }

    // for deploy manager
    public void addBackends(List<HostInfo> hostInfos, boolean isFree)
            throws UserException {
        addBackends(hostInfos, Tag.DEFAULT_BACKEND_TAG.toMap());
    }

    /**
     * @param hostInfos : backend's ip, hostName and port
     * @throws DdlException
     */
    public void addBackends(List<HostInfo> hostInfos, Map<String, String> tagMap) throws UserException {
        for (HostInfo hostInfo : hostInfos) {
            // check is already exist
            if (getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort()) != null) {
                String backendIdentifier = hostInfo.getHost() + ":"
                        + hostInfo.getPort();
                throw new DdlException("Same backend already exists[" + backendIdentifier + "]");
            }
        }

        for (HostInfo hostInfo : hostInfos) {
            addBackend(hostInfo.getHost(), hostInfo.getPort(), tagMap);
        }
    }

    // for test
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(backend.getId(), backend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort, Map<String, String> tagMap) {
        Backend newBackend = new Backend(Env.getCurrentEnv().getNextId(), host, heartbeatPort);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        // set tags
        newBackend.setTagMap(tagMap);

        // log
        Env.getCurrentEnv().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void dropBackends(List<HostInfo> hostInfos) throws DdlException {
        for (HostInfo hostInfo : hostInfos) {
            // check is already exist
            if (getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort()) == null) {
                String backendIdentifier = hostInfo.getHost() + ":" + hostInfo.getPort();
                throw new DdlException("backend does not exists[" + backendIdentifier + "]");
            }
            dropBackend(hostInfo.getHost(), hostInfo.getPort());
        }
    }

    public void dropBackendsByIds(List<String> ids) throws DdlException {

        for (String id : ids) {
            if (getBackend(Long.parseLong(id)) == null) {
                throw new DdlException("backend does not exists[" + id + "]");
            }
            dropBackend(Long.parseLong(id));
        }

    }

    // for decommission
    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }
        dropBackend(backend.getHost(), backend.getHeartbeatPort());
    }

    // final entry of dropping backend
    public void dropBackend(String host, int heartbeatPort) throws DdlException {
        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (droppedBackend == null) {
            throw new DdlException("backend does not exists[" + host
                    + ":" + heartbeatPort + "]");
        }
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(droppedBackend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(droppedBackend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        // log
        Env.getCurrentEnv().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    // only for test
    public void dropAllBackend() {
        // update idToBackend
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        // update idToReportVersion
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();
    }

    public Backend getBackend(long backendId) {
        return idToBackendRef.get(backendId);
    }

    public boolean checkBackendLoadAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        if (backend == null || !backend.isLoadAvailable()) {
            return false;
        }
        return true;
    }

    public boolean checkBackendQueryAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        if (backend == null || !backend.isQueryAvailable()) {
            return false;
        }
        return true;
    }

    public boolean checkBackendScheduleAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        if (backend == null || !backend.isScheduleAvailable()) {
            return false;
        }
        return true;
    }

    public boolean checkBackendAlive(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        if (backend == null || !backend.isAlive()) {
            return false;
        }
        return true;
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithBePort(String ip, int bePort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(ip) && backend.getBePort() == bePort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithHttpPort(String ip, int httpPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(ip) && backend.getHttpPort() == httpPort) {
                return backend;
            }
        }
        return null;
    }

    public List<Long> getAllBackendIds() {
        return getAllBackendIds(false);
    }

    public int getBackendsNumber(boolean needAlive) {
        int beNumber = ConnectContext.get().getSessionVariable().getBeNumberForTest();
        if (beNumber < 0) {
            beNumber = getAllBackendIds(needAlive).size();
        }
        return beNumber;
    }

    public List<Long> getAllBackendIds(boolean needAlive) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());
        if (!needAlive) {
            return backendIds;
        } else {
            Iterator<Long> iter = backendIds.iterator();
            while (iter.hasNext()) {
                Backend backend = this.getBackend(iter.next());
                if (backend == null || !backend.isAlive()) {
                    iter.remove();
                }
            }
            return backendIds;
        }
    }

    public List<Long> getDecommissionedBackendIds() {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isDecommissioned()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    public List<Backend> getAllBackends() {
        return Lists.newArrayList(idToBackendRef.values());
    }

    public List<Backend> getMixBackends() {
        return idToBackendRef.values().stream().filter(backend -> backend.isMixNode()).collect(Collectors.toList());
    }

    public List<Backend> getCnBackends() {
        return idToBackendRef.values().stream().filter(backend -> backend.isComputeNode()).collect(Collectors.toList());
    }

    class BeComparator implements Comparator<Backend> {
        public int compare(Backend a, Backend b) {
            return (int) (a.getId() - b.getId());
        }
    }

    public List<Long> selectBackendIdsRoundRobinByPolicy(BeSelectionPolicy policy, int number,
            int nextIndex) {
        Preconditions.checkArgument(number >= -1);
        List<Backend> candidates = getCandidates(policy);
        if (number != -1 && candidates.size() < number) {
            LOG.info("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            return Lists.newArrayList();
        }

        int realIndex = nextIndex % candidates.size();
        List<Long> partialOrderList = new ArrayList<Long>();
        partialOrderList.addAll(candidates.subList(realIndex, candidates.size())
                .stream().map(b -> b.getId()).collect(Collectors.toList()));
        partialOrderList.addAll(candidates.subList(0, realIndex)
                .stream().map(b -> b.getId()).collect(Collectors.toList()));

        if (number == -1) {
            return partialOrderList;
        } else {
            return partialOrderList.subList(0, number);
        }
    }

    public List<Backend> getCandidates(BeSelectionPolicy policy) {
        List<Backend> candidates = policy.getCandidateBackends(idToBackendRef.values());
        if (candidates.isEmpty()) {
            LOG.info("Not match policy: {}. candidates num: {}", policy, candidates.size());
            return Lists.newArrayList();
        }

        if (!policy.allowOnSameHost) {
            Map<String, List<Backend>> backendMaps = Maps.newHashMap();
            for (Backend backend : candidates) {
                if (backendMaps.containsKey(backend.getHost())) {
                    backendMaps.get(backend.getHost()).add(backend);
                } else {
                    List<Backend> list = Lists.newArrayList();
                    list.add(backend);
                    backendMaps.put(backend.getHost(), list);
                }
            }
            candidates.clear();
            for (List<Backend> list : backendMaps.values()) {
                candidates.add(list.get(0));
            }
        }

        if (candidates.isEmpty()) {
            LOG.info("Not match policy: {}. candidates num: {}", policy, candidates.size());
            return Lists.newArrayList();
        }

        Collections.sort(candidates, new BeComparator());
        return candidates;
    }

    // Select the smallest number of tablets as the starting position of
    // round robin in the BE that match the policy
    public int getStartPosOfRoundRobin(Tag tag, TStorageMedium storageMedium) {
        BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                .needScheduleAvailable().needCheckDiskUsage().addTags(Sets.newHashSet(tag))
                .setStorageMedium(storageMedium);
        if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
            builder.allowOnSameHost();
        }

        BeSelectionPolicy policy = builder.build();
        List<Backend> candidates = getCandidates(policy);

        long minBeTabletsNum = Long.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i < candidates.size(); ++i) {
            long tabletsNum = Env.getCurrentInvertedIndex()
                    .getTabletIdsByBackendId(candidates.get(i).getId()).size();
            if (tabletsNum < minBeTabletsNum) {
                minBeTabletsNum = tabletsNum;
                minIndex = i;
            }
        }
        return minIndex;
    }

    public Map<Tag, List<Long>> getBeIdRoundRobinForReplicaCreation(
            ReplicaAllocation replicaAlloc, TStorageMedium storageMedium,
            Map<Tag, Integer> nextIndexs) throws DdlException {
        Map<Tag, List<Long>> chosenBackendIds = Maps.newHashMap();
        Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
        short totalReplicaNum = 0;
        for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
            BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                    .needScheduleAvailable().needCheckDiskUsage().addTags(Sets.newHashSet(entry.getKey()))
                    .setStorageMedium(storageMedium);
            if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
                builder.allowOnSameHost();
            }

            BeSelectionPolicy policy = builder.build();
            int nextIndex = nextIndexs.get(entry.getKey());
            List<Long> beIds = selectBackendIdsRoundRobinByPolicy(policy, entry.getValue(), nextIndex);
            nextIndexs.put(entry.getKey(), nextIndex + beIds.size());

            if (beIds.isEmpty()) {
                throw new DdlException("Failed to find " + entry.getValue() + " backend(s) for policy: " + policy);
            }
            chosenBackendIds.put(entry.getKey(), beIds);
            totalReplicaNum += beIds.size();
        }
        Preconditions.checkState(totalReplicaNum == replicaAlloc.getTotalReplicaNum());
        return chosenBackendIds;
    }

    /**
     * Select a set of backends for replica creation.
     * The following parameters need to be considered when selecting backends.
     *
     * @param replicaAlloc
     * @param storageMedium
     * @param isStorageMediumSpecified
     * @param isOnlyForCheck set true if only used for check available backend
     * @return return the selected backend ids group by tag.
     * @throws DdlException
     */
    public Map<Tag, List<Long>> selectBackendIdsForReplicaCreation(
            ReplicaAllocation replicaAlloc, TStorageMedium storageMedium, boolean isStorageMediumSpecified,
            boolean isOnlyForCheck)
            throws DdlException {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        Map<Tag, List<Long>> chosenBackendIds = Maps.newHashMap();
        Map<Tag, Short> allocMap = replicaAlloc.getAllocMap();
        short totalReplicaNum = 0;

        int aliveBackendNum = (int) copiedBackends.values().stream().filter(Backend::isAlive).count();
        if (aliveBackendNum < replicaAlloc.getTotalReplicaNum()) {
            throw new DdlException("replication num should be less than the number of available backends. "
                    + "replication num is " + replicaAlloc.getTotalReplicaNum()
                    + ", available backend num is " + aliveBackendNum);
        } else {
            List<String> failedEntries = Lists.newArrayList();

            for (Map.Entry<Tag, Short> entry : allocMap.entrySet()) {
                BeSelectionPolicy.Builder builder = new BeSelectionPolicy.Builder()
                        .needScheduleAvailable().needCheckDiskUsage().addTags(Sets.newHashSet(entry.getKey()))
                        .setStorageMedium(storageMedium);
                if (FeConstants.runningUnitTest || Config.allow_replica_on_same_host) {
                    builder.allowOnSameHost();
                }

                BeSelectionPolicy policy = builder.build();
                List<Long> beIds = selectBackendIdsByPolicy(policy, entry.getValue());
                // first time empty, retry with different storage medium
                // if only for check, no need to retry different storage medium to get backend
                if (beIds.isEmpty() && storageMedium != null && !isStorageMediumSpecified && !isOnlyForCheck) {
                    storageMedium = (storageMedium == TStorageMedium.HDD) ? TStorageMedium.SSD : TStorageMedium.HDD;
                    policy = builder.setStorageMedium(storageMedium).build();
                    beIds = selectBackendIdsByPolicy(policy, entry.getValue());
                }
                // after retry different storage medium, it's still empty
                if (beIds.isEmpty()) {
                    LOG.error("failed backend(s) for policy:" + policy);
                    String errorReplication = "replication tag: " + entry.getKey()
                            + ", replication num: " + entry.getValue()
                            + ", storage medium: " + storageMedium;
                    failedEntries.add(errorReplication);
                } else {
                    chosenBackendIds.put(entry.getKey(), beIds);
                    totalReplicaNum += beIds.size();
                }
            }

            if (!failedEntries.isEmpty()) {
                String failedMsg = Joiner.on("\n").join(failedEntries);
                throw new DdlException("Failed to find enough backend, please check the replication num,"
                        + "replication tag and storage medium.\n" + "Create failed replications:\n" + failedMsg);
            }
        }

        Preconditions.checkState(totalReplicaNum == replicaAlloc.getTotalReplicaNum());
        return chosenBackendIds;
    }

    /**
     * Select a set of backends by the given policy.
     *
     * @param policy
     * @param number number of backends which need to be selected. -1 means return as many as possible.
     * @return return #number of backend ids,
     * or empty set if no backends match the policy, or the number of matched backends is less than "number";
     */
    public List<Long> selectBackendIdsByPolicy(BeSelectionPolicy policy, int number) {
        Preconditions.checkArgument(number >= -1);
        List<Backend> candidates = policy.getCandidateBackends(idToBackendRef.values());
        if ((number != -1 && candidates.size() < number) || candidates.isEmpty()) {
            LOG.debug("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            return Lists.newArrayList();
        }
        // If only need one Backend, just return a random one.
        if (number == 1) {
            Collections.shuffle(candidates);
            return Lists.newArrayList(candidates.get(0).getId());
        }

        if (policy.allowOnSameHost) {
            Collections.shuffle(candidates);
            if (number == -1) {
                return candidates.stream().map(b -> b.getId()).collect(Collectors.toList());
            } else {
                return candidates.subList(0, number).stream().map(b -> b.getId()).collect(Collectors.toList());
            }
        }

        // for each host, random select one backend.
        Map<String, List<Backend>> backendMaps = Maps.newHashMap();
        for (Backend backend : candidates) {
            if (backendMaps.containsKey(backend.getHost())) {
                backendMaps.get(backend.getHost()).add(backend);
            } else {
                List<Backend> list = Lists.newArrayList();
                list.add(backend);
                backendMaps.put(backend.getHost(), list);
            }
        }
        candidates.clear();
        for (List<Backend> list : backendMaps.values()) {
            Collections.shuffle(list);
            candidates.add(list.get(0));
        }
        if (number != -1 && candidates.size() < number) {
            LOG.debug("Not match policy: {}. candidates num: {}, expected: {}", policy, candidates.size(), number);
            return Lists.newArrayList();
        }
        Collections.shuffle(candidates);
        if (number != -1) {
            return candidates.subList(0, number).stream().map(b -> b.getId()).collect(Collectors.toList());
        } else {
            return candidates.stream().map(b -> b.getId()).collect(Collectors.toList());
        }
    }

    public ImmutableMap<Long, Backend> getIdToBackend() {
        return idToBackendRef;
    }

    public ImmutableMap<Long, Backend> getAllBackendsMap() {
        return idToBackendRef;
    }

    public long getBackendReportVersion(long backendId) {
        AtomicLong atomicLong = null;
        if ((atomicLong = idToReportVersionRef.get(backendId)) == null) {
            return -1L;
        } else {
            return atomicLong.get();
        }
    }

    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId, long tableId) {
        AtomicLong atomicLong;
        if ((atomicLong = idToReportVersionRef.get(backendId)) != null) {
            Database db = (Database) Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("failed to update backend report version, db {} does not exist", dbId);
                return;
            }
            atomicLong.set(newReportVersion);
            LOG.debug("update backend {} report version: {}, db: {}, table: {}",
                    backendId, newReportVersion, dbId, tableId);
        }
    }

    public long saveBackends(CountingDataOutputStream dos, long checksum) throws IOException {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        int backendCount = idToBackend.size();
        checksum ^= backendCount;
        dos.writeInt(backendCount);
        for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
            long key = entry.getKey();
            checksum ^= key;
            dos.writeLong(key);
            entry.getValue().write(dos);
        }
        return checksum;
    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (int i = 0; i < count; i++) {
            long key = dis.readLong();
            checksum ^= key;
            Backend backend = Backend.read(dis);
            replayAddBackend(backend);
        }
        return checksum;
    }

    public void clear() {
        this.idToBackendRef = null;
        this.idToReportVersionRef = null;
    }

    public static HostInfo getHostAndPort(String hostPort)
            throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        HostInfo hostInfo = NetUtils.resolveHostInfoFromHostPort(hostPort);

        String host = hostInfo.getHost();
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("Host is null");
        }

        int heartbeatPort = -1;
        try {
            // validate port
            heartbeatPort = hostInfo.getPort();
            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return new HostInfo(host, heartbeatPort);
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
        }
    }


    public static Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
        HostInfo hostInfo = getHostAndPort(hostPort);
        return Pair.of(hostInfo.getHost(), hostInfo.getPort());
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;
    }

    public void replayDropBackend(Backend backend) {
        LOG.debug("replayDropBackend: {}", backend);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(backend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(backend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

    }

    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        // backend may already be dropped. this may happen when
        // drop and modify operations do not guarantee the order.
        if (memoryBe != null) {
            memoryBe.setHost(be.getHost());
            memoryBe.setBePort(be.getBePort());
            memoryBe.setAlive(be.isAlive());
            memoryBe.setDecommissioned(be.isDecommissioned());
            memoryBe.setHttpPort(be.getHttpPort());
            memoryBe.setBeRpcPort(be.getBeRpcPort());
            memoryBe.setBrpcPort(be.getBrpcPort());
            memoryBe.setArrowFlightSqlPort(be.getArrowFlightSqlPort());
            memoryBe.setLastUpdateMs(be.getLastUpdateMs());
            memoryBe.setLastStartTime(be.getLastStartTime());
            memoryBe.setDisks(be.getDisks());
            memoryBe.setCpuCores(be.getCputCores());
            memoryBe.setPipelineExecutorSize(be.getPipelineExecutorSize());
        }
    }

    private long getAvailableCapacityB() {
        long capacity = 0L;
        for (Backend backend : idToBackendRef.values()) {
            // Here we do not check if backend is alive,
            // We suppose the dead backends will back to alive later.
            if (backend.isDecommissioned()) {
                // Data on decommissioned backend will move to other backends,
                // So we need to minus size of those data.
                capacity -= backend.getDataUsedCapacityB();
            } else {
                capacity += backend.getAvailableCapacityB();
            }
        }
        return capacity;
    }

    public void checkAvailableCapacity() throws DdlException {
        if (getAvailableCapacityB() <= 0L) {
            throw new DdlException("System has no available disk capacity or no available BE nodes");
        }
    }

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
    public long getBackendIdByHost(String host) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Backend> selectedBackends = Lists.newArrayList();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host)) {
                selectedBackends.add(backend);
            }
        }

        if (selectedBackends.isEmpty()) {
            return -1L;
        }

        Collections.shuffle(selectedBackends);
        return selectedBackends.get(0).getId();
    }

    /*
     * Check if the specified disks' capacity has reached the limit.
     * bePathsMap is (BE id -> list of path hash)
     * If floodStage is true, it will check with the floodStage threshold.
     *
     * return Status.OK if not reach the limit
     */
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage) {
        LOG.debug("pathBeMap: {}", bePathsMap);
        ImmutableMap<Long, DiskInfo> pathHashToDiskInfo = pathHashToDiskInfoRef;
        for (Long beId : bePathsMap.keySet()) {
            for (Long pathHash : bePathsMap.get(beId)) {
                DiskInfo diskInfo = pathHashToDiskInfo.get(pathHash);
                if (diskInfo != null && diskInfo.exceedLimit(floodStage)) {
                    return new Status(TStatusCode.CANCELLED,
                            "disk " + diskInfo.getRootPath() + " on backend "
                                    + beId + " exceed limit usage, path hash: " + pathHash);
                }
            }
        }
        return Status.OK;
    }

    // update the path info when disk report
    // there is only one thread can update path info, so no need to worry about concurrency control
    public void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks) {
        Map<Long, DiskInfo> copiedPathInfos = Maps.newHashMap(pathHashToDiskInfoRef);
        for (DiskInfo diskInfo : addedDisks) {
            copiedPathInfos.put(diskInfo.getPathHash(), diskInfo);
        }
        for (DiskInfo diskInfo : removedDisks) {
            copiedPathInfos.remove(diskInfo.getPathHash());
        }
        ImmutableMap<Long, DiskInfo> newPathInfos = ImmutableMap.copyOf(copiedPathInfos);
        pathHashToDiskInfoRef = newPathInfos;
        LOG.debug("update path infos: {}", newPathInfos);
    }

    public void modifyBackendHost(ModifyBackendHostNameClause clause) throws UserException {
        Backend be = getBackendWithHeartbeatPort(clause.getHost(), clause.getPort());
        if (be == null) {
            throw new DdlException("backend does not exists[" + clause.getHost() + ":" + clause.getPort() + "]");
        }
        if (be.getHost().equals(clause.getNewHost())) {
            // no need to modify
            return;
        }
        be.setHost(clause.getNewHost());
        Env.getCurrentEnv().getEditLog().logModifyBackend(be);
    }

    public void modifyBackends(ModifyBackendClause alterClause) throws UserException {
        List<HostInfo> hostInfos = alterClause.getHostInfos();
        List<Backend> backends = Lists.newArrayList();
        if (hostInfos.isEmpty()) {
            List<String> ids = alterClause.getIds();
            for (String id : ids) {
                long backendId = Long.parseLong(id);
                Backend be = getBackend(backendId);
                if (be == null) {
                    throw new DdlException("backend does not exists[" + backendId + "]");
                }
                backends.add(be);
            }
        } else {
            for (HostInfo hostInfo : hostInfos) {
                Backend be = getBackendWithHeartbeatPort(hostInfo.getHost(), hostInfo.getPort());
                if (be == null) {
                    throw new DdlException(
                            "backend does not exists[" + hostInfo.getHost() + ":" + hostInfo.getPort() + "]");
                }
                backends.add(be);
            }
        }

        for (Backend be : backends) {
            boolean shouldModify = false;
            Map<String, String> tagMap = alterClause.getTagMap();
            if (!tagMap.isEmpty()) {
                be.setTagMap(tagMap);
                shouldModify = true;
            }

            if (alterClause.isQueryDisabled() != null) {
                if (!alterClause.isQueryDisabled().equals(be.isQueryDisabled())) {
                    be.setQueryDisabled(alterClause.isQueryDisabled());
                    shouldModify = true;
                }
            }

            if (alterClause.isLoadDisabled() != null) {
                if (!alterClause.isLoadDisabled().equals(be.isLoadDisabled())) {
                    be.setLoadDisabled(alterClause.isLoadDisabled());
                    shouldModify = true;
                }
            }

            if (shouldModify) {
                Env.getCurrentEnv().getEditLog().logModifyBackend(be);
                LOG.info("finished to modify backend {} ", be);
            }
        }
    }

    public void replayModifyBackend(Backend backend) {
        Backend memBe = getBackend(backend.getId());
        memBe.setTagMap(backend.getTagMap());
        memBe.setQueryDisabled(backend.isQueryDisabled());
        memBe.setLoadDisabled(backend.isLoadDisabled());
        memBe.setHost(backend.getHost());
        LOG.debug("replay modify backend: {}", backend);
    }

    // Check if there is enough suitable BE for replica allocation
    public void checkReplicaAllocation(ReplicaAllocation replicaAlloc) throws DdlException {
        List<Backend> backends = getMixBackends();
        for (Map.Entry<Tag, Short> entry : replicaAlloc.getAllocMap().entrySet()) {
            if (backends.stream().filter(b -> b.getLocationTag().equals(entry.getKey()))
                    .count() < entry.getValue()) {
                throw new DdlException(
                        "Failed to find enough host with tag(" + entry.getKey() + ") in all backends. need: "
                                + entry.getValue());
            }
        }
    }

    public Set<Tag> getTags() {
        List<Backend> bes = getMixBackends();
        Set<Tag> tags = Sets.newHashSet();
        for (Backend be : bes) {
            tags.add(be.getLocationTag());
        }
        return tags;
    }

    public List<Backend> getBackendsByTag(Tag tag) {
        List<Backend> bes = getMixBackends();
        return bes.stream().filter(b -> b.getLocationTag().equals(tag)).collect(Collectors.toList());
    }

    public int getMinPipelineExecutorSize() {
        if (idToBackendRef.size() == 0) {
            return 1;
        }
        int minPipelineExecutorSize = Integer.MAX_VALUE;
        for (Backend be : idToBackendRef.values()) {
            int size = be.getPipelineExecutorSize();
            if (size > 0) {
                minPipelineExecutorSize = Math.min(minPipelineExecutorSize, size);
            }
        }
        return minPipelineExecutorSize;
    }

    public long aliveBECount() {
        return idToBackendRef.values().stream().filter(Backend::isAlive).count();
    }
}
