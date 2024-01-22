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

package org.apache.doris.cloud.system;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class CloudSystemInfoService extends SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(CloudSystemInfoService.class);

    @Override
    public Map<Tag, List<Long>> selectBackendIdsForReplicaCreation(
            ReplicaAllocation replicaAlloc, Map<Tag, Integer> nextIndexs,
            TStorageMedium storageMedium, boolean isStorageMediumSpecified,
            boolean isOnlyForCheck)
            throws DdlException {
        return Maps.newHashMap();
    }

    /**
     * Gets cloud cluster from remote with either clusterId or clusterName
     *
     * @param clusterName cluster name
     * @param clusterId cluster id
     * @return
     */
    public Cloud.GetClusterResponse getCloudCluster(String clusterName, String clusterId, String userName) {
        Cloud.GetClusterRequest.Builder builder = Cloud.GetClusterRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id)
            .setClusterName(clusterName).setClusterId(clusterId).setMysqlUserName(userName);
        final Cloud.GetClusterRequest pRequest = builder.build();
        Cloud.GetClusterResponse response;
        try {
            response = MetaServiceProxy.getInstance().getCluster(pRequest);
            return response;
        } catch (RpcException e) {
            LOG.warn("rpcToMetaGetClusterInfo exception: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }


    public synchronized void updateCloudBackends(List<Backend> toAdd, List<Backend> toDel) {
        // Deduplicate and validate
        if (toAdd.size() == 0 && toDel.size() == 0) {
            LOG.info("nothing to do");
            return;
        }
        Set<String> existedBes = idToBackendRef.entrySet().stream().map(i -> i.getValue())
                .map(i -> i.getHost() + ":" + i.getHeartbeatPort())
                .collect(Collectors.toSet());
        LOG.debug("deduplication existedBes={}", existedBes);
        LOG.debug("before deduplication toAdd={} toDel={}", toAdd, toDel);
        toAdd = toAdd.stream().filter(i -> !existedBes.contains(i.getHost() + ":" + i.getHeartbeatPort()))
            .collect(Collectors.toList());
        toDel = toDel.stream().filter(i -> existedBes.contains(i.getHost() + ":" + i.getHeartbeatPort()))
            .collect(Collectors.toList());
        LOG.debug("after deduplication toAdd={} toDel={}", toAdd, toDel);

        Map<String, List<Backend>> existedHostToBeList = idToBackendRef.values().stream().collect(Collectors.groupingBy(
                Backend::getHost));
        for (Backend be : toAdd) {
            Env.getCurrentEnv().getEditLog().logAddBackend(be);
            LOG.info("added cloud backend={} ", be);
            // backends is changed, regenerated tablet number metrics
            MetricRepo.generateBackendsTabletMetrics();

            String host = be.getHost();
            if (existedHostToBeList.keySet().contains(host)) {
                if (be.isSmoothUpgradeDst()) {
                    LOG.info("a new BE process will start on the existed node for smooth upgrading");
                    int beNum = existedHostToBeList.get(host).size();
                    Backend colocatedBe = existedHostToBeList.get(host).get(0);
                    if (beNum != 1) {
                        LOG.warn("find multiple co-located BEs, num: {}, select the 1st {} as migration src", beNum,
                                colocatedBe.getId());
                    }
                    colocatedBe.setSmoothUpgradeSrc(true);
                    handleNewBeOnSameNode(colocatedBe, be);
                } else {
                    LOG.warn("a new BE process will start on the existed node, it should not happend unless testing");
                }
            }
        }
        for (Backend be : toDel) {
            // drop be, set it not alive
            be.setAlive(false);
            be.setLastMissingHeartbeatTime(System.currentTimeMillis());
            Env.getCurrentEnv().getEditLog().logDropBackend(be);
            LOG.info("dropped cloud backend={}, and lastMissingHeartbeatTime={}", be, be.getLastMissingHeartbeatTime());
            // backends is changed, regenerated tablet number metrics
            MetricRepo.generateBackendsTabletMetrics();
        }

        // Update idToBackendRef
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        toAdd.forEach(i -> copiedBackends.put(i.getId(), i));
        toDel.forEach(i -> copiedBackends.remove(i.getId()));
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // Update idToReportVersionRef
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        toAdd.forEach(i -> copiedReportVersions.put(i.getId(), new AtomicLong(0L)));
        toDel.forEach(i -> copiedReportVersions.remove(i.getId()));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        updateCloudClusterMap(toAdd, toDel);
    }

    private void handleNewBeOnSameNode(Backend oldBe, Backend newBe) {
        LOG.info("new BE {} starts on the same node as existing BE {}", newBe.getId(), oldBe.getId());
        // TODO(merge-cloud): add it when has CloudTabletRebalancer.
        // Env.getCurrentEnv().getCloudTabletRebalancer().addTabletMigrationTask(oldBe.getId(), newBe.getId());
    }

    public void updateCloudClusterMap(List<Backend> toAdd, List<Backend> toDel) {
        lock.lock();
        Set<String> clusterNameSet = new HashSet<>();
        for (Backend b : toAdd) {
            String clusterName = b.getCloudClusterName();
            String clusterId = b.getCloudClusterId();
            if (clusterName.isEmpty() || clusterId.isEmpty()) {
                LOG.warn("cloud cluster name or id empty: id={}, name={}", clusterId, clusterName);
                continue;
            }
            clusterNameSet.add(clusterName);
            if (clusterNameSet.size() != 1) {
                LOG.warn("toAdd be list have multi clusterName, please check, Set: {}", clusterNameSet);
            }

            clusterNameToId.put(clusterName, clusterId);
            List<Backend> be = clusterIdToBackend.get(clusterId);
            if (be == null) {
                be = new ArrayList<>();
                clusterIdToBackend.put(clusterId, be);
                MetricRepo.registerClusterMetrics(clusterName, clusterId);
            }
            Set<String> existed = be.stream().map(i -> i.getHost() + ":" + i.getHeartbeatPort())
                    .collect(Collectors.toSet());
            // Deduplicate
            // TODO(gavin): consider vpc
            boolean alreadyExisted = existed.contains(b.getHost() + ":" + b.getHeartbeatPort());
            if (alreadyExisted) {
                LOG.info("BE already existed, clusterName={} clusterId={} backendNum={} backend={}",
                        clusterName, clusterId, be.size(), b);
                continue;
            }
            be.add(b);
            LOG.info("update (add) cloud cluster map, clusterName={} clusterId={} backendNum={} current backend={}",
                    clusterName, clusterId, be.size(), b);
        }

        for (Backend b : toDel) {
            String clusterName = b.getCloudClusterName();
            String clusterId = b.getCloudClusterId();
            // We actually don't care about cluster name here
            if (clusterName.isEmpty() || clusterId.isEmpty()) {
                LOG.warn("cloud cluster name or id empty: id={}, name={}", clusterId, clusterName);
                continue;
            }
            List<Backend> be = clusterIdToBackend.get(clusterId);
            if (be == null) {
                LOG.warn("try to remove a non-existing cluster, clusterId={} clusterName={}",
                        clusterId, clusterName);
                continue;
            }
            Set<Long> d = toDel.stream().map(i -> i.getId()).collect(Collectors.toSet());
            be = be.stream().filter(i -> !d.contains(i.getId())).collect(Collectors.toList());
            // ATTN: clusterId may have zero nodes
            clusterIdToBackend.replace(clusterId, be);
            // such as dropCluster, but no lock
            // ATTN: Empty clusters are treated as dropped clusters.
            if (be.size() == 0) {
                LOG.info("del clusterId {} and clusterName {} due to be nodes eq 0", clusterId, clusterName);
                boolean succ = clusterNameToId.remove(clusterName, clusterId);
                if (!succ) {
                    LOG.warn("impossible, somewhere err, clusterNameToId {}, "
                            + "want remove cluster name {}, cluster id {}", clusterNameToId, clusterName, clusterId);
                }
                clusterIdToBackend.remove(clusterId);
            }
            LOG.info("update (del) cloud cluster map, clusterName={} clusterId={} backendNum={} current backend={}",
                    clusterName, clusterId, be.size(), b);
        }
        lock.unlock();
    }


    public synchronized void updateFrontends(List<Frontend> toAdd,
                                             List<Frontend> toDel) throws DdlException {
        LOG.debug("updateCloudFrontends toAdd={} toDel={}", toAdd, toDel);
        String masterIp = Env.getCurrentEnv().getMasterHost();
        for (Frontend fe : toAdd) {
            if (masterIp.equals(fe.getHost())) {
                continue;
            }
            Env.getCurrentEnv().addFrontend(FrontendNodeType.OBSERVER,
                    fe.getHost(), fe.getEditLogPort(), fe.getNodeName());
            LOG.info("added cloud frontend={} ", fe);
        }
        for (Frontend fe : toDel) {
            if (masterIp.equals(fe.getHost())) {
                continue;
            }
            Env.getCurrentEnv().dropFrontend(FrontendNodeType.OBSERVER, fe.getHost(), fe.getEditLogPort());
            LOG.info("dropped cloud frontend={} ", fe);
        }
    }
}

