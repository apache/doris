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

import org.apache.doris.analysis.WarmUpClusterStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.CloudWarmUpJob;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;



public class CloudInstanceStatusChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudInstanceStatusChecker.class);
    private CloudSystemInfoService cloudSystemInfoService;
    // if find vcg failed sync, record it timestamp, virtual compute group name <-> timestamp
    private Map<String, Long> lastFailedSyncTimeMap = new ConcurrentHashMap<>();

    public CloudInstanceStatusChecker(CloudSystemInfoService cloudSystemInfoService) {
        super("cloud instance check", Config.cloud_cluster_check_interval_second * 1000L);
        this.cloudSystemInfoService = cloudSystemInfoService;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            Cloud.GetInstanceResponse response = cloudSystemInfoService.getCloudInstance();
            if (LOG.isDebugEnabled()) {
                LOG.debug("get from ms response {}", response);
            }
            if (!isResponseValid(response)) {
                return;
            }

            Cloud.InstanceInfoPB instance = response.getInstance();
            cloudSystemInfoService.setInstanceStatus(instance.getStatus());
            syncStorageVault(instance);
            processVirtualClusters(instance.getClustersList());

        } catch (Exception e) {
            LOG.warn("get instance from ms exception", e);
        }
    }

    private boolean isResponseValid(Cloud.GetInstanceResponse response) {
        if (response == null || !response.hasStatus() || !response.getStatus().hasCode()
                || response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("failed to get cloud instance due to incomplete response, "
                    + "cloud_unique_id={}, response={}", Config.cloud_unique_id, response);
            return false;
        }
        return true;
    }

    private void syncStorageVault(Cloud.InstanceInfoPB instance) {
        Map<String, String> vaultMap = new HashMap<>();
        int cnt = instance.getResourceIdsCount();
        for (int i = 0; i < cnt; i++) {
            String name = instance.getStorageVaultNames(i);
            String id = instance.getResourceIds(i);
            vaultMap.put(name, id);
        }
        Env.getCurrentEnv().getStorageVaultMgr().refreshVaultMap(vaultMap,
                Pair.of(instance.getDefaultStorageVaultName(), instance.getDefaultStorageVaultId()));
    }

    private void processVirtualClusters(List<Cloud.ClusterPB> clusters) {
        List<Cloud.ClusterPB> virtualClusters = new ArrayList<>();
        List<Cloud.ClusterPB> computeClusters = new ArrayList<>();
        categorizeClusters(clusters, virtualClusters, computeClusters);
        handleVirtualClusters(virtualClusters, computeClusters);
        removeObsoleteVirtualGroups(virtualClusters);
    }

    private void categorizeClusters(List<Cloud.ClusterPB> clusters,
                                    List<Cloud.ClusterPB> virtualClusters, List<Cloud.ClusterPB> computeClusters) {
        for (Cloud.ClusterPB cluster : clusters) {
            if (!cluster.hasType()) {
                LOG.warn("found a cluster {} which has no type", cluster);
                continue;
            }
            if (Cloud.ClusterPB.Type.COMPUTE == cluster.getType()) {
                computeClusters.add(cluster);
            }
            if (Cloud.ClusterPB.Type.VIRTUAL == cluster.getType()) {
                virtualClusters.add(cluster);
            }
        }
    }

    private void handleVirtualClusters(List<Cloud.ClusterPB> virtualGroups, List<Cloud.ClusterPB> computeClusters) {
        for (Cloud.ClusterPB virtualGroupInMs : virtualGroups) {
            ComputeGroup virtualGroupInFe = cloudSystemInfoService
                    .getComputeGroupById(virtualGroupInMs.getClusterId());
            if (virtualGroupInFe != null) {
                handleExistingVirtualComputeGroup(virtualGroupInMs, virtualGroupInFe);
            } else {
                handleNewVirtualComputeGroup(virtualGroupInMs, computeClusters);
            }
            // just fe master gen file cache sync task
            if (Env.getCurrentEnv().isMaster()) {
                // get again in fe mem
                virtualGroupInFe = cloudSystemInfoService
                    .getComputeGroupById(virtualGroupInMs.getClusterId());
                if (virtualGroupInFe == null) {
                    LOG.info("virtual compute can not find virtual group {} after handle, may be a empty vcg",
                            virtualGroupInMs);
                    continue;
                }
                syncFileCacheTasksForVirtualGroup(virtualGroupInMs, virtualGroupInFe);
            }
        }
    }

    private void cancelCacheJobs(ComputeGroup vcgInFe, List<String> jobIds) {
        CacheHotspotManager cacheHotspotManager = ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr();
        for (String jobId : jobIds) {
            try {
                if (Env.getCurrentEnv().isMaster()) {
                    // cancel old jobId, will write editlog, so just master can do
                    cacheHotspotManager.cancel(Long.parseLong(jobId));
                    LOG.info("virtual compute group {}, cancel jobId {}", vcgInFe.getName(), jobId);
                }
            } catch (DdlException e) {
                LOG.warn("virtual compute err, name {}, failed to cancel expired jobId failed {}",
                        vcgInFe.getName(), jobId, e);
            }
        }
    }

    private void checkNeedRebuildFileCache(ComputeGroup virtualGroupInFe, List<String> jobIdsInMs) {
        CacheHotspotManager cacheHotspotManager = ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr();
        // check jobIds in Ms valid, if been cancelled, start new jobs
        for (String jobId : jobIdsInMs) {
            CloudWarmUpJob job = cacheHotspotManager.getCloudWarmUpJob(Long.parseLong(jobId));
            if (job == null) {
                LOG.warn("virtual compute err, clusterName {} jobId {} not found, need rebuild file cache",
                        virtualGroupInFe.getName(), jobId);
                virtualGroupInFe.setNeedRebuildFileCache(true);
                return;
            }
            if (job.getSrcClusterName() == null || job.getDstClusterName() == null) {
                LOG.info("may be Upgrade after downgrade, warm up job info lost,"
                        + " so just rebuild job, clusterName {}, jobId {}",
                        virtualGroupInFe.getName(), jobId);
                virtualGroupInFe.setNeedRebuildFileCache(true);
                return;
            }
            // check src
            String expectedSrc = virtualGroupInFe.getActiveComputeGroup();
            if (!job.getSrcClusterName().equals(expectedSrc)) {
                LOG.debug("file cache job src mismatch: jobId {} jobSrc {} expectedSrc {}, need rebuild",
                        jobId, job.getSrcClusterName(), expectedSrc);
                virtualGroupInFe.setNeedRebuildFileCache(true);
                return;
            }
            // check dest
            String expectedDst = virtualGroupInFe.getStandbyComputeGroup();
            if (!job.getDstClusterName().equals(expectedDst)) {
                LOG.debug("file cache job dest mismatch: jobId {} jobDst {} expectedDst {}, need rebuild",
                        jobId, job.getDstClusterName(), expectedDst);
                virtualGroupInFe.setNeedRebuildFileCache(true);
                return;
            }
            // check job state
            CloudWarmUpJob.JobState jobState = job.getJobState();
            if (jobState == CloudWarmUpJob.JobState.CANCELLED) {
                LOG.warn("virtual compute err, clusterName {} jobId {} has been cancelled, need rebuild",
                        virtualGroupInFe.getName(), jobId);
                virtualGroupInFe.setNeedRebuildFileCache(true);
                return;
            }
        }
    }

    /**
     * Generates and synchronizes file cache related tasks for virtual computing groups on the FE master.
     */
    private void syncFileCacheTasksForVirtualGroup(Cloud.ClusterPB virtualGroupInMs, ComputeGroup virtualGroupInFe) {
        if (!virtualGroupInMs.hasClusterPolicy()) {
            LOG.warn("virtual compute err, clusterName {}, no cluster policy {}",
                    virtualGroupInFe.getName(), virtualGroupInMs);
            return;
        }
        CacheHotspotManager cacheHotspotManager = ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr();
        List<String> jobIdsInMs =
                new ArrayList<>(virtualGroupInMs.getClusterPolicy().getCacheWarmupJobidsList());

        checkNeedRebuildFileCache(virtualGroupInFe, jobIdsInMs);
        LOG.debug("virtual compute group {}, get from ms file cache sync task jobIds {}",
                virtualGroupInFe, jobIdsInMs);
        // virtual group has been changed in before step
        if (virtualGroupInFe.isNeedRebuildFileCache()) {
            String srcCg = virtualGroupInFe.getActiveComputeGroup();
            String dstCg = virtualGroupInFe.getStandbyComputeGroup();
            cancelCacheJobs(virtualGroupInFe, jobIdsInMs);
            try {
                // all
                Map<String, String> periodicProperties = new HashMap<>();
                // "sync_mode" = "periodic", "sync_interval_sec" = "fetch_cluster_cache_hotspot_interval_ms"
                periodicProperties.put("sync_mode", "periodic");
                long syncInterValSec = Config.fetch_cluster_cache_hotspot_interval_ms / 1000;
                if (syncInterValSec <= 0) {
                    LOG.warn("invalid Config fetch_cluster_cache_hotspot_interval_ms set it to 600s");
                    syncInterValSec = 600;
                }
                periodicProperties.put("sync_interval_sec", String.valueOf(syncInterValSec));
                WarmUpClusterStmt periodicStmtPeriodic =
                        new WarmUpClusterStmt(dstCg, srcCg, true, periodicProperties);
                long jobIdPeriodic = cacheHotspotManager.createJob(periodicStmtPeriodic);

                // load event
                Map<String, String> eventProperties = new HashMap<>();
                // "sync_mode" = "event_driven", "sync_event" = "load"
                eventProperties.put("sync_mode", "event_driven");
                eventProperties.put("sync_event", "load");
                WarmUpClusterStmt eventStmtPeriodic =
                        new WarmUpClusterStmt(dstCg, srcCg, true, eventProperties);
                long jobIdEvent = cacheHotspotManager.createJob(eventStmtPeriodic);
                // send jobIds to ms
                List<String> newJobIds = Arrays.asList(Long.toString(jobIdPeriodic), Long.toString(jobIdEvent));
                CloudSystemInfoService.updateFileCacheJobIds(virtualGroupInFe, newJobIds);
                LOG.info("virtual compute group {}, generate new jobIds periodic={}, event={}, and old jobIds {}",
                        virtualGroupInFe, jobIdPeriodic, jobIdEvent, jobIdsInMs);
            } catch (AnalysisException e) {
                LOG.warn("virtual compute err, name: {}, analysis error", virtualGroupInFe.getName(), e);
                return;
            }
            virtualGroupInFe.setNeedRebuildFileCache(false);
        }
    }

    private void handleExistingVirtualComputeGroup(Cloud.ClusterPB clusterInMs, ComputeGroup virtualGroupInFe) {
        if (!isClusterIdConsistent(clusterInMs, virtualGroupInFe)) {
            return;
        }

        if (!isClusterPolicyValid(clusterInMs)) {
            return;
        }

        if (!areSubComputeGroupsValid(clusterInMs, virtualGroupInFe)) {
            return;
        }

        diffAndUpdateComputeGroup(clusterInMs, virtualGroupInFe);
    }

    private boolean isClusterIdConsistent(Cloud.ClusterPB cluster, ComputeGroup computeGroup) {
        if (!cluster.getClusterId().equals(computeGroup.getId())) {
            LOG.warn("virtual compute err, group id changed, in fe={} but in ms={}, "
                    + "verbose {}, please check it",
                    computeGroup.getId(), cluster.getClusterId(), computeGroup);
            return false;
        }
        return true;
    }

    private boolean isClusterPolicyValid(Cloud.ClusterPB cluster) {
        if (!cluster.hasClusterPolicy()) {
            LOG.warn("virtual compute err, no cluster policy {}", cluster);
        }
        if (!cluster.getClusterPolicy().hasType()
                || cluster.getClusterPolicy().getType() != Cloud.ClusterPolicy.PolicyType.ActiveStandby) {
            LOG.warn("virtual compute err, current just support Virtual compute group policy ActiveStandby");
            return false;
        }
        if (cluster.getClusterPolicy().getStandbyClusterNamesList().size() != 1) {
            LOG.warn("virtual compute err, current just support one Standby compute group policy ActiveStandby,"
                    + " verbose {}", cluster);
            return false;
        }
        return true;
    }

    private boolean areSubComputeGroupsValid(Cloud.ClusterPB clusterInMs, ComputeGroup virtualGroupInFe) {
        List<String> subComputeGroups = clusterInMs.getClusterNamesList();
        if (subComputeGroups.isEmpty() || virtualGroupInFe.getSubComputeGroups() == null) {
            LOG.warn("virtual compute err, please check it, verbose {}", virtualGroupInFe);
            return false;
        }
        if (subComputeGroups.size() != virtualGroupInFe.getSubComputeGroups().size() || subComputeGroups.size() != 2) {
            LOG.warn("virtual compute err, sub compute group in fe {}, in ms {}",
                    virtualGroupInFe, subComputeGroups);
            return false;
        }
        return true;
    }

    private void diffAndUpdateComputeGroup(Cloud.ClusterPB cluster, ComputeGroup computeGroup) {
        // vcg rename logic, here cluster_id same, but cluster_name changed, so vcg renamed
        String clusterNameInMs = cluster.getClusterName();
        String computeGroupNameInFe = computeGroup.getName();
        if (!clusterNameInMs.equals(computeGroupNameInFe)) {
            LOG.info("virtual compute group renamed from {} to {}", computeGroupNameInFe, clusterNameInMs);
            computeGroup.setName(clusterNameInMs);
            cloudSystemInfoService.renameVirtualComputeGroup(computeGroup.getId(), computeGroupNameInFe, computeGroup);
        }

        List<String> subCgsInFe = computeGroup.getSubComputeGroups();
        List<String> subCgsInMs = new ArrayList<>(cluster.getClusterNamesList());
        Collections.sort(subCgsInFe);
        Collections.sort(subCgsInMs);

        if (!subCgsInFe.equals(subCgsInMs)) {
            LOG.info("virtual compute group change sub cgs from {} to {}", subCgsInFe, subCgsInMs);
            computeGroup.setSubComputeGroups(subCgsInMs);
            computeGroup.setNeedRebuildFileCache(true);
        }

        if (!cluster.getClusterPolicy().getActiveClusterName()
                .equals(computeGroup.getPolicy().activeComputeGroup)) {
            LOG.info("virtual compute group change active group from {} to {}",
                    computeGroup.getPolicy().activeComputeGroup,
                    cluster.getClusterPolicy().getActiveClusterName());
            computeGroup.getPolicy().setActiveComputeGroup(cluster.getClusterPolicy().getActiveClusterName());
            computeGroup.setNeedRebuildFileCache(true);
        }

        if (!cluster.getClusterPolicy().getStandbyClusterNames(0)
                .equals(computeGroup.getPolicy().standbyComputeGroup)) {
            LOG.info("virtual compute group change standby group from {} to {}",
                    computeGroup.getPolicy().standbyComputeGroup,
                    cluster.getClusterPolicy().getStandbyClusterNames(0));
            computeGroup.getPolicy().setStandbyComputeGroup(cluster.getClusterPolicy().getStandbyClusterNames(0));
            computeGroup.setNeedRebuildFileCache(true);
        }

        if (cluster.getClusterPolicy().getFailoverFailureThreshold()
                != computeGroup.getPolicy().failoverFailureThreshold) {
            LOG.info("virtual compute group change failover failure threshold from {} to {}",
                    computeGroup.getPolicy().failoverFailureThreshold,
                    cluster.getClusterPolicy().getFailoverFailureThreshold());
            computeGroup.getPolicy()
                    .setFailoverFailureThreshold(cluster.getClusterPolicy().getFailoverFailureThreshold());
        }

        if (cluster.getClusterPolicy().getUnhealthyNodeThresholdPercent()
                != computeGroup.getPolicy().unhealthyNodeThresholdPercent) {
            LOG.info("virtual compute group change unhealthy node threshold from {} to {}",
                    computeGroup.getPolicy().unhealthyNodeThresholdPercent,
                    cluster.getClusterPolicy().getUnhealthyNodeThresholdPercent());
            computeGroup.getPolicy()
                    .setUnhealthyNodeThresholdPercent(cluster.getClusterPolicy().getUnhealthyNodeThresholdPercent());
        }

        List<String> jobIdsInMs =
                new ArrayList<>(cluster.getClusterPolicy().getCacheWarmupJobidsList());
        List<String> jobIdsInFe = computeGroup.getPolicy().getCacheWarmupJobIds();
        if (!jobIdsInMs.equals(jobIdsInFe)) {
            LOG.debug("set exist vcg {}, jobIds in FE {} in Ms ms {}",
                    cluster.getClusterName(), jobIdsInFe, jobIdsInMs);
            computeGroup.getPolicy().setCacheWarmupJobIds(jobIdsInMs);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("after diff virtual cg info {}", computeGroup);
        }
    }

    private void handleNewVirtualComputeGroup(Cloud.ClusterPB cluster, List<Cloud.ClusterPB> computeClusters) {
        List<String> subComputeGroups = cluster.getClusterNamesList();
        if (subComputeGroups.isEmpty()) {
            LOG.info("found virtual cluster {} which has no sub clusters, skip empty virtual cluster", cluster);
            return;
        }
        if (subComputeGroups.size() != 2) {
            LOG.warn("virtual compute err, sub compute group size not eq 2, in ms {}", subComputeGroups);
            return;
        }
        if (!cluster.hasClusterPolicy()) {
            LOG.warn("virtual compute err, no cluster policy {}", cluster);
            return;
        }
        if (!cluster.getClusterPolicy().hasActiveClusterName()) {
            LOG.warn("virtual compute err, active cluster empty in ms {}", cluster);
            return;
        }
        if (cluster.getClusterPolicy().getStandbyClusterNamesList().size() != 1) {
            LOG.warn("virtual compute err, standby cluster size not eq 1 in ms {}", cluster);
            return;
        }
        checkSubClusters(subComputeGroups, cluster, computeClusters);
        ComputeGroup computeGroup = new ComputeGroup(cluster.getClusterId(),
                cluster.getClusterName(), ComputeGroup.ComputeTypeEnum.VIRTUAL);
        computeGroup.setSubComputeGroups(new ArrayList<>(subComputeGroups));
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(cluster.getClusterPolicy().getActiveClusterName());
        policy.setStandbyComputeGroup(cluster.getClusterPolicy().getStandbyClusterNames(0));
        policy.setFailoverFailureThreshold(cluster.getClusterPolicy().getFailoverFailureThreshold());
        policy.setUnhealthyNodeThresholdPercent(cluster.getClusterPolicy().getUnhealthyNodeThresholdPercent());
        computeGroup.setPolicy(policy);
        computeGroup.setNeedRebuildFileCache(true);
        cloudSystemInfoService.addComputeGroup(cluster.getClusterId(), computeGroup);
        MetricRepo.registerCloudMetrics(cluster.getClusterId(), cluster.getClusterName());
    }

    private void checkSubClusters(List<String> subClusterNames, Cloud.ClusterPB cluster,
                                  List<Cloud.ClusterPB> computeClustersInPB) {
        for (String subClusterName : subClusterNames) {
            if (cloudSystemInfoService.getCloudClusterIdByName(subClusterName) == null) {
                handleFailedSync(cluster, subClusterName, computeClustersInPB);
                continue;
            }
            // CloudClusterChecker find sub compute group
            lastFailedSyncTimeMap.remove(cluster.getClusterName());
        }
    }

    private void handleFailedSync(Cloud.ClusterPB cluster, String subClusterName,
                                  List<Cloud.ClusterPB> computeClustersInPB) {
        if (!lastFailedSyncTimeMap.containsKey(cluster.getClusterName())) {
            lastFailedSyncTimeMap.put(cluster.getClusterName(), System.currentTimeMillis());
        } else {
            List<String> computeGroupsInPb = computeClustersInPB.stream()
                    .map(Cloud.ClusterPB::getClusterName).collect(Collectors.toList());
            if (computeGroupsInPb.contains(subClusterName)) {
                LOG.warn("fe mem cant find {}, it may be wait cluster check to sync", subClusterName);
            } else {
                LOG.warn("fe mem and ms cant find {}, it may be dropped or renamed", subClusterName);
            }
            // sub cluster may be dropped or rename, or fe may be slowly,
            // need manual intervention
            if (System.currentTimeMillis() - lastFailedSyncTimeMap.get(cluster.getClusterName())
                    > 3 * Config.cloud_cluster_check_interval_second * 1000L) {
                LOG.warn("virtual compute err, cant find cluster info by cluster checker, "
                        + "sub cluster: {}, virtual cluster: {}", subClusterName, cluster.getClusterName());
            }
        }
    }

    private void removeObsoleteVirtualGroups(List<Cloud.ClusterPB> virtualClusters) {
        List<String> msVirtualClusters = virtualClusters.stream().map(Cloud.ClusterPB::getClusterId)
                .collect(Collectors.toList());
        for (ComputeGroup computeGroup : cloudSystemInfoService.getComputeGroups(true)) {
            // in fe mem, but not in meta server
            if (!msVirtualClusters.contains(computeGroup.getId())) {
                LOG.info("virtual compute group {} will be removed.", computeGroup.getName());
                cloudSystemInfoService.removeComputeGroup(computeGroup.getId(), computeGroup.getName());
                // cancel invalid job
                if (!computeGroup.getPolicy().getCacheWarmupJobIds().isEmpty()) {
                    cancelCacheJobs(computeGroup, computeGroup.getPolicy().getCacheWarmupJobIds());
                }
            }
        }
    }
}
