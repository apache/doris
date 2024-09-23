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

import org.apache.doris.analysis.CancelCloudWarmUpStmt;
import org.apache.doris.analysis.CreateStageStmt;
import org.apache.doris.analysis.DropStageStmt;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.CloudWarmUpJob;
import org.apache.doris.cloud.CloudWarmUpJob.JobState;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.load.CleanCopyJobScheduler;
import org.apache.doris.cloud.persist.UpdateCloudReplicaInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.NodeInfoPB;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class CloudEnv extends Env {

    private static final Logger LOG = LogManager.getLogger(CloudEnv.class);

    private CloudInstanceStatusChecker cloudInstanceStatusChecker;
    private CloudClusterChecker cloudClusterCheck;
    private CloudUpgradeMgr upgradeMgr;

    private CloudTabletRebalancer cloudTabletRebalancer;
    private CacheHotspotManager cacheHotspotMgr;

    private boolean enableStorageVault;

    private CleanCopyJobScheduler cleanCopyJobScheduler;

    public CloudEnv(boolean isCheckpointCatalog) {
        super(isCheckpointCatalog);
        this.cleanCopyJobScheduler = new CleanCopyJobScheduler();
        this.loadManager = ((CloudEnvFactory) EnvFactory.getInstance())
                                    .createLoadManager(loadJobScheduler, cleanCopyJobScheduler);
        this.cloudClusterCheck = new CloudClusterChecker((CloudSystemInfoService) systemInfo);
        this.cloudInstanceStatusChecker = new CloudInstanceStatusChecker((CloudSystemInfoService) systemInfo);
        this.cloudTabletRebalancer = new CloudTabletRebalancer((CloudSystemInfoService) systemInfo);
        this.cacheHotspotMgr = new CacheHotspotManager((CloudSystemInfoService) systemInfo);
        this.upgradeMgr = new CloudUpgradeMgr((CloudSystemInfoService) systemInfo);
    }

    public CloudTabletRebalancer getCloudTabletRebalancer() {
        return this.cloudTabletRebalancer;
    }

    public CloudUpgradeMgr getCloudUpgradeMgr() {
        return this.upgradeMgr;
    }

    @Override
    public void initialize(String[] args) throws Exception {
        if (Strings.isNullOrEmpty(Config.cloud_unique_id)) {
            if (Strings.isNullOrEmpty(Config.cloud_instance_id)) {
                throw new UserException("cloud_instance_id must be specified if deployed in dissaggregated");
            }
            LOG.info("cloud_unique_id is not set, setting it using instance_id");
            Config.cloud_unique_id = "1:" + Config.cloud_instance_id + ":sql_server00";
        }

        LOG.info("Initializing CloudEnv with cloud_unique_id: {}", Config.cloud_unique_id);

        super.initialize(args);
    }

    @Override
    protected void startMasterOnlyDaemonThreads() {
        LOG.info("start cloud Master only daemon threads");
        super.startMasterOnlyDaemonThreads();
        cleanCopyJobScheduler.start();
        cloudClusterCheck.start();
        cloudTabletRebalancer.start();
        if (Config.enable_fetch_cluster_cache_hotspot) {
            cacheHotspotMgr.start();
        }
        upgradeMgr.start();
    }

    @Override
    protected void startNonMasterDaemonThreads() {
        LOG.info("start cloud Non Master only daemon threads");
        super.startNonMasterDaemonThreads();
        cloudInstanceStatusChecker.start();
    }

    public static String genFeNodeNameFromMeta(String host, int port, long timeMs) {
        return host + "_" + port + "_" + timeMs;
    }

    public CacheHotspotManager getCacheHotspotMgr() {
        return cacheHotspotMgr;
    }

    private CloudSystemInfoService getCloudSystemInfoService() {
        return (CloudSystemInfoService) systemInfo;
    }

    private Cloud.NodeInfoPB getLocalTypeFromMetaService() {
        // get helperNodes from ms
        Cloud.GetClusterResponse response = getCloudSystemInfoService()
                .getCloudCluster(Config.cloud_sql_server_cluster_name, Config.cloud_sql_server_cluster_id, "");
        if (!response.hasStatus() || !response.getStatus().hasCode()
                || response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("failed to get cloud cluster due to incomplete response, "
                    + "cloud_unique_id={}, clusterId={}, response={}",
                    Config.cloud_unique_id, Config.cloud_sql_server_cluster_id, response);
            return null;
        }
        LOG.info("get cluster response from meta service {}", response);
        // Note: get_cluster interface cluster(option -> repeated), so it has at least one cluster.
        if (response.getClusterCount() == 0) {
            LOG.warn("meta service error , return cluster zero, plz check it, "
                    + "cloud_unique_id={}, clusterId={}, response={}",
                    Config.cloud_unique_id, Config.cloud_sql_server_cluster_id, response);
            return null;
        }
        this.enableStorageVault = response.getEnableStorageVault();
        List<Cloud.NodeInfoPB> allNodes = response.getCluster(0).getNodesList()
                .stream().filter(NodeInfoPB::hasNodeType).collect(Collectors.toList());

        helperNodes.clear();
        if (allNodes.stream().anyMatch(n -> n.getNodeType() == NodeInfoPB.NodeType.FE_FOLLOWER)) {
            // multi followers mode, select first
            Optional<HostInfo> helperNode = allNodes.stream()
                    .filter(nodeInfoPB -> nodeInfoPB.getNodeType() == NodeInfoPB.NodeType.FE_FOLLOWER)
                    .map(nodeInfoPB -> new HostInfo(
                    Config.enable_fqdn_mode ? nodeInfoPB.getHost() : nodeInfoPB.getIp(), nodeInfoPB.getEditLogPort()))
                    .min(Comparator.comparing(HostInfo::getHost));
            helperNode.ifPresent(hostInfo -> helperNodes.add(hostInfo));
        } else {
            // master observers mode
            // helper node select follower's first, just one
            helperNodes.addAll(allNodes.stream()
                    .filter(nodeInfoPB -> nodeInfoPB.getNodeType() == NodeInfoPB.NodeType.FE_MASTER)
                    .map(nodeInfoPB -> new HostInfo(
                    Config.enable_fqdn_mode ? nodeInfoPB.getHost() : nodeInfoPB.getIp(), nodeInfoPB.getEditLogPort()))
                    .collect(Collectors.toList()));
            // check only have one master node.
        }
        Preconditions.checkState(helperNodes.size() == 1);

        Optional<NodeInfoPB> local = allNodes.stream().filter(n -> ((Config.enable_fqdn_mode ? n.getHost() : n.getIp())
                + "_" + n.getEditLogPort()).equals(selfNode.getIdent())).findAny();
        return local.orElse(null);
    }

    private void tryAddMyselToMS() {
        try {
            try {
                if (Strings.isNullOrEmpty(Config.cloud_instance_id)) {
                    throw new DdlException("unable to create instance due to empty cloud_instance_id");
                }
                getCloudSystemInfoService().tryCreateInstance(Config.cloud_instance_id,
                        Config.cloud_instance_id, false);
            } catch (Exception e) {
                return;
            }
            addFrontend(FrontendNodeType.MASTER, selfNode.getHost(), selfNode.getPort());
        } catch (DdlException e) {
            LOG.warn("get ddl exception ", e);
        }
    }

    protected void getClusterIdAndRole() throws IOException {
        NodeInfoPB.NodeType type = NodeInfoPB.NodeType.UNKNOWN;
        // cloud mode
        while (true) {
            Cloud.NodeInfoPB nodeInfoPB = null;
            try {
                nodeInfoPB = getLocalTypeFromMetaService();
            } catch (Exception e) {
                LOG.warn("failed to get local fe's type, sleep {} s, try again. exception: {}",
                        Config.resource_not_ready_sleep_seconds, e.getMessage());
            }
            if (nodeInfoPB == null) {
                LOG.warn("failed to get local fe's type, sleep {} s, try again.",
                        Config.resource_not_ready_sleep_seconds);
                if (isStartFromEmpty()) {
                    tryAddMyselToMS();
                }
                try {
                    Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
                } catch (InterruptedException e) {
                    LOG.info("interrupted by {}", e);
                }
                continue;
            }
            type = nodeInfoPB.getNodeType();
            break;
        }

        LOG.info("current fe's role is {}", type == NodeInfoPB.NodeType.FE_MASTER ? "MASTER" :
                type == NodeInfoPB.NodeType.FE_FOLLOWER ? "FOLLOWER" :
                type == NodeInfoPB.NodeType.FE_OBSERVER ? "OBSERVER" : "UNKNOWN");
        if (type == NodeInfoPB.NodeType.UNKNOWN) {
            LOG.warn("type current not support, please check it");
            System.exit(-1);
        }

        super.getClusterIdAndRole();
    }

    @Override
    public long loadTransactionState(DataInputStream dis, long checksum) throws IOException {
        // for CloudGlobalTransactionMgr do nothing.
        return checksum;
    }

    @Override
    public long saveTransactionState(CountingDataOutputStream dos, long checksum) throws IOException {
        return checksum;
    }

    public void checkCloudClusterPriv(String clusterName) throws DdlException {
        // check resource usage privilege
        if (!Env.getCurrentEnv().getAuth().checkCloudPriv(ConnectContext.get().getCurrentUserIdentity(),
                clusterName, PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
            throw new DdlException("USAGE denied to user "
                + ConnectContext.get().getQualifiedUser() + "'@'" + ConnectContext.get().getRemoteIP()
                + "' for cloud cluster '" + clusterName + "'", ErrorCode.ERR_CLUSTER_NO_PERMISSIONS);
        }

        if (!getCloudSystemInfoService().getCloudClusterNames().contains(clusterName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("current instance does not have a cluster name :{}", clusterName);
            }
            throw new DdlException(String.format("Cluster %s not exist", clusterName),
                ErrorCode.ERR_CLOUD_CLUSTER_ERROR);
        }
    }

    public void changeCloudCluster(String clusterName, ConnectContext ctx) throws DdlException {
        checkCloudClusterPriv(clusterName);
        getCloudSystemInfoService().waitForAutoStart(clusterName);
        try {
            getCloudSystemInfoService().addCloudCluster(clusterName, "");
        } catch (UserException e) {
            throw new DdlException(e.getMessage(), e.getMysqlErrorCode());
        }
        ctx.setCloudCluster(clusterName);
        ctx.getState().setOk();
    }

    public String analyzeCloudCluster(String name, ConnectContext ctx) throws DdlException {
        String[] res = name.split("@");
        if (res.length != 1 && res.length != 2) {
            LOG.warn("invalid database name {}", name);
            throw new DdlException("Invalid database name: " + name, ErrorCode.ERR_BAD_DB_ERROR);
        }

        if (res.length == 1) {
            return name;
        }

        changeCloudCluster(res[1], ctx);
        return res[0];
    }

    public void replayUpdateCloudReplica(UpdateCloudReplicaInfo info) throws MetaNotFoundException {
        ((CloudInternalCatalog) getInternalCatalog()).replayUpdateCloudReplica(info);
    }

    public boolean getEnableStorageVault() {
        return this.enableStorageVault;
    }

    public void createStage(CreateStageStmt stmt) throws DdlException {
        if (Config.isNotCloudMode()) {
            throw new DdlException("stage is only supported in cloud mode");
        }
        if (!stmt.isDryRun()) {
            ((CloudInternalCatalog) getInternalCatalog()).createStage(stmt.toStageProto(), stmt.isIfNotExists());
        }
    }

    public void dropStage(DropStageStmt stmt) throws DdlException {
        if (Config.isNotCloudMode()) {
            throw new DdlException("stage is only supported in cloud mode");
        }
        ((CloudInternalCatalog) getInternalCatalog()).dropStage(Cloud.StagePB.StageType.EXTERNAL,
                null, null, stmt.getStageName(), null, stmt.isIfExists());
    }

    public long loadCloudWarmUpJob(DataInputStream dis, long checksum) throws Exception {
        int size = dis.readInt();
        long newChecksum = checksum ^ size;
        if (size > 0) {
            // There should be no old cloudWarmUp jobs, if exist throw exception, should not use this FE version
            throw new IOException("There are [" + size + "] cloud warm up jobs."
                    + " Please downgrade FE to an older version and handle residual jobs");
        }

        // finished or cancelled jobs
        size = dis.readInt();
        newChecksum ^= size;
        if (size > 0) {
            throw new IOException("There are [" + size + "] old finished or cancelled cloud warm up jobs."
                    + " Please downgrade FE to an older version and handle residual jobs");
        }

        size = dis.readInt();
        newChecksum ^= size;
        for (int i = 0; i < size; i++) {
            CloudWarmUpJob cloudWarmUpJob = CloudWarmUpJob.read(dis);
            if (cloudWarmUpJob.isExpire() || cloudWarmUpJob.getJobState() == JobState.DELETED) {
                LOG.info("cloud warm up job is expired, {}, ignore it", cloudWarmUpJob.getJobId());
                continue;
            }
            this.getCacheHotspotMgr().addCloudWarmUpJob(cloudWarmUpJob);
        }
        LOG.info("finished load cloud warm up job from image");
        return newChecksum;
    }

    public long saveCloudWarmUpJob(CountingDataOutputStream dos, long checksum) throws IOException {

        Map<Long, CloudWarmUpJob> cloudWarmUpJobs;
        cloudWarmUpJobs = this.getCacheHotspotMgr().getCloudWarmUpJobs();

        /*
         * reference: Env.java:saveAlterJob
         * alter jobs == 0
         * If the FE version upgrade from old version, if it have alter jobs, the FE will failed during start process
         *
         * the number of old version alter jobs has to be 0
         */
        int size = 0;
        checksum ^= size;
        dos.writeInt(size);

        checksum ^= size;
        dos.writeInt(size);

        size = cloudWarmUpJobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (CloudWarmUpJob cloudWarmUpJob : cloudWarmUpJobs.values()) {
            cloudWarmUpJob.write(dos);
        }
        return checksum;
    }

    public void cancelCloudWarmUp(CancelCloudWarmUpStmt stmt) throws DdlException {
        getCacheHotspotMgr().cancel(stmt);
    }

    @Override
    public void addFrontend(FrontendNodeType role, String host, int editLogPort) throws DdlException {
        getCloudSystemInfoService().addFrontend(role, host, editLogPort);
    }

    @Override
    public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        if (port == selfNode.getPort() && feType == FrontendNodeType.MASTER
                && selfNode.getHost().equals(host)) {
            throw new DdlException("can not drop current master node.");
        }

        getCloudSystemInfoService().dropFrontend(role, host, port);
    }

    @Override
    public void modifyFrontendHostName(String srcHost, int srcPort, String destHost) throws DdlException {
        throw new DdlException("modify frontend host name is not supported in cloud mode");
    }
}
