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
import org.apache.doris.common.util.HttpURLUtil;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.httpv2.meta.MetaBaseAction;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.Storage;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
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

    private Cloud.NodeInfoPB getLocalTypeFromMetaService() {
        // get helperNodes from ms
        Cloud.GetClusterResponse response = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
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
        helperNodes.addAll(allNodes.stream()
                .filter(nodeInfoPB -> nodeInfoPB.getNodeType() == NodeInfoPB.NodeType.FE_MASTER)
                .map(nodeInfoPB -> new HostInfo(
                Config.enable_fqdn_mode ? nodeInfoPB.getHost() : nodeInfoPB.getIp(), nodeInfoPB.getEditLogPort()))
                .collect(Collectors.toList()));
        // check only have one master node.
        Preconditions.checkState(helperNodes.size() == 1);

        Optional<NodeInfoPB> local = allNodes.stream().filter(n -> ((Config.enable_fqdn_mode ? n.getHost() : n.getIp())
                + "_" + n.getEditLogPort()).equals(selfNode.getIdent())).findAny();
        return local.orElse(null);
    }


    protected void getClusterIdAndRole() throws IOException {
        NodeInfoPB.NodeType type = NodeInfoPB.NodeType.UNKNOWN;
        String feNodeNameFromMeta = "";
        // cloud mode
        while (true) {
            Cloud.NodeInfoPB nodeInfoPB = null;
            try {
                nodeInfoPB = getLocalTypeFromMetaService();
            } catch (Exception e) {
                LOG.warn("failed to get local fe's type, sleep 5 s, try again. exception: {}", e.getMessage());
            }
            if (nodeInfoPB == null) {
                LOG.warn("failed to get local fe's type, sleep 5 s, try again.");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    LOG.warn("thread sleep Exception", e);
                }
                continue;
            }
            type = nodeInfoPB.getNodeType();
            feNodeNameFromMeta = genFeNodeNameFromMeta(
                Config.enable_fqdn_mode ? nodeInfoPB.getHost() : nodeInfoPB.getIp(),
                nodeInfoPB.getEditLogPort(), nodeInfoPB.getCtime() * 1000);
            break;
        }

        LOG.info("current fe's role is {}", type == NodeInfoPB.NodeType.FE_MASTER ? "MASTER" :
                type == NodeInfoPB.NodeType.FE_OBSERVER ? "OBSERVER" : "UNKNOWN");
        if (type == NodeInfoPB.NodeType.UNKNOWN) {
            LOG.warn("type current not support, please check it");
            System.exit(-1);
        }
        File roleFile = new File(super.imageDir, Storage.ROLE_FILE);
        File versionFile = new File(super.imageDir, Storage.VERSION_FILE);

        // if helper node is point to self, or there is ROLE and VERSION file in local.
        // get the node type from local
        if ((type == NodeInfoPB.NodeType.FE_MASTER) || type != NodeInfoPB.NodeType.FE_OBSERVER
                && (isMyself() || (roleFile.exists() && versionFile.exists()))) {
            if (!isMyself()) {
                LOG.info("find ROLE and VERSION file in local, ignore helper nodes: {}", helperNodes);
            }

            // check file integrity, if has.
            if ((roleFile.exists() && !versionFile.exists()) || (!roleFile.exists() && versionFile.exists())) {
                throw new IOException("role file and version file must both exist or both not exist. "
                    + "please specific one helper node to recover. will exit.");
            }

            // ATTN:
            // If the version file and role file does not exist and the helper node is itself,
            // this should be the very beginning startup of the cluster, so we create ROLE and VERSION file,
            // set isFirstTimeStartUp to true, and add itself to frontends list.
            // If ROLE and VERSION file is deleted for some reason, we may arbitrarily start this node as
            // FOLLOWER, which may cause UNDEFINED behavior.
            // Everything may be OK if the origin role is exactly FOLLOWER,
            // but if not, FE process will exit somehow.
            Storage storage = new Storage(this.imageDir);
            if (!roleFile.exists()) {
                // The very first time to start the first node of the cluster.
                // It should became a Master node (Master node's role is also FOLLOWER, which means electable)

                // For compatibility. Because this is the very first time to start, so we arbitrarily choose
                // a new name for this node
                role = FrontendNodeType.FOLLOWER;
                if (type == NodeInfoPB.NodeType.FE_MASTER) {
                    nodeName = feNodeNameFromMeta;
                } else {
                    nodeName = genFeNodeName(selfNode.getHost(), selfNode.getPort(), false /* new style */);
                }

                storage.writeFrontendRoleAndNodeName(role, nodeName);
                LOG.info("very first time to start this node. role: {}, node name: {}", role.name(), nodeName);
            } else {
                role = storage.getRole();
                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }

                nodeName = storage.getNodeName();
                if (Strings.isNullOrEmpty(nodeName)) {
                    // In normal case, if ROLE file exist, role and nodeName should both exist.
                    // But we will get a empty nodeName after upgrading.
                    // So for forward compatibility, we use the "old-style" way of naming: "ip_port",
                    // and update the ROLE file.
                    if (type == NodeInfoPB.NodeType.FE_MASTER) {
                        nodeName = feNodeNameFromMeta;
                    } else {
                        nodeName = genFeNodeName(selfNode.getHost(), selfNode.getPort(), true /* old style */);
                    }
                    storage.writeFrontendRoleAndNodeName(role, nodeName);
                    LOG.info("forward compatibility. role: {}, node name: {}", role.name(), nodeName);
                }
                // Notice:
                // With the introduction of FQDN, the nodeName is no longer bound to an IP address,
                // so consistency is no longer checked here. Otherwise, the startup will fail.
            }

            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            if (!versionFile.exists()) {
                clusterId = Config.cluster_id == -1 ? Storage.newClusterID() : Config.cluster_id;
                token = Strings.isNullOrEmpty(Config.auth_token) ? Storage.newToken() : Config.auth_token;
                storage = new Storage(clusterId, token, this.imageDir);
                storage.writeClusterIdAndToken();

                isFirstTimeStartUp = true;
                Frontend self = new Frontend(role, nodeName, selfNode.getHost(), selfNode.getPort());
                // Set self alive to true, the BDBEnvironment.getReplicationGroupAdmin() will rely on this to get
                // helper node, before the heartbeat thread is started.
                self.setIsAlive(true);
                // We don't need to check if frontends already contains self.
                // frontends must be empty cause no image is loaded and no journal is replayed yet.
                // And this frontend will be persisted later after opening bdbje environment.
                frontends.put(nodeName, self);
                LOG.info("add self frontend: {}", self);
            } else {
                clusterId = storage.getClusterID();
                if (storage.getToken() == null) {
                    token = Strings.isNullOrEmpty(Config.auth_token) ? Storage.newToken() : Config.auth_token;
                    LOG.info("new token={}", token);
                    storage.setToken(token);
                    storage.writeClusterIdAndToken();
                } else {
                    token = storage.getToken();
                }
                isFirstTimeStartUp = false;
            }
        } else {
            // cloud mode, type == NodeInfoPB.NodeType.FE_OBSERVER
            // try to get role and node name from helper node,
            // this loop will not end until we get certain role type and name
            while (true) {
                if (!getFeNodeTypeAndNameFromHelpers()) {
                    LOG.warn("current node is not added to the group. please add it first. "
                            + "sleep 5 seconds and retry, current helper nodes: {}", helperNodes);
                    try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e) {
                        LOG.warn("", e);
                        System.exit(-1);
                    }
                }

                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }
                break;
            }

            Preconditions.checkState(helperNodes.size() == 1);
            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            HostInfo rightHelperNode = helperNodes.get(0);

            Storage storage = new Storage(this.imageDir);
            if (roleFile.exists() && (role != storage.getRole() || !nodeName.equals(storage.getNodeName()))
                    || !roleFile.exists()) {
                storage.writeFrontendRoleAndNodeName(role, nodeName);
            }
            if (!versionFile.exists()) {
                // If the version file doesn't exist, download it from helper node
                if (!getVersionFileFromHelper(rightHelperNode)) {
                    throw new IOException("fail to download version file from "
                        + rightHelperNode.getHost() + " will exit.");
                }

                // NOTE: cluster_id will be init when Storage object is constructed,
                //       so we new one.
                storage = new Storage(this.imageDir);
                clusterId = storage.getClusterID();
                token = storage.getToken();
                if (Strings.isNullOrEmpty(token)) {
                    token = Config.auth_token;
                }
                LOG.info("get version file from helper, cluster id {}, token {}", clusterId, token);
            } else {
                // If the version file exist, read the cluster id and check the
                // id with helper node to make sure they are identical
                clusterId = storage.getClusterID();
                token = storage.getToken();
                LOG.info("check local cluster id {} and token via helper node", clusterId, token);
                try {
                    String url = "http://" + NetUtils
                            .getHostPortInAccessibleFormat(rightHelperNode.getHost(), Config.http_port) + "/check";
                    HttpURLConnection conn = HttpURLUtil.getConnectionWithNodeIdent(url);
                    conn.setConnectTimeout(2 * 1000);
                    conn.setReadTimeout(2 * 1000);
                    String clusterIdString = conn.getHeaderField(MetaBaseAction.CLUSTER_ID);
                    int remoteClusterId = Integer.parseInt(clusterIdString);
                    if (remoteClusterId != clusterId) {
                        LOG.error("cluster id is not equal with helper node {}. will exit. remote:{}, local:{}",
                                rightHelperNode.getHost(), clusterIdString, clusterId);
                        throw new IOException(
                            "cluster id is not equal with helper node "
                                + rightHelperNode.getHost() + ". will exit.");
                    }
                    String remoteToken = conn.getHeaderField(MetaBaseAction.TOKEN);
                    if (token == null && remoteToken != null) {
                        LOG.info("get token from helper node. token={}.", remoteToken);
                        token = remoteToken;
                        storage.writeClusterIdAndToken();
                        storage.reload();
                    }
                    if (Config.enable_token_check) {
                        Preconditions.checkNotNull(token);
                        Preconditions.checkNotNull(remoteToken);
                        if (!token.equals(remoteToken)) {
                            throw new IOException(
                                "token is not equal with helper node " + rightHelperNode.getHost()
                                    + ", local token " + token + ", remote token " + remoteToken + ". will exit.");
                        }
                    }
                } catch (Exception e) {
                    throw new IOException("fail to check cluster_id and token with helper node.", e);
                }
            }

            getNewImage(rightHelperNode);
        }

        if (Config.cluster_id != -1 && clusterId != Config.cluster_id) {
            throw new IOException("cluster id is not equal with config item cluster_id. will exit. "
                + "If you are in recovery mode, please also modify the cluster_id in 'doris-meta/image/VERSION'");
        }

        if (role.equals(FrontendNodeType.FOLLOWER)) {
            isElectable = true;
        } else {
            isElectable = false;
        }

        Preconditions.checkState(helperNodes.size() == 1);
        LOG.info("finished to get cluster id: {}, isElectable: {}, role: {}, node name: {}, token: {}",
                clusterId, isElectable, role.name(), nodeName, token);
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

        if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames().contains(clusterName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("current instance does not have a cluster name :{}", clusterName);
            }
            throw new DdlException(String.format("Cluster %s not exist", clusterName),
                ErrorCode.ERR_CLOUD_CLUSTER_ERROR);
        }
    }

    public void changeCloudCluster(String clusterName, ConnectContext ctx) throws DdlException {
        checkCloudClusterPriv(clusterName);
        ((CloudSystemInfoService) Env.getCurrentSystemInfo()).waitForAutoStart(clusterName);
        try {
            ((CloudSystemInfoService) Env.getCurrentSystemInfo()).addCloudCluster(clusterName, "");
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
}
