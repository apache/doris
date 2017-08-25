// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.alter.Alter;
import com.baidu.palo.alter.AlterJob;
import com.baidu.palo.alter.AlterJob.JobType;
import com.baidu.palo.alter.DecommissionBackendJob.DecomissionType;
import com.baidu.palo.alter.RollupHandler;
import com.baidu.palo.alter.SchemaChangeHandler;
import com.baidu.palo.alter.SystemHandler;
import com.baidu.palo.analysis.AddPartitionClause;
import com.baidu.palo.analysis.AlterClusterStmt;
import com.baidu.palo.analysis.AlterDatabaseQuotaStmt;
import com.baidu.palo.analysis.AlterDatabaseRename;
import com.baidu.palo.analysis.AlterSystemStmt;
import com.baidu.palo.analysis.AlterTableStmt;
import com.baidu.palo.analysis.AlterUserStmt;
import com.baidu.palo.analysis.BackupStmt;
import com.baidu.palo.analysis.CancelAlterSystemStmt;
import com.baidu.palo.analysis.CancelAlterTableStmt;
import com.baidu.palo.analysis.CancelBackupStmt;
import com.baidu.palo.analysis.ColumnRenameClause;
import com.baidu.palo.analysis.CreateClusterStmt;
import com.baidu.palo.analysis.CreateDbStmt;
import com.baidu.palo.analysis.CreateTableStmt;
import com.baidu.palo.analysis.CreateViewStmt;
import com.baidu.palo.analysis.DecommissionBackendClause;
import com.baidu.palo.analysis.DistributionDesc;
import com.baidu.palo.analysis.DropClusterStmt;
import com.baidu.palo.analysis.DropDbStmt;
import com.baidu.palo.analysis.DropPartitionClause;
import com.baidu.palo.analysis.DropTableStmt;
import com.baidu.palo.analysis.HashDistributionDesc;
import com.baidu.palo.analysis.KeysDesc;
import com.baidu.palo.analysis.LinkDbStmt;
import com.baidu.palo.analysis.MigrateDbStmt;
import com.baidu.palo.analysis.ModifyPartitionClause;
import com.baidu.palo.analysis.PartitionDesc;
import com.baidu.palo.analysis.PartitionRenameClause;
import com.baidu.palo.analysis.RangePartitionDesc;
import com.baidu.palo.analysis.RecoverDbStmt;
import com.baidu.palo.analysis.RecoverPartitionStmt;
import com.baidu.palo.analysis.RecoverTableStmt;
import com.baidu.palo.analysis.RestoreStmt;
import com.baidu.palo.analysis.RollupRenameClause;
import com.baidu.palo.analysis.ShowAlterStmt.AlterType;
import com.baidu.palo.analysis.SingleRangePartitionDesc;
import com.baidu.palo.analysis.TableRenameClause;
import com.baidu.palo.backup.AbstractBackupJob;
import com.baidu.palo.backup.BackupHandler;
import com.baidu.palo.backup.BackupJob;
import com.baidu.palo.backup.RestoreJob;
import com.baidu.palo.catalog.BrokerMgr.BrokerAddress;
import com.baidu.palo.catalog.Database.DbState;
import com.baidu.palo.catalog.DistributionInfo.DistributionInfoType;
import com.baidu.palo.catalog.KuduPartition.KuduRange;
import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.OlapTable.OlapTableState;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.clone.Clone;
import com.baidu.palo.clone.CloneChecker;
import com.baidu.palo.cluster.BaseParam;
import com.baidu.palo.cluster.Cluster;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.FeConstants;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.MarkedCountDownLatch;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.common.util.KuduUtil;
import com.baidu.palo.common.util.PrintableMap;
import com.baidu.palo.common.util.PropertyAnalyzer;
import com.baidu.palo.common.util.Util;
import com.baidu.palo.consistency.ConsistencyChecker;
import com.baidu.palo.ha.FrontendNodeType;
import com.baidu.palo.ha.HAProtocol;
import com.baidu.palo.ha.MasterInfo;
import com.baidu.palo.journal.JournalCursor;
import com.baidu.palo.journal.JournalEntity;
import com.baidu.palo.journal.bdbje.Timestamp;
import com.baidu.palo.load.DeleteInfo;
import com.baidu.palo.load.ExportChecker;
import com.baidu.palo.load.ExportJob;
import com.baidu.palo.load.ExportMgr;
import com.baidu.palo.load.Load;
import com.baidu.palo.load.LoadChecker;
import com.baidu.palo.load.LoadErrorHub;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.load.LoadJob.JobState;
import com.baidu.palo.master.Checkpoint;
import com.baidu.palo.master.MetaHelper;
import com.baidu.palo.persist.ClusterInfo;
import com.baidu.palo.persist.DatabaseInfo;
import com.baidu.palo.persist.DropInfo;
import com.baidu.palo.persist.DropLinkDbAndUpdateDbInfo;
import com.baidu.palo.persist.DropPartitionInfo;
import com.baidu.palo.persist.EditLog;
import com.baidu.palo.persist.LinkDbInfo;
import com.baidu.palo.persist.ModifyPartitionInfo;
import com.baidu.palo.persist.PartitionPersistInfo;
import com.baidu.palo.persist.RecoverInfo;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.persist.Storage;
import com.baidu.palo.persist.StorageInfo;
import com.baidu.palo.persist.TableInfo;
import com.baidu.palo.persist.UpdateClusterAndBackends;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.qe.JournalObservable;
import com.baidu.palo.qe.SessionVariable;
import com.baidu.palo.qe.VariableMgr;
import com.baidu.palo.service.FrontendOptions;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.Frontend;
import com.baidu.palo.system.SystemInfoService;
import com.baidu.palo.system.Backend.BackendState;
import com.baidu.palo.task.AgentBatchTask;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.task.AgentTaskExecutor;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.task.CreateReplicaTask;
import com.baidu.palo.task.PullLoadJobMgr;
import com.baidu.palo.thrift.TStorageMedium;
import com.baidu.palo.thrift.TStorageType;
import com.baidu.palo.thrift.TTaskType;

import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.util.Iterator;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Catalog {
    private static final Logger LOG = LogManager.getLogger(Catalog.class);
    // 0 ~ 9999 used for qe
    public static final long NEXT_ID_INIT_VALUE = 10000;
    private static final int HTTP_TIMEOUT_SECOND = 5;
    private static final int STATE_CHANGE_CHECK_INTERVAL_MS = 100;
    private static final int REPLAY_INTERVAL_MS = 1;
    public static final String BDB_DIR = Config.meta_dir + "/bdb";
    public static final String IMAGE_DIR = Config.meta_dir + "/image";

    // Image file meta data version. Use this version to load image file
    private int imageVersion = 0;
    // Current journal meta data version. Use this version to load journals
    private int journalVersion = 0;
    private long epoch = 0;

    private Map<Long, Database> idToDb;
    private Map<String, Database> nameToDb;

    private Map<Long, Cluster> idToCluster;
    private Map<String, Cluster> nameToCluster;

    private Load load;
    private ExportMgr exportMgr;
    private Clone clone;
    private Alter alter;
    private ConsistencyChecker consistencyChecker;
    private BackupHandler backupHandler;

    private UserPropertyMgr userPropertyMgr;

    private Daemon cleaner; // To clean old LabelInfo, ExportJobInfos
    private Daemon replayer;
    private Daemon timePrinter;
    private Daemon listener;

    private ReentrantReadWriteLock lock;
    private boolean isFirstTimeStartUp = false;
    private boolean isMaster;
    private boolean isElectable;
    private boolean canWrite;
    private boolean canRead;

    // false if default_cluster is not created.
    private boolean isDefaultClusterCreated = false;

    private FrontendNodeType role;
    private FrontendNodeType feType;
    private FrontendNodeType formerFeType;
    // replica and observer use this value to decide provide read service or not
    private long synchronizedTimeMs;
    private int masterRpcPort;
    private int masterHttpPort;
    private String masterIp;

    // For metadata persistence
    private AtomicLong nextId = new AtomicLong(NEXT_ID_INIT_VALUE);
    private String metaDir;
    private EditLog editLog;
    private int clusterId;
    private long replayedJournalId; // For checkpoint and observer memory
                                    // replayed marker

    private static Catalog CHECKPOINT = null;
    private static long checkpointThreadId = -1;
    private Checkpoint checkpointer;
    private Pair<String, Integer> helperNode = null;
    private Pair<String, Integer> selfNode = null;
    private List<Frontend> frontends;
    private List<Frontend> removedFrontends;

    private HAProtocol haProtocol = null;

    private JournalObservable journalObservable;

    private SystemInfoService systemInfo;
    private TabletInvertedIndex tabletInvertedIndex;

    private CatalogRecycleBin recycleBin;
    private FunctionSet functionSet;

    private MetaReplayState metaReplayState;

    // TODO(zc):
    private PullLoadJobMgr pullLoadJobMgr;
    private BrokerMgr brokerMgr;

    public List<Frontend> getFrontends() {
        return frontends;
    }

    public List<Frontend> getRemovedFrontends() {
        return removedFrontends;
    }

    public JournalObservable getJournalObservable() {
        return journalObservable;
    }

    private SystemInfoService getClusterInfo() {
        return this.systemInfo;
    }

    private TabletInvertedIndex getTabletInvertedIndex() {
        return this.tabletInvertedIndex;
    }

    private CatalogRecycleBin getRecycleBin() {
        return this.recycleBin;
    }

    public MetaReplayState getMetaReplayState() {
        return metaReplayState;
    }

    private static class SingletonHolder {
        private static final Catalog INSTANCE = new Catalog();
    }

    private Catalog() {
        this.idToDb = new HashMap<Long, Database>();
        this.nameToDb = new HashMap<String, Database>();
        this.load = new Load();
        this.exportMgr = new ExportMgr();
        this.clone = new Clone();
        this.alter = new Alter();
        this.consistencyChecker = new ConsistencyChecker();
        this.backupHandler = new BackupHandler();
        this.lock = new ReentrantReadWriteLock(true);
        this.metaDir = Config.meta_dir;
        this.userPropertyMgr = new UserPropertyMgr();

        this.canWrite = false;
        this.canRead = false;
        this.replayedJournalId = 0;
        this.isMaster = false;
        this.isElectable = false;
        this.synchronizedTimeMs = 0;
        this.feType = FrontendNodeType.INIT;

        this.role = FrontendNodeType.UNKNOWN;
        this.frontends = new ArrayList<Frontend>();
        this.removedFrontends = new ArrayList<Frontend>();

        this.journalObservable = new JournalObservable();
        this.formerFeType = FrontendNodeType.INIT;
        this.masterRpcPort = 0;
        this.masterHttpPort = 0;
        this.masterIp = "";

        this.systemInfo = new SystemInfoService();
        this.tabletInvertedIndex = new TabletInvertedIndex();
        this.recycleBin = new CatalogRecycleBin();
        this.functionSet = new FunctionSet();
        this.functionSet.init();

        this.metaReplayState = new MetaReplayState();

        this.idToCluster = new HashMap<Long, Cluster>();
        this.nameToCluster = new HashMap<String, Cluster>();

        this.isDefaultClusterCreated = false;

        pullLoadJobMgr = new PullLoadJobMgr();
        brokerMgr = new BrokerMgr();
    }

    public static void destroyCheckpoint() {
        if (CHECKPOINT != null) {
            CHECKPOINT = null;
        }
    }

    // use this to get real Catalog instance
    public static Catalog getInstance() {
        return SingletonHolder.INSTANCE;
    }

    // use this to get CheckPoint Catalog instance
    public static Catalog getCheckpoint() {
        if (CHECKPOINT == null) {
            CHECKPOINT = new Catalog();
        }
        return CHECKPOINT;
    }

    private static Catalog getCurrentCatalog() {
        if (isCheckpointThread()) {
            return CHECKPOINT;
        } else {
            return SingletonHolder.INSTANCE;
        }
    }

    public PullLoadJobMgr getPullLoadJobMgr() {
        return pullLoadJobMgr;
    }

    public BrokerMgr getBrokerMgr() {
        return brokerMgr;
    }

    // use this to get correct ClusterInfoService instance
    public static SystemInfoService getCurrentSystemInfo() {
        return getCurrentCatalog().getClusterInfo();
    }

    // use this to get correct TabletInvertedIndex instance
    public static TabletInvertedIndex getCurrentInvertedIndex() {
        return getCurrentCatalog().getTabletInvertedIndex();
    }

    public static CatalogRecycleBin getCurrentRecycleBin() {
        return getCurrentCatalog().getRecycleBin();
    }

    // use this to get correct Catalog's journal version
    public static int getCurrentCatalogJournalVersion() {
        return getCurrentCatalog().getJournalVersion();
    }

    public static final boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
    }

    public void readLock() {
        this.lock.readLock().lock();
    }

    public void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public void initialize(String[] args) throws Exception {

        // 0. get local node and helper node info
        getSelfHostPort();
        checkArgs(args);

        // // 1. add Information schema database
        // unprotectCreateDb(new InfoSchemaDb());

        // 2. check and create dirs and files
        File meta = new File(metaDir);
        if (!meta.exists()) {
            LOG.error("{} does not exist, will exit", meta.getAbsolutePath());
            System.exit(-1);
        }

        if (Config.edit_log_type.equalsIgnoreCase("bdb")) {
            File bdbDir = new File(BDB_DIR);
            if (!bdbDir.exists()) {
                bdbDir.mkdirs();
            }

            File imageDir = new File(IMAGE_DIR);
            if (!imageDir.exists()) {
                imageDir.mkdirs();
            }
        }

        // 3. get cluster id and role (Observer or Replica)
        getClusterIdAndRole();

        // 4. Load image first and replay edits
        this.editLog = new EditLog();
        loadImage(IMAGE_DIR); // load image file
        editLog.open(); // open bdb env or local output stream
        this.userPropertyMgr.setEditLog(editLog);
        // 5. start load label cleaner thread
        createCleaner();
        cleaner.setName("labelCleaner");
        cleaner.setInterval(Config.label_clean_interval_second * 1000L);
        cleaner.start();

        // 6. start state listener thread
        createStateListener();
        listener.setName("stateListener");
        listener.setInterval(STATE_CHANGE_CHECK_INTERVAL_MS);
        listener.start();

        userPropertyMgr.setUp();
    }

    private void getClusterIdAndRole() throws IOException {
        File roleFile = new File(IMAGE_DIR, Storage.ROLE_FILE);
        File versionFile = new File(IMAGE_DIR, Storage.VERSION_FILE);

        // helper node is the local node. This usually happens when the very
        // first node to start
        // or when one node to restart.
        if (isMyself()) {
            if (roleFile.exists() && !versionFile.exists() || !roleFile.exists() && versionFile.exists()) {
                LOG.error("role file and version file must both exist or both not exist. "
                        + "please specific one helper node to recover. will exit.");
                System.exit(-1);
            }

            Storage storage = new Storage(IMAGE_DIR);
            if (!roleFile.exists()) {
                // The very first time to start the first node of the cluster.
                // It should be one REPLICA node.
                storage.writeFrontendRole(FrontendNodeType.FOLLOWER);
                role = FrontendNodeType.FOLLOWER;
            } else {
                role = storage.getRole();
                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }
            }

            if (!versionFile.exists()) {
                clusterId = Config.cluster_id == -1 ? Storage.newClusterID() : Config.cluster_id;
                storage = new Storage(clusterId, IMAGE_DIR);
                storage.writeClusterId();
                // If the version file and role file does not exist and the
                // helper node is itself,
                // it must be the very beginning startup of the cluster
                isFirstTimeStartUp = true;
                Frontend self = new Frontend(role, selfNode.first, selfNode.second);
                // In normal case, checkFeExist will return null.
                // However, if version file and role file are deleted by some
                // reason,
                // the self node may already added to the frontends list.
                if (checkFeExist(selfNode.first, selfNode.second) == null) {
                    frontends.add(self);
                }
            } else {
                storage = new Storage(IMAGE_DIR);
                clusterId = storage.getClusterID();
            }
        } else { // Designate one helper node. Get the roll and version info
                 // from the helper node
            role = getFeNodeType();
            if (role == FrontendNodeType.REPLICA) {
                // for compatibility
                role = FrontendNodeType.FOLLOWER;
            }
            Storage storage = new Storage(IMAGE_DIR);
            if (role == FrontendNodeType.UNKNOWN) {
                LOG.error("current node is not added to the group. please add it first.");
                System.exit(-1);
            }
            if (roleFile.exists() && role != storage.getRole() || !roleFile.exists()) {
                storage.writeFrontendRole(role);
            }
            if (!versionFile.exists()) {
                // If the version file doesn't exist, download it from helper
                // node
                if (!getVersionFile()) {
                    LOG.error("fail to download version file from " + helperNode.first + " will exit.");
                    System.exit(-1);
                }

                storage = new Storage(IMAGE_DIR);
                clusterId = storage.getClusterID();
            } else {
                // If the version file exist, read the cluster id and check the
                // id with helper node
                // to make sure they are identical
                clusterId = storage.getClusterID();
                try {
                    URL idURL = new URL("http://" + helperNode.first + ":" + Config.http_port + "/check");
                    HttpURLConnection conn = null;
                    conn = (HttpURLConnection) idURL.openConnection();
                    conn.setConnectTimeout(2 * 1000);
                    conn.setReadTimeout(2 * 1000);
                    String clusterIdString = conn.getHeaderField("cluster_id");
                    int remoteClusterId = Integer.parseInt(clusterIdString);
                    if (remoteClusterId != clusterId) {
                        LOG.error("cluster id not equal with node {}. will exit.", helperNode.first);
                        System.exit(-1);
                    }
                } catch (Exception e) {
                    LOG.warn(e);
                }
            }

            getNewImage();
        }

        if (Config.cluster_id != -1 && clusterId != Config.cluster_id) {
            LOG.error("cluster id is not equal with config item cluster_id. will exit.");
            System.exit(-1);
        }

        if (role.equals(FrontendNodeType.FOLLOWER)) {
            isElectable = true;
        } else {
            isElectable = false;
        }
    }

    // Get the roll info from helper node. UNKNOWN means this node is not added
    // to the cluster yet.
    private FrontendNodeType getFeNodeType() {
        try {
            URL url = new URL("http://" + helperNode.first + ":" + Config.http_port + "/role?host=" + selfNode.first
                    + "&port=" + selfNode.second);
            HttpURLConnection conn = null;
            conn = (HttpURLConnection) url.openConnection();
            String type = conn.getHeaderField("role");
            FrontendNodeType ret = FrontendNodeType.valueOf(type);
            LOG.info("get fe node type {} from {}:{}", ret, helperNode.first, Config.http_port);

            return ret;
        } catch (Exception e) {
            LOG.warn(e);
        }

        return FrontendNodeType.UNKNOWN;
    }

    private void getSelfHostPort() {
        selfNode = new Pair<String, Integer>(FrontendOptions.getLocalHostAddress(), Config.edit_log_port);
    }

    private void checkArgs(String[] args) throws AnalysisException {
        String helper = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-helper")) {
                if (i + 1 >= args.length) {
                    System.out.println("-helper need parameter host:port");
                    System.exit(-1);
                }
                helper = args[i + 1];
                break;
            }
        }

        // If helper node is not designated, use local node as helper node.
        if (helper != null) {
            helperNode = SystemInfoService.validateHostAndPort(helper);
        } else {
            helperNode = new Pair<String, Integer>(selfNode.first, Config.edit_log_port);
        }
    }

    private void transferToMaster() throws IOException {
        editLog.open();
        if (replayer != null) {
            replayer.exit();
        }
        if (!haProtocol.fencing()) {
            LOG.error("fencing failed. will exit.");
            System.exit(-1);
        }

        canWrite = false;
        canRead = false;

        long replayStartTime = System.currentTimeMillis();
        // replay journals. -1 means replay all the journals larger than current
        // journal id.
        replayJournal(-1);
        checkCurrentNodeExist();
        formerFeType = feType;
        long replayEndTime = System.currentTimeMillis();
        LOG.info("finish replay in " + (replayEndTime - replayStartTime) + " msec");

        editLog.rollEditLog();

        // Log meta_version
        if (journalVersion < FeConstants.meta_version) {
            editLog.logMetaVersion(FeConstants.meta_version);
            this.setJournalVersion(FeConstants.meta_version);
        }

        // Log the first frontend
        if (isFirstTimeStartUp) {
            Frontend self = new Frontend(role, selfNode.first, selfNode.second);
            editLog.logAddFirstFrontend(self);
        }

        isMaster = true;
        canWrite = true;
        canRead = true;
        String msg = "master finish replay journal, can write now.";
        System.out.println(msg);
        LOG.info(msg);

        checkpointer = new Checkpoint(editLog);
        checkpointer.setName("leaderCheckpointer");
        checkpointer.setInterval(FeConstants.checkpoint_interval_second * 1000L);

        checkpointer.start();
        checkpointThreadId = checkpointer.getId();
        LOG.info("checkpointer thread started. thread id is {}", checkpointThreadId);

        // ClusterInfoService
        Catalog.getCurrentSystemInfo().setMaster(FrontendOptions.getLocalHostAddress(), Config.rpc_port, clusterId, epoch);
        Catalog.getCurrentSystemInfo().start();

        pullLoadJobMgr.start();

        // Load checker
        LoadChecker.init(Config.load_checker_interval_second * 1000L);
        LoadChecker.startAll();

        // Export checker
        ExportChecker.init(Config.export_checker_interval_second * 1000L);
        ExportChecker.startAll();

        // Clone checker
        CloneChecker.getInstance().setInterval(Config.clone_checker_interval_second * 1000L);
        CloneChecker.getInstance().start();

        // Alter
        getAlterInstance().start();

        // Consistency checker
        getConsistencyChecker().start();

        // Backup handler
        getBackupHandler().start();

        // catalog recycle bin
        getRecycleBin().start();

        this.masterIp = FrontendOptions.getLocalHostAddress();
        this.masterRpcPort = Config.rpc_port;
        this.masterHttpPort = Config.http_port;

        MasterInfo info = new MasterInfo();
        info.setIp(FrontendOptions.getLocalHostAddress());
        info.setRpcPort(Config.rpc_port);
        info.setHttpPort(Config.http_port);
        editLog.logMasterInfo(info);

        createTimePrinter();
        timePrinter.setName("timePrinter");
        long tsInterval = (long) ((Config.meta_delay_toleration_second / 2.0) * 1000L);
        timePrinter.setInterval(tsInterval);
        timePrinter.start();

        if (isDefaultClusterCreated) {
            initDefaultCluster();
        }
    }

    private void transferToNonMaster() {
        canWrite = false;
        isMaster = false;

        // donot set canRead
        // let canRead be what it was

        if (formerFeType == FrontendNodeType.OBSERVER && feType == FrontendNodeType.UNKNOWN) {
            LOG.warn("OBSERVER to UNKNOWN, still offer read service");
            Config.ignore_meta_check = true;
            metaReplayState.setTransferToUnknown();
        } else {
            Config.ignore_meta_check = false;
        }

        if (replayer == null) {
            createReplayer();
            replayer.setName("replayer");
            replayer.setInterval(REPLAY_INTERVAL_MS);
            replayer.start();
        }

        formerFeType = feType;
    }

    /*
     * If the current node is not in the frontend list, then exit. This may
     * happen when this node is removed from frontend list, and the drop
     * frontend log is deleted because of checkpoint.
     */
    private void checkCurrentNodeExist() {
        if (Config.metadata_failure_recovery.equals("true")) {
            return;
        }

        Frontend fe = checkFeExist(selfNode.first, selfNode.second);
        if (fe == null) {
            LOG.error("current node is not added to the cluster, will exit");
            System.exit(-1);
        } else if (fe.getRole() != role) {
            LOG.error("current node role is {} not match with frontend recorded role {}. will exit", role,
                    fe.getRole());
            System.exit(-1);
        }
    }

    private boolean getVersionFile() throws IOException {
        try {
            String url = "http://" + helperNode.first + ":" + Config.http_port + "/version";
            File dir = new File(IMAGE_DIR);
            MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000,
                    MetaHelper.getOutputStream(Storage.VERSION_FILE, dir));
            MetaHelper.complete(Storage.VERSION_FILE, dir);
            return true;
        } catch (Exception e) {
            LOG.warn(e);
        }

        return false;
    }

    private void getNewImage() throws IOException {
        long localImageVersion = 0;
        Storage storage = new Storage(IMAGE_DIR);
        localImageVersion = storage.getImageSeq();

        try {
            URL infoUrl = new URL("http://" + helperNode.first + ":" + Config.http_port + "/info");
            StorageInfo info = getStorageInfo(infoUrl);
            long version = info.getImageSeq();
            if (version > localImageVersion) {
                String url = "http://" + helperNode.first + ":" + Config.http_port + "/image?version=" + version;
                String filename = Storage.IMAGE + "." + version;
                File dir = new File(IMAGE_DIR);
                MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000, MetaHelper.getOutputStream(filename, dir));
                MetaHelper.complete(filename, dir);
            }
        } catch (Exception e) {
            return;
        }
    }

    public boolean isMyself() {
        Preconditions.checkNotNull(selfNode);
        Preconditions.checkNotNull(helperNode);
        LOG.debug("self ip-port: {}:{}. helper ip-port: {}:{}", selfNode.first, selfNode.second, helperNode.first,
                helperNode.second);
        return selfNode.equals(helperNode);
    }

    private StorageInfo getStorageInfo(URL url) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(HTTP_TIMEOUT_SECOND * 1000);
            connection.setReadTimeout(HTTP_TIMEOUT_SECOND * 1000);
            return mapper.readValue(connection.getInputStream(), StorageInfo.class);
        } catch (JsonParseException e) {
            throw new IOException(e);
        } catch (JsonMappingException e) {
            throw new IOException(e);
        } catch (IOException e) {
            throw e;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public boolean hasReplayer() {
        return replayer != null;
    }

    public void loadImage(String imageDir) throws IOException, DdlException {
        Storage storage = new Storage(imageDir);
        clusterId = storage.getClusterID();
        File curFile = storage.getCurrentImageFile();
        if (!curFile.exists()) {
            // image.0 may not exist
            LOG.info("image does not exist: {}", curFile.getAbsolutePath());
            return;
        }
        replayedJournalId = storage.getImageSeq();
        LOG.info("start load image from {}. is ckpt: {}", curFile.getAbsolutePath(), Catalog.isCheckpointThread());
        long loadImageStartTime = System.currentTimeMillis();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));

        long checksum = 0;
        try {
            checksum = loadHeader(dis, checksum);
            checksum = loadMasterInfo(dis, checksum);
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
                checksum = loadFrontends(dis, checksum);
            }
            checksum = Catalog.getCurrentSystemInfo().loadBackends(dis, checksum);
            checksum = loadDb(dis, checksum);
            // ATTN: this should be done after load Db, and before loadAlterJob
            recreateTabletInvertIndex();

            checksum = loadLoadJob(dis, checksum);
            checksum = loadAlterJob(dis, checksum);
            checksum = loadBackupAndRestoreJob(dis, checksum);
            checksum = loadAccessService(dis, checksum);
            checksum = loadRecycleBin(dis, checksum);
            checksum = loadGlobalVariable(dis, checksum);
            checksum = loadCluster(dis, checksum);
            checksum = loadBrokers(dis, checksum);
            checksum = loadExportJob(dis, checksum);

            long remoteChecksum = dis.readLong();
            Preconditions.checkState(remoteChecksum == checksum, remoteChecksum + " vs. " + checksum);
        } finally {
            dis.close();
        }

        long loadImageEndTime = System.currentTimeMillis();
        LOG.info("finished load image in " + (loadImageEndTime - loadImageStartTime) + " ms");
    }

    private void recreateTabletInvertIndex() {
        if (isCheckpointThread()) {
            return;
        }

        // create inverted index
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        for (Database db : this.nameToDb.values()) {
            long dbId = db.getId();
            for (Table table : db.getTables()) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableId = olapTable.getId();
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();
                    for (MaterializedIndex index : partition.getMaterializedIndices()) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash);
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            for (Replica replica : tablet.getReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                            }
                        }
                    } // end for indices
                } // end for partitions
            } // end for tables
        } // end for dbs
    }

    public long loadHeader(DataInputStream dis, long checksum) throws IOException {
        imageVersion = dis.readInt();
        checksum ^= imageVersion;
        journalVersion = imageVersion;
        long replayedJournalId = dis.readLong();
        checksum ^= replayedJournalId;
        long id = dis.readLong();
        checksum ^= id;
        nextId.set(id);

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_32) {
            isDefaultClusterCreated = dis.readBoolean();
        }

        return checksum;
    }

    public long loadMasterInfo(DataInputStream dis, long checksum) throws IOException {
        masterIp = Text.readString(dis);
        masterRpcPort = dis.readInt();
        checksum ^= masterRpcPort;
        masterHttpPort = dis.readInt();
        checksum ^= masterHttpPort;

        return checksum;
    }

    public long loadFrontends(DataInputStream dis, long checksum) throws IOException {
        int size = dis.readInt();
        checksum ^= size;
        for (int i = 0; i < size; i++) {
            Frontend fe = Frontend.read(dis);
            frontends.add(fe);
        }

        size = dis.readInt();
        checksum ^= size;
        for (int i = 0; i < size; i++) {
            Frontend fe = Frontend.read(dis);
            removedFrontends.add(fe);
        }
        return checksum;
    }

    public long loadDb(DataInputStream dis, long checksum) throws IOException, DdlException {
        int dbCount = dis.readInt();
        checksum ^= dbCount;
        for (long i = 0; i < dbCount; ++i) {
            Database db = new Database();
            db.readFields(dis);
            checksum ^= db.getId();
            idToDb.put(db.getId(), db);
            nameToDb.put(db.getName(), db);
            if (db.getDbState() == DbState.LINK) {
                nameToDb.put(db.getAttachDb(), db);
            }
        }

        return checksum;
    }

    public long loadLoadJob(DataInputStream dis, long checksum) throws IOException, DdlException {
        // load jobs
        int jobSize = dis.readInt();
        checksum ^= jobSize;
        for (int i = 0; i < jobSize; i++) {
            long dbId = dis.readLong();
            checksum ^= dbId;

            int loadJobCount = dis.readInt();
            checksum ^= loadJobCount;
            for (int j = 0; j < loadJobCount; j++) {
                LoadJob job = new LoadJob();
                job.readFields(dis);
                long currentTimeMs = System.currentTimeMillis();

                // Delete the history load jobs that are older than
                // LABEL_KEEP_MAX_MS
                // This job must be FINISHED or CANCELLED
                if ((currentTimeMs - job.getCreateTimeMs()) / 1000 <= Config.label_keep_max_second
                        || (job.getState() != JobState.FINISHED && job.getState() != JobState.CANCELLED)) {
                    load.unprotectAddLoadJob(job);
                }
            }
        }

        // delete jobs
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_11) {
            jobSize = dis.readInt();
            checksum ^= jobSize;
            for (int i = 0; i < jobSize; i++) {
                long dbId = dis.readLong();
                checksum ^= dbId;

                int deleteCount = dis.readInt();
                checksum ^= deleteCount;
                for (int j = 0; j < deleteCount; j++) {
                    DeleteInfo deleteInfo = new DeleteInfo();
                    deleteInfo.readFields(dis);
                    long currentTimeMs = System.currentTimeMillis();

                    // Delete the history delete jobs that are older than
                    // LABEL_KEEP_MAX_MS
                    if ((currentTimeMs - deleteInfo.getCreateTimeMs()) / 1000 <= Config.label_keep_max_second) {
                        load.unprotectAddDeleteInfo(deleteInfo);
                    }
                }
            }
        }

        // load error hub info
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_24) {
            LoadErrorHub.Param param = new LoadErrorHub.Param();
            param.readFields(dis);
            load.setLoadErrorHubInfo(param);
        }

        return checksum;
    }

    public long loadExportJob(DataInputStream dis, long checksum) throws IOException, DdlException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_32) {
            int size = dis.readInt();
            checksum ^= size;
            for (int i = 0; i < size; ++i) {
                long jobId = dis.readLong();
                checksum ^= jobId;
                ExportJob job = new ExportJob();
                job.readFields(dis);
                exportMgr.unprotectAddJob(job);
            }
        }

        return checksum;
    }

    public long loadAlterJob(DataInputStream dis, long checksum) throws IOException {
        for (JobType type : JobType.values()) {
            if (type == JobType.DECOMMISSION_BACKEND) {
                if (Catalog.getCurrentCatalogJournalVersion() >= 5) {
                    checksum = loadAlterJob(dis, checksum, type);
                }
            } else {
                checksum = loadAlterJob(dis, checksum, type);
            }
        }
        return checksum;
    }

    public long loadAlterJob(DataInputStream dis, long checksum, JobType type) throws IOException {
        Map<Long, AlterJob> alterJobs = null;
        List<AlterJob> finishedOrCancelledAlterJobs = null;
        if (type == JobType.ROLLUP) {
            alterJobs = this.getRollupHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getRollupHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        } else if (type == JobType.SCHEMA_CHANGE) {
            alterJobs = this.getSchemaChangeHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getSchemaChangeHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        } else if (type == JobType.DECOMMISSION_BACKEND) {
            alterJobs = this.getClusterHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getClusterHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        }


        // alter jobs
        int size = dis.readInt();
        checksum ^= size;
        for (int i = 0; i < size; i++) {
            long tableId = dis.readLong();
            checksum ^= tableId;
            AlterJob job = AlterJob.read(dis);
            alterJobs.put(tableId, job);

            // init job
            Database db = getDb(job.getDbId());
            if (db != null) {
                job.unprotectedReplayInitJob(db);
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= 2) {
            // finished or cancelled jobs
            long currentTimeMs = System.currentTimeMillis();
            size = dis.readInt();
            checksum ^= size;
            for (int i = 0; i < size; i++) {
                long tableId = dis.readLong();
                checksum ^= tableId;
                AlterJob job = AlterJob.read(dis);
                if ((currentTimeMs - job.getCreateTimeMs()) / 1000 <= Config.label_keep_max_second) {
                    // delete history jobs
                    finishedOrCancelledAlterJobs.add(job);
                }
            }
        }

        return checksum;
    }

    public long loadBackupAndRestoreJob(DataInputStream dis, long checksum) throws IOException {
        if (getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
            checksum = loadBackupAndRestoreJob(dis, checksum, BackupJob.class);
            checksum = loadBackupAndRestoreJob(dis, checksum, RestoreJob.class);
            checksum = loadBackupAndRestoreLabel(dis, checksum);
        }
        return checksum;
    }

    private long loadBackupAndRestoreJob(DataInputStream dis, long checksum,
            Class<? extends AbstractBackupJob> jobClass) throws IOException {
        Map<Long, AbstractBackupJob> jobs = null;
        List<AbstractBackupJob> finishedOrCancelledJobs = null;
        if (jobClass == BackupJob.class) {
            jobs = getBackupHandler().unprotectedGetBackupJobs();
            finishedOrCancelledJobs = getBackupHandler().unprotectedGetFinishedOrCancelledBackupJobs();
        } else if (jobClass == RestoreJob.class) {
            jobs = getBackupHandler().unprotectedGetRestoreJobs();
            finishedOrCancelledJobs = getBackupHandler().unprotectedGetFinishedOrCancelledRestoreJobs();
        } else {
            Preconditions.checkState(false);
        }

        int size = dis.readInt();
        checksum ^= size;
        for (int i = 0; i < size; i++) {
            long dbId = dis.readLong();
            checksum ^= dbId;
            if (jobClass == BackupJob.class) {
                BackupJob job = new BackupJob();
                job.readFields(dis);
                jobs.put(dbId, job);
            } else {
                RestoreJob job = new RestoreJob();
                job.readFields(dis);
                jobs.put(dbId, job);
            }
            LOG.debug("put {} job to map", dbId);
        }

        // finished or cancelled
        size = dis.readInt();
        checksum ^= size;
        for (int i = 0; i < size; i++) {
            long dbId = dis.readLong();
            checksum ^= dbId;
            if (jobClass == BackupJob.class) {
                BackupJob job = new BackupJob();
                job.readFields(dis);
                finishedOrCancelledJobs.add(job);
            } else {
                RestoreJob job = new RestoreJob();
                job.readFields(dis);
                finishedOrCancelledJobs.add(job);
            }
        }

        return checksum;
    }

    private long loadBackupAndRestoreLabel(DataInputStream dis, long checksum) throws IOException {
        int size = dis.readInt();
        checksum ^= size;

        Multimap<Long, String> dbIdtoLabels = getBackupHandler().unprotectedGetDbIdToLabels();

        for (int i = 0; i < size; i++) {
            long dbId = dis.readLong();
            checksum ^= dbId;
            String label = Text.readString(dis);
            dbIdtoLabels.put(dbId, label);
        }
        return checksum;
    }

    public long loadAccessService(DataInputStream dis, long checksum) throws IOException {
        int size = dis.readInt();
        checksum ^= size;
        userPropertyMgr.readFields(dis);
        return checksum;
    }

    public long loadRecycleBin(DataInputStream dis, long checksum) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_10) {
            Catalog.getCurrentRecycleBin().readFields(dis);

            if (!isCheckpointThread()) {
                // add tablet in Recyclebin to TabletInvertedIndex
                Catalog.getCurrentRecycleBin().addTabletToInvertedIndex();
            }
        }
        return checksum;
    }

    // Only called by checkpoint thread
    public void saveImage() throws IOException {
        // Write image.ckpt
        Storage storage = new Storage(IMAGE_DIR);
        File curFile = storage.getImageFile(replayedJournalId);
        File ckpt = new File(IMAGE_DIR, Storage.IMAGE_NEW);
        saveImage(ckpt, replayedJournalId);

        // Move image.ckpt to image.dataVersion
        LOG.info("Move " + ckpt.getAbsolutePath() + " to " + curFile.getAbsolutePath());
        if (!ckpt.renameTo(curFile)) {
            curFile.delete();
            throw new IOException();
        }
    }

    public void saveImage(File curFile, long replayedJournalId) throws IOException {
        if (!curFile.exists()) {
            curFile.createNewFile();
        }

        LOG.info("start save image to {}. is ckpt: {}", curFile.getAbsolutePath(), Catalog.isCheckpointThread());

        long checksum = 0;
        long saveImageStartTime = System.currentTimeMillis();
        readLock();
        try {
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(curFile));
            try {
                checksum = saveHeader(dos, replayedJournalId, checksum);
                checksum = saveMasterInfo(dos, checksum);
                checksum = saveFrontends(dos, checksum);
                checksum = Catalog.getCurrentSystemInfo().saveBackends(dos, checksum);
                checksum = saveDb(dos, checksum);
                checksum = saveLoadJob(dos, checksum);
                checksum = saveAlterJob(dos, checksum);
                checksum = saveBackupAndRestoreJob(dos, checksum);
                checksum = saveAccessService(dos, checksum);
                checksum = saveRecycleBin(dos, checksum);
                checksum = saveGlobalVariable(dos, checksum);
                checksum = saveCluster(dos, checksum);
                checksum = saveBrokers(dos, checksum);
                checksum = saveExportJob(dos, checksum);
                dos.writeLong(checksum);
            } finally {
                dos.close();
            }
        } finally {
            readUnlock();
        }

        long saveImageEndTime = System.currentTimeMillis();
        LOG.info("finished save image in {} ms. checksum is {}", (saveImageEndTime - saveImageStartTime), checksum);
    }

    public long saveHeader(DataOutputStream dos, long replayedJournalId, long checksum) throws IOException {
        // Write meta version
        checksum ^= FeConstants.meta_version;
        dos.writeInt(FeConstants.meta_version);

        // Write replayed journal id
        checksum ^= replayedJournalId;
        dos.writeLong(replayedJournalId);

        // Write id
        long id = nextId.getAndIncrement();
        checksum ^= id;
        dos.writeLong(id);

        dos.writeBoolean(isDefaultClusterCreated);

        return checksum;
    }

    public long saveMasterInfo(DataOutputStream dos, long checksum) throws IOException {
        Text.writeString(dos, masterIp);

        checksum ^= masterRpcPort;
        dos.writeInt(masterRpcPort);

        checksum ^= masterHttpPort;
        dos.writeInt(masterHttpPort);

        return checksum;
    }

    public long saveFrontends(DataOutputStream dos, long checksum) throws IOException {
        int size = frontends.size();
        checksum ^= size;

        dos.writeInt(size);
        for (Frontend fe : frontends) {
            fe.write(dos);
        }

        size = removedFrontends.size();
        checksum ^= size;

        dos.writeInt(size);
        for (Frontend fe : removedFrontends) {
            fe.write(dos);
        }

        return checksum;
    }

    public long saveDb(DataOutputStream dos, long checksum) throws IOException {
        int dbCount = idToDb.size() - nameToCluster.keySet().size();
        checksum ^= dbCount;
        dos.writeInt(dbCount);
        for (Map.Entry<Long, Database> entry : idToDb.entrySet()) {
            long dbId = entry.getKey();
            if (dbId >= NEXT_ID_INIT_VALUE) {
                checksum ^= dbId;
                Database db = entry.getValue();
                db.readLock();
                try {
                    db.write(dos);
                } finally {
                    db.readUnlock();
                }
            }
        }
        return checksum;
    }

    public long saveLoadJob(DataOutputStream dos, long checksum) throws IOException {
        // 1. save load.dbToLoadJob
        Map<Long, List<LoadJob>> dbToLoadJob = load.getDbToLoadJobs();
        int jobSize = dbToLoadJob.size();
        checksum ^= jobSize;
        dos.writeInt(jobSize);
        for (Entry<Long, List<LoadJob>> entry : dbToLoadJob.entrySet()) {
            long dbId = entry.getKey();
            checksum ^= dbId;
            dos.writeLong(dbId);

            List<LoadJob> loadJobs = entry.getValue();
            int loadJobCount = loadJobs.size();
            checksum ^= loadJobCount;
            dos.writeInt(loadJobCount);
            for (LoadJob job : loadJobs) {
                job.write(dos);
            }
        }

        // 2. save delete jobs
        Map<Long, List<DeleteInfo>> dbToDeleteInfos = load.getDbToDeleteInfos();
        jobSize = dbToDeleteInfos.size();
        checksum ^= jobSize;
        dos.writeInt(jobSize);
        for (Entry<Long, List<DeleteInfo>> entry : dbToDeleteInfos.entrySet()) {
            long dbId = entry.getKey();
            checksum ^= dbId;
            dos.writeLong(dbId);

            List<DeleteInfo> deleteInfos = entry.getValue();
            int deletInfoCount = deleteInfos.size();
            checksum ^= deletInfoCount;
            dos.writeInt(deletInfoCount);
            for (DeleteInfo deleteInfo : deleteInfos) {
                deleteInfo.write(dos);
            }
        }

        // 3. load error hub info
        LoadErrorHub.Param param = load.getLoadErrorHubInfo();
        param.write(dos);

        return checksum;
    }

    public long saveExportJob(DataOutputStream dos, long checksum) throws IOException {
        Map<Long, ExportJob> idToJob = exportMgr.getIdToJob();
        int size = idToJob.size();
        checksum ^= size;
        dos.writeInt(size);
        for (ExportJob job : idToJob.values()) {
            long jobId = job.getId();
            checksum ^= jobId;
            dos.writeLong(jobId);
            job.write(dos);
        }

        return checksum;
    }

    public long saveAlterJob(DataOutputStream dos, long checksum) throws IOException {
        for (JobType type : JobType.values()) {
            checksum = saveAlterJob(dos, checksum, type);
        }
        return checksum;
    }

    public long saveAlterJob(DataOutputStream dos, long checksum, JobType type) throws IOException {
        Map<Long, AlterJob> alterJobs = null;
        List<AlterJob> finishedOrCancelledAlterJobs = null;
        if (type == JobType.ROLLUP) {
            alterJobs = this.getRollupHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getRollupHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        } else if (type == JobType.SCHEMA_CHANGE) {
            alterJobs = this.getSchemaChangeHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getSchemaChangeHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        } else if (type == JobType.DECOMMISSION_BACKEND) {
            alterJobs = this.getClusterHandler().unprotectedGetAlterJobs();
            finishedOrCancelledAlterJobs = this.getClusterHandler().unprotectedGetFinishedOrCancelledAlterJobs();
        }

        // alter jobs
        int size = alterJobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (Entry<Long, AlterJob> entry : alterJobs.entrySet()) {
            long tableId = entry.getKey();
            checksum ^= tableId;
            dos.writeLong(tableId);
            entry.getValue().write(dos);
        }

        // finished or cancelled jobs
        size = finishedOrCancelledAlterJobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
            long tableId = alterJob.getTableId();
            checksum ^= tableId;
            dos.writeLong(tableId);
            alterJob.write(dos);
        }

        return checksum;
    }

    private long saveBackupAndRestoreJob(DataOutputStream dos, long checksum) throws IOException {
        checksum = saveBackupAndRestoreJob(dos, checksum, BackupJob.class);
        checksum = saveBackupAndRestoreJob(dos, checksum, RestoreJob.class);
        checksum = saveBackupAndRestoreLabel(dos, checksum);
        return checksum;
    }

    private long saveBackupAndRestoreJob(DataOutputStream dos, long checksum,
            Class<? extends AbstractBackupJob> jobClass) throws IOException {
        Map<Long, AbstractBackupJob> jobs = null;
        List<AbstractBackupJob> finishedOrCancelledJobs = null;
        if (jobClass == BackupJob.class) {
            jobs = getBackupHandler().unprotectedGetBackupJobs();
            finishedOrCancelledJobs = getBackupHandler().unprotectedGetFinishedOrCancelledBackupJobs();
        } else if (jobClass == RestoreJob.class) {
            jobs = getBackupHandler().unprotectedGetRestoreJobs();
            finishedOrCancelledJobs = getBackupHandler().unprotectedGetFinishedOrCancelledRestoreJobs();
        } else {
            Preconditions.checkState(false);
        }

        // jobs
        int size = jobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (Entry<Long, AbstractBackupJob> entry : jobs.entrySet()) {
            long dbId = entry.getKey();
            checksum ^= dbId;
            dos.writeLong(dbId);
            entry.getValue().write(dos);
            LOG.debug("save {} job", dbId);
        }

        // finished or cancelled jobs
        size = finishedOrCancelledJobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (AbstractBackupJob job : finishedOrCancelledJobs) {
            long dbId = job.getDbId();
            checksum ^= dbId;
            dos.writeLong(dbId);
            job.write(dos);
        }

        return checksum;
    }

    private long saveBackupAndRestoreLabel(DataOutputStream dos, long checksum) throws IOException {
        Multimap<Long, String> dbIdtoLabels = getBackupHandler().unprotectedGetDbIdToLabels();
        Collection<Map.Entry<Long, String>> entries = dbIdtoLabels.entries();
        int size = entries.size();
        checksum ^= size;
        dos.writeInt(size);
        for (Map.Entry<Long, String> entry : entries) {
            long dbId = entry.getKey();
            String label = entry.getValue();
            checksum ^= dbId;
            dos.writeLong(dbId);
            Text.writeString(dos, label);
        }

        return checksum;
    }

    public long saveAccessService(DataOutputStream dos, long checksum) throws IOException {
        int size = userPropertyMgr.getUserMapSize();
        checksum ^= size;
        dos.writeInt(size);
        userPropertyMgr.write(dos);
        return checksum;
    }

    public long saveRecycleBin(DataOutputStream dos, long checksum) throws IOException {
        CatalogRecycleBin recycleBin = Catalog.getCurrentRecycleBin();
        recycleBin.write(dos);
        return checksum;
    }

    // global variable persistence
    public long loadGlobalVariable(DataInputStream in, long checksum) throws IOException, DdlException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
            VariableMgr.read(in);
        }
        return checksum;
    }

    public long saveGlobalVariable(DataOutputStream out, long checksum) throws IOException {
        VariableMgr.write(out);
        return checksum;
    }

    public void replayGlobalVariable(SessionVariable variable) throws IOException, DdlException {
        VariableMgr.replayGlobalVariable(variable);
    }

    public void createCleaner() {
        cleaner = new Daemon() {
            protected void runOneCycle() {
                load.removeOldLoadJobs();
                load.removeOldDeleteJobs();
                exportMgr.removeOldExportJobs();
            }
        };
    }

    public void createReplayer() {
        if (isMaster) {
            return;
        }

        replayer = new Daemon() {
            protected void runOneCycle() {
                boolean err = false;
                boolean hasLog = false;
                try {
                    hasLog = replayJournal(-1);
                    metaReplayState.setOk();
                } catch (InsufficientLogException insufficientLogEx) {
                    // Copy the missing log files from a member of the
                    // replication group who owns the files
                    LOG.warn("catch insufficient log exception. please restart.", insufficientLogEx);
                    NetworkRestore restore = new NetworkRestore();
                    NetworkRestoreConfig config = new NetworkRestoreConfig();
                    config.setRetainLogFiles(false);
                    restore.execute(insufficientLogEx, config);
                    System.exit(-1);
                } catch (Throwable e) {
                    LOG.error("replayer thread catch an exception when replay journal.", e);
                    metaReplayState.setException(e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        LOG.error("sleep got exception. ", e);
                    }
                    err = true;
                }

                setCanRead(hasLog, err);
            }
        };
    }

    private void setCanRead(boolean hasLog, boolean err) {
        if (err) {
            canRead = false;
            return;
        }

        if (Config.ignore_meta_check) {
            canRead = true;
            return;
        }

        long currentTimeMs = System.currentTimeMillis();
        if (currentTimeMs - synchronizedTimeMs > Config.meta_delay_toleration_second * 1000) {
            // we stll need this log to observe this situation
            // but service may be continued when there is no log being replayed.
            LOG.warn("meta out of date. current time: {}, synchronized time: {}, has log: {}, fe type: {}",
                    currentTimeMs, synchronizedTimeMs, hasLog, feType);
            if (hasLog || (!hasLog && feType == FrontendNodeType.UNKNOWN)) {
                // 1. if we read log from BDB, which means master is still
                // alive.
                // So we need to set meta out of date.
                // 2. if we didn't read any log from BDB and feType is UNKNOWN,
                // which means this non-master node is disconnected with master.
                // So we need to set meta out of date either.
                metaReplayState.setOutOfDate(currentTimeMs, synchronizedTimeMs);
                canRead = false;
            }
        } else {
            canRead = true;
        }
    }

    public void createStateListener() {
        listener = new Daemon() {
            protected void runOneCycle() {
                if (formerFeType == feType) {
                    return;
                }

                if (formerFeType == FrontendNodeType.INIT) {
                    switch (feType) {
                    case MASTER: {
                        try {
                            transferToMaster();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                    case UNKNOWN:
                    case FOLLOWER:
                    case OBSERVER: {
                        transferToNonMaster();
                        break;
                    }
                    default:
                        break;
                    }
                    return;
                }

                if (formerFeType == FrontendNodeType.UNKNOWN) {
                    switch (feType) {
                    case MASTER: {
                        try {
                            transferToMaster();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                    case FOLLOWER:
                    case OBSERVER: {
                        transferToNonMaster();
                        break;
                    }
                    default:
                    }
                    return;
                }

                if (formerFeType == FrontendNodeType.FOLLOWER) {
                    switch (feType) {
                    case MASTER: {
                        try {
                            transferToMaster();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                    case UNKNOWN:
                    case OBSERVER: {
                        transferToNonMaster();
                        break;
                    }
                    default:
                    }
                    return;
                }

                if (formerFeType == FrontendNodeType.OBSERVER) {
                    switch (feType) {
                    case UNKNOWN: {
                        transferToNonMaster();
                    }
                    default:
                    }
                    return;
                }

                if (formerFeType == FrontendNodeType.MASTER) {
                    switch (feType) {
                    case UNKNOWN:
                    case FOLLOWER:
                    case OBSERVER: {
                        System.exit(-1);
                    }
                    default:
                    }
                    return;
                }
            }
        };
    }

    public synchronized boolean replayJournal(long toJournalId) {
        if (toJournalId == -1) {
            toJournalId = getMaxJournalId();
        }
        if (toJournalId <= replayedJournalId) {
            return false;
        }

        LOG.info("replayed journal id is {}, replay to journal id is {}", replayedJournalId, toJournalId);
        JournalCursor cursor = editLog.read(replayedJournalId + 1, toJournalId);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", replayedJournalId + 1, toJournalId);
            return false;
        }

        long startTime = System.currentTimeMillis();
        boolean hasLog = false;
        while (true) {
            JournalEntity entity = cursor.next();
            if (entity == null) {
                break;
            }
            hasLog = true;
            EditLog.loadJournal(this, entity);
            replayedJournalId++;
            LOG.debug("journal {} replayed.", replayedJournalId);
            if (!isMaster) {
                journalObservable.notifyObservers(replayedJournalId);
            }
        }
        long cost = System.currentTimeMillis() - startTime;
        if (cost >= 1000) {
            LOG.warn("replay journal cost too much time: {} replayedJournalId: {}", cost, replayedJournalId);
        }

        return hasLog;
    }

    public void createTimePrinter() {
        if (!isMaster) {
            return;
        }

        timePrinter = new Daemon() {
            protected void runOneCycle() {
                if (canWrite) {
                    Timestamp stamp = new Timestamp();
                    editLog.logTimestamp(stamp);
                }
            }
        };
    }

    public void addFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        writeLock();
        try {
            Frontend fe = checkFeExist(host, port);
            if (fe != null) {
                throw new DdlException("frontend already exists " + fe);
            }
            fe = checkFeRemoved(host, port);
            if (fe != null) {
                throw new DdlException("frontend already removed " + fe + " can not be added again. "
                        + "try to use a different host or port");
            }
            fe = new Frontend(role, host, port);
            frontends.add(fe);
            editLog.logAddFrontend(fe);
        } finally {
            writeUnlock();
        }
    }

    public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        if (host.equals(selfNode.first) && port == selfNode.second && isMaster) {
            throw new DdlException("can not drop current master node.");
        }
        writeLock();
        try {
            Frontend fe = checkFeExist(host, port);
            if (fe == null) {
                throw new DdlException("frontend does not exist[" + host + ":" + port + "]");
            }
            if (fe.getRole() != role) {
                throw new DdlException(role.toString() + " does not exist[" + host + ":" + port + "]");
            }
            frontends.remove(fe);
            removedFrontends.add(fe);
            if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                haProtocol.removeElectableNode(fe.getHost() + "_" + fe.getPort());
            }
            editLog.logRemoveFrontend(fe);
        } finally {
            writeUnlock();
        }
    }

    public Frontend checkFeExist(String host, int port) {
        for (Frontend fe : frontends) {
            if (fe.getHost().equals(host) && fe.getPort() == port) {
                return fe;
            }
        }
        return null;
    }

    public Frontend checkFeRemoved(String host, int port) {
        for (Frontend fe : removedFrontends) {
            if (fe.getHost().equals(host) && fe.getPort() == port) {
                return fe;
            }
        }
        return null;
    }

    // The interface which DdlExecutor needs.
    public void createDb(CreateDbStmt stmt) throws DdlException {
        final String clusterName = stmt.getClusterName();
        String dbName = stmt.getDbName();
        long id = 0L;
        writeLock();
        try {
            if (!nameToCluster.containsKey(clusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER, clusterName);
            }
            if (nameToDb.containsKey(dbName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create database[{}] which already exists", dbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);
                }
            } else {
                id = getNextId();
                Database db = new Database(id, dbName);
                db.setClusterName(clusterName);
                unprotectCreateDb(db);
                editLog.logCreateDb(db);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("createDb dbName = " + dbName + ", id = " + id);
    }

    // For replay edit log, need't lock metadata
    public void unprotectCreateDb(Database db) {
        idToDb.put(db.getId(), db);
        nameToDb.put(db.getName(), db);
        final Cluster cluster = nameToCluster.get(db.getClusterName());
        cluster.addDb(db.getName(), db.getId());
    }

    // for test
    public void addCluster(Cluster cluster) {
        nameToCluster.put(cluster.getName(), cluster);
        idToCluster.put(cluster.getId(), cluster);
    }


    public void replayCreateDb(Database db) {
        writeLock();
        try {
            unprotectCreateDb(db);
        } finally {
            writeUnlock();
        }
    }

    public void dropDb(DropDbStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();

        // 1. check if database exists
        writeLock();
        try {
            if (!nameToDb.containsKey(dbName)) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop database[{}] which does not exist", dbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                }
            }

            // 2. drop tables in db
            Database db = this.nameToDb.get(dbName);
            db.writeLock();
            try {

                if (db.getDbState() == DbState.LINK && dbName.equals(db.getAttachDb())) {
                    final DropLinkDbAndUpdateDbInfo info = new DropLinkDbAndUpdateDbInfo();
                    nameToDb.remove(db.getAttachDb());
                    db.setDbState(DbState.NORMAL);
                    info.setUpdateDbState(DbState.NORMAL);
                    final Cluster cluster = nameToCluster
                            .get(ClusterNamespace.getClusterNameFromFullName(db.getAttachDb()));
                    final BaseParam param = new BaseParam();
                    param.addStringParam(db.getAttachDb());
                    param.addLongParam(db.getId());
                    cluster.removeLinkDb(param);
                    info.setDropDbCluster(cluster.getName());
                    info.setDropDbId(db.getId());
                    info.setDropDbName(db.getAttachDb());
                    editLog.logDropLinkDb(info);
                    return;
                }

                if (dbName.equals(db.getName()) && db.getDbState() == DbState.LINK) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                            ClusterNamespace.getDbNameFromFullName(dbName));
                    return;
                }

                if (dbName.equals(db.getAttachDb()) && db.getDbState() == DbState.MOVE) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                            ClusterNamespace.getDbNameFromFullName(dbName));
                    return;
                }

                // save table names for recycling
                Set<String> tableNames = db.getTableNamesWithLock();
                unprotectDropDb(db);
                Catalog.getCurrentRecycleBin().recycleDatabase(db, tableNames);
            } finally {
                db.writeUnlock();
            }

            // 3. remove db from catalog
            idToDb.remove(db.getId());
            nameToDb.remove(db.getName());
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(dbName, db.getId());
            editLog.logDropDb(dbName);
        } finally {
            writeUnlock();
        }

        LOG.info("finish drop database[{}]", dbName);
    }

    public void unprotectDropDb(Database db) {
        for (Table table : db.getTables()) {
            unprotectDropTable(db, table.getId());
        }
    }

    public void replayDropLinkDb(DropLinkDbAndUpdateDbInfo info) {
        writeLock();
        try {
            final Database db = this.nameToDb.remove(info.getDropDbName());
            db.setDbState(info.getUpdateDbState());
            final Cluster cluster = nameToCluster
                    .get(info.getDropDbCluster());
            final BaseParam param = new BaseParam();
            param.addStringParam(db.getAttachDb());
            param.addLongParam(db.getId());
            cluster.removeLinkDb(param);
        } finally {
            writeUnlock();
        }
    }

    public void replayDropDb(String dbName) throws DdlException {
        writeLock();
        try {
            Database db = nameToDb.get(dbName);
            db.writeLock();
            try {
                Set<String> tableNames = db.getTableNamesWithLock();
                unprotectDropDb(db);
                Catalog.getCurrentRecycleBin().recycleDatabase(db, tableNames);
            } finally {
                db.writeUnlock();
            }

            nameToDb.remove(dbName);
            idToDb.remove(db.getId());
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(dbName, db.getId());
        } finally {
            writeUnlock();
        }
    }

    public void recoverDatabase(RecoverDbStmt recoverStmt) throws DdlException {
        // check is new db with same name already exist
        if (getDb(recoverStmt.getDbName()) != null) {
            throw new DdlException("Database[" + recoverStmt.getDbName() + "] already exist.");
        }

        Database db = Catalog.getCurrentRecycleBin().recoverDatabase(recoverStmt.getDbName());

        // add db to catalog
        writeLock();
        try {
            if (nameToDb.containsKey(db.getName())) {
                throw new DdlException("Database[" + db.getName() + "] already exist.");
                // it's ok that we do not put db back to CatalogRecycleBin
                // cause this db cannot recover any more
            }

            nameToDb.put(db.getName(), db);
            idToDb.put(db.getId(), db);
        } finally {
            writeUnlock();
        }

        // log
        RecoverInfo recoverInfo = new RecoverInfo(db.getId(), -1L, -1L);
        Catalog.getInstance().getEditLog().logRecoverDb(recoverInfo);
        LOG.info("recover database[{}]", db.getId());
    }

    public void recoverTable(RecoverTableStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table != null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }

            if (!Catalog.getCurrentRecycleBin().recoverTable(db, tableName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("table[" + tableName + "] is not OLAP table");
            }
            OlapTable olapTable = (OlapTable) table;

            String partitionName = recoverStmt.getPartitionName();
            if (olapTable.getPartition(partitionName) != null) {
                throw new DdlException("partition[" + partitionName + "] already exist in table[" + tableName + "]");
            }

            Catalog.getCurrentRecycleBin().recoverPartition(db.getId(), olapTable, partitionName);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayEraseDatabase(long dbId) throws DdlException {
        Catalog.getCurrentRecycleBin().replayEraseDatabase(dbId);
    }

    public void replayRecoverDatabase(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = Catalog.getCurrentRecycleBin().replayRecoverDatabase(dbId);

        // add db to catalog
        replayCreateDb(db);

        LOG.info("replay recover db[{}]", dbId);
    }

    public void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        db.setDataQuotaWithLock(stmt.getQuota());

        DatabaseInfo dbInfo = new DatabaseInfo(dbName, "", db.getDataQuota());
        editLog.logAlterDb(dbInfo);
    }

    public void replayAlterDatabaseQuota(String dbName, long quota) {
        Database db = getDb(dbName);
        Preconditions.checkNotNull(db);
        db.setDataQuotaWithLock(quota);
    }

    public void renameDatabase(AlterDatabaseRename stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String newDbName = stmt.getNewDbName();
        String clusterName = stmt.getClusterName();

        if (dbName.equals(newDbName)) {
            throw new DdlException("Same database name");
        }

        Database db = null;
        Cluster cluster = null;
        writeLock();
        try {
            cluster = nameToCluster.get(clusterName);
            if (cluster == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
            }
            final String dbNameWithoutPrefix = ClusterNamespace.getDbNameFromFullName(dbName);
            // check if db exists
            db = nameToDb.get(dbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbNameWithoutPrefix);
            }

            if (db.getDbState() == DbState.LINK || db.getDbState() == DbState.MOVE) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_RENAME_DB_ERR, dbNameWithoutPrefix);
            }
            final String newDbNameWithoutPrefix = ClusterNamespace.getDbNameFromFullName(newDbName);
            // check if name is already used
            if (nameToDb.get(newDbName) != null) {
                throw new DdlException("Database name[" + newDbNameWithoutPrefix + "] is already used");
            }

            cluster.removeDb(db.getName(), db.getId());
            cluster.addDb(newDbName, db.getId());
            // 1. rename db
            db.setNameWithLock(newDbName);

            // 2. add to meta. check again
            nameToDb.remove(dbName);
            nameToDb.put(newDbName, db);

            DatabaseInfo dbInfo = new DatabaseInfo(dbName, newDbName, -1L);
            editLog.logDatabaseRename(dbInfo);
        } finally {
            writeUnlock();
        }

        LOG.info("rename database[{}] to [{}]", dbName, newDbName);
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        writeLock();
        try {
            Database db = getDb(dbName);
            db.setNameWithLock(newDbName);
            db = nameToDb.get(dbName);
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(db.getName(), db.getId());
            cluster.addDb(newDbName, db.getId());
            nameToDb.remove(dbName);
            nameToDb.put(newDbName, db);
        } finally {
            writeUnlock();
        }

        LOG.info("replay rename database[{}] to {}", dbName, newDbName);
    }

    public void createTable(CreateTableStmt stmt) throws DdlException {
        createTable(stmt, false);
    }

    public Table createTable(CreateTableStmt stmt, boolean isRestore) throws DdlException {
        String engineName = stmt.getEngineName();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check if db exists
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check cluster capacity
        Catalog.getCurrentSystemInfo().checkCapacity();
        // check db quota
        db.checkQuota();

        // check if table exists in db
        if (!isRestore) {
            db.readLock();
            try {
                if (db.getTable(tableName) != null) {
                    if (stmt.isSetIfNotExists()) {
                        LOG.info("create table[{}] which already exists", tableName);
                        return db.getTable(tableName);
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                    }
                }
            } finally {
                db.readUnlock();
            }
        }

        if (engineName.equals("olap")) {
            return createOlapTable(db, stmt, isRestore);
        } else if (engineName.equals("mysql")) {
            return createMysqlTable(db, stmt, isRestore);
        } else if (engineName.equals("kudu")) {
            return createKuduTable(db, stmt);
        } else if (engineName.equals("broker")) {
            return createBrokerTable(db, stmt, isRestore);
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, engineName);
        }
        Preconditions.checkState(false);
        return null;
    }

    public void addPartition(Database db, String tableName, AddPartitionClause addPartitionClause) throws DdlException {
        addPartition(db, tableName, null, addPartitionClause, false);
    }

    public Pair<Long, Partition> addPartition(Database db, String tableName, OlapTable givenTable,
            AddPartitionClause addPartitionClause, boolean isRestore) throws DdlException {
        SingleRangePartitionDesc singlePartitionDesc = addPartitionClause.getSingeRangePartitionDesc();
        DistributionDesc distributionDesc = addPartitionClause.getDistributionDesc();

        DistributionInfo distributionInfo = null;
        OlapTable olapTable = null;

        Map<Long, List<Column>> indexIdToSchema = null;
        Map<Long, Integer> indexIdToSchemaHash = null;
        Map<Long, Short> indexIdToShortKeyColumnCount = null;
        Map<Long, TStorageType> indexIdToStorageType = null;
        Set<String> bfColumns = null;

        String partitionName = singlePartitionDesc.getPartitionName();

        Pair<Long, Long> versionInfo = null;
        // check
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (givenTable == null) {
                if (table == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }
            } else {
                table = givenTable;
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Table[" + tableName + "] is not OLAP table");
            }

            // check state
            olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTableState.NORMAL && !isRestore) {
                throw new DdlException("Table[" + tableName + "]'s state is not NORMAL");
            }

            // check partition type
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo.getType() != PartitionType.RANGE) {
                throw new DdlException("Only support adding partition to range partitioned table");
            }

            // check partition name
            if (olapTable.getPartition(partitionName) != null) {
                if (singlePartitionDesc.isSetIfNotExists()) {
                    LOG.info("add partition[{}] which already exists", partitionName);
                    return null;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_SAME_NAME_PARTITION, partitionName);
                }
            }

            Map<String, String> properties = singlePartitionDesc.getProperties();
            versionInfo = PropertyAnalyzer.analyzeVersionInfo(properties);

            // check range
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            // here we check partition's properties
            singlePartitionDesc.analyze(rangePartitionInfo.getPartitionColumns().size(), null);

            if (!isRestore) {
                rangePartitionInfo.checkAndCreateRange(singlePartitionDesc);
            }

            // get distributionInfo
            List<Column> baseSchema = olapTable.getBaseSchema();
            DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
            if (distributionDesc != null) {
                distributionInfo = distributionDesc.toDistributionInfo(baseSchema);
                // for now. we only support modify distribution's bucket num
                if (distributionInfo.getType() != defaultDistributionInfo.getType()) {
                    throw new DdlException("Cannot assign different distribution type. default is: "
                            + defaultDistributionInfo.getType());
                }

                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    List<Column> newDistriCols = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
                    List<Column> defaultDistriCols = ((HashDistributionInfo) defaultDistributionInfo)
                            .getDistributionColumns();
                    if (!newDistriCols.equals(defaultDistriCols)) {
                        throw new DdlException("Cannot assign hash distribution with different distribution cols. "
                                + "default is: " + defaultDistriCols);
                    }
                }

            } else {
                distributionInfo = defaultDistributionInfo;
            }

            indexIdToShortKeyColumnCount = olapTable.getCopiedIndexIdToShortKeyColumnCount();
            indexIdToSchemaHash = olapTable.getCopiedIndexIdToSchemaHash();
            indexIdToStorageType = olapTable.getCopiedIndexIdToStorageType();
            indexIdToSchema = olapTable.getCopiedIndexIdToSchema();
            bfColumns = olapTable.getCopiedBfColumns();

        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        } finally {
            db.readUnlock();
        }

        Preconditions.checkNotNull(distributionInfo);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkNotNull(indexIdToShortKeyColumnCount);
        Preconditions.checkNotNull(indexIdToSchemaHash);
        Preconditions.checkNotNull(indexIdToStorageType);
        Preconditions.checkNotNull(indexIdToSchema);

        // create partition without lock
        DataProperty dataProperty = singlePartitionDesc.getPartitionDataProperty();
        Preconditions.checkNotNull(dataProperty);

        Set<Long> tabletIdSet = new HashSet<Long>();
        try {
            long partitionId = getNextId();
            Partition partition = createPartitionWithIndices(db.getClusterName(), db.getId(),
                                                             olapTable.getId(),
                                                             partitionId, partitionName,
                                                             indexIdToShortKeyColumnCount,
                                                             indexIdToSchemaHash,
                                                             indexIdToStorageType,
                                                             indexIdToSchema,
                                                             olapTable.getKeysType(),
                                                             distributionInfo,
                                                             dataProperty.getStorageMedium(),
                                                             singlePartitionDesc.getReplicationNum(),
                                                             versionInfo, bfColumns, olapTable.getBfFpp(),
                                                             tabletIdSet, isRestore);

            // check again
            db.writeLock();
            try {
                Table table = db.getTable(tableName);
                if (givenTable == null) {
                    if (table == null) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                    }
                } else {
                    table = givenTable;
                }

                if (table.getType() != TableType.OLAP) {
                    throw new DdlException("Table[" + tableName + "] is not OLAP table");
                }

                // check partition type
                olapTable = (OlapTable) table;
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                if (partitionInfo.getType() != PartitionType.RANGE) {
                    throw new DdlException("Only support adding partition to range partitioned table");
                }

                // check partition name
                if (olapTable.getPartition(partitionName) != null) {
                    throw new DdlException("Partition " + partitionName + " already exists");
                }

                // check if meta changed
                // rollup index may be added or dropped during add partition op
                // schema may be changed during add partition op
                boolean metaChanged = false;
                if (olapTable.getIndexNameToId().size() != indexIdToSchema.size()) {
                    metaChanged = true;
                } else {
                    // compare schemaHash
                    for (Map.Entry<Long, Integer> entry : olapTable.getIndexIdToSchemaHash().entrySet()) {
                        long indexId = entry.getKey();
                        if (!indexIdToSchemaHash.containsKey(indexId)) {
                            metaChanged = true;
                            break;
                        }
                        if (indexIdToSchemaHash.get(indexId) != entry.getValue()) {
                            metaChanged = true;
                            break;
                        }
                    }
                }

                if (metaChanged) {
                    throw new DdlException("Table[" + tableName + "]'s meta has been changed. try again.");
                }

                if (!isRestore) {
                    // update partition info
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    rangePartitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, partitionId);

                    olapTable.addPartition(partition);

                    // log
                    PartitionPersistInfo info = new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                            rangePartitionInfo.getRange(partitionId), dataProperty,
                            rangePartitionInfo.getReplicationNum(partitionId));
                    editLog.logAddPartition(info);

                    LOG.info("succeed in creating partition[{}]", partitionId);
                    return null;
                } else {
                    // ATTN: do not add this partition to table.
                    // if add, replica info may be removed when handling tablet
                    // report,
                    // cause be does not have real data file
                    LOG.info("succeed in creating partition[{}] to restore", partitionId);
                    return new Pair<Long, Partition>(olapTable.getId(), partition);
                }
            } finally {
                db.writeUnlock();
            }
        } catch (DdlException e) {
            for (Long tabletId : tabletIdSet) {
                Catalog.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
    }

    public void replayAddPartition(PartitionPersistInfo info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            Partition partition = info.getPartition();
            olapTable.addPartition(partition);
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            ((RangePartitionInfo) partitionInfo).unprotectHandleNewSinglePartitionDesc(partition.getId(),
                    info.getRange(), info.getDataProperty(), info.getReplicationNum());

            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices()) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(), partition.getId(),
                            index.getId(), schemaHash);
                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        for (Replica replica : tablet.getReplicas()) {
                            invertedIndex.addReplica(tabletId, replica);
                        }
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void dropPartition(Database db, OlapTable olapTable, DropPartitionClause clause) throws DdlException {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());

        String partitionName = clause.getPartitionName();

        if (olapTable.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s state is not NORMAL");
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        Partition partition = olapTable.getPartition(partitionName);
        if (partition == null) {
            if (clause.isSetIfExists()) {
                LOG.info("drop partition[{}] which does not exist", partitionName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DROP_PARTITION_NON_EXISTENT, partitionName);
            }
        }

        if (partitionInfo.getType() != PartitionType.RANGE) {
            String errMsg = "Alter table [" + olapTable.getName() + "] failed. Not a partitioned table";
            LOG.warn(errMsg);
            throw new DdlException(errMsg);
        }

        // drop
        olapTable.dropPartition(db.getId(), partitionName);

        // remove clone job
        Clone clone = Catalog.getInstance().getCloneInstance();
        clone.cancelCloneJob(partition);

        // log
        DropPartitionInfo info = new DropPartitionInfo(db.getId(), olapTable.getId(), partitionName);
        editLog.logDropPartition(info);

        LOG.info("succeed in droping partition[{}]", partition.getId());
    }

    public void replayDropPartition(DropPartitionInfo info) {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            olapTable.dropPartition(info.getDbId(), info.getPartitionName());
        } finally {
            db.writeUnlock();
        }
    }

    public void replayErasePartition(long partitionId) throws DdlException {
        Catalog.getCurrentRecycleBin().replayErasePartition(partitionId);
    }

    public void replayRecoverPartition(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        db.writeLock();
        try {
            Table table = db.getTable(info.getTableId());
            Catalog.getCurrentRecycleBin().replayRecoverPartition((OlapTable) table, info.getPartitionId());
        } finally {
            db.writeUnlock();
        }
    }

    public void modifyPartition(Database db, OlapTable olapTable, ModifyPartitionClause modifyPartitionClause)
            throws DdlException {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        if (olapTable.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s state is not NORMAL");
        }

        String partitionName = modifyPartitionClause.getPartitionName();
        Partition partition = olapTable.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException(
                    "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        Map<String, String> properties = modifyPartitionClause.getProperties();

        // 1. data property
        DataProperty oldDataProperty = partitionInfo.getDataProperty(partition.getId());
        DataProperty newDataProperty = null;
        try {
            newDataProperty = PropertyAnalyzer.analyzeDataProperty(properties, oldDataProperty);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(newDataProperty);

        if (newDataProperty.equals(oldDataProperty)) {
            newDataProperty = null;
        }

        // 2. replication num
        short oldReplicationNum = partitionInfo.getReplicationNum(partition.getId());
        short newReplicationNum = (short) -1;
        try {
            newReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, oldReplicationNum);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        if (newReplicationNum == oldReplicationNum) {
            newReplicationNum = (short) -1;
        }

        // check if has other undefined properties
        if (properties != null && !properties.isEmpty()) {
            MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
            throw new DdlException("Unknown properties: " + mapJoiner.join(properties));
        }

        // modify meta here
        // date property
        if (newDataProperty != null) {
            partitionInfo.setDataProperty(partition.getId(), newDataProperty);
            LOG.debug("modify partition[{}-{}-{}] data property to {}", db.getId(), olapTable.getId(), partitionName,
                    newDataProperty.toString());
        }

        // replication num
        if (newReplicationNum != (short) -1) {
            partitionInfo.setReplicationNum(partition.getId(), newReplicationNum);
            LOG.debug("modify partition[{}-{}-{}] replication num to {}", db.getId(), olapTable.getId(), partitionName,
                    newReplicationNum);
        }

        // log
        ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), olapTable.getId(), partition.getId(),
                newDataProperty, newReplicationNum);
        editLog.logModifyPartition(info);
    }

    public void replayModifyPartition(ModifyPartitionInfo info) {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.getDataProperty() != null) {
                partitionInfo.setDataProperty(info.getPartitionId(), info.getDataProperty());
            }
            if (info.getReplicationNum() != (short) -1) {
                partitionInfo.setReplicationNum(info.getPartitionId(), info.getReplicationNum());
            }
        } finally {
            db.writeUnlock();
        }
    }

    private Partition createPartitionWithIndices(String clusterName, long dbId, long tableId,
                                                 long partitionId, String partitionName,
                                                 Map<Long, Short> indexIdToShortKeyColumnCount,
                                                 Map<Long, Integer> indexIdToSchemaHash,
                                                 Map<Long, TStorageType> indexIdToStorageType,
                                                 Map<Long, List<Column>> indexIdToSchema,
                                                 KeysType keysType,
                                                 DistributionInfo distributionInfo,
                                                 TStorageMedium storageMedium,
                                                 short replicationNum,
                                                 Pair<Long, Long> versionInfo,
                                                 Set<String> bfColumns,
                                                 double bfFpp,
                                                 Set<Long> tabletIdSet,
                                                 boolean isRestore) throws DdlException {
        // create base index first. use table id as base index id
        long baseIndexId = tableId;
        MaterializedIndex baseIndex = new MaterializedIndex(baseIndexId, IndexState.NORMAL);

        // create partition with base index
        Partition partition = new Partition(partitionId, partitionName, baseIndex, distributionInfo);

        // add to index map
        Map<Long, MaterializedIndex> indexMap = new HashMap<Long, MaterializedIndex>();
        indexMap.put(baseIndexId, baseIndex);

        // create rollup index if has
        for (long indexId : indexIdToSchema.keySet()) {
            if (indexId == baseIndexId) {
                continue;
            }

            MaterializedIndex rollup = new MaterializedIndex(indexId, IndexState.NORMAL);
            indexMap.put(indexId, rollup);
        }

        // version and version hash
        if (versionInfo != null) {
            partition.setCommittedVersion(versionInfo.first);
            partition.setCommittedVersionHash(versionInfo.second);
        }
        long version = partition.getCommittedVersion();
        long versionHash = partition.getCommittedVersionHash();

        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndex index = entry.getValue();

            // create tablets
            int schemaHash = indexIdToSchemaHash.get(indexId);
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash);
            createTablets(clusterName, index, ReplicaState.NORMAL, distributionInfo, version, versionHash,
                    replicationNum, tabletMeta, tabletIdSet);

            boolean ok = false;
            String errMsg = null;

            if (!isRestore) {
                // add create replica task for olap
                short shortKeyColumnCount = indexIdToShortKeyColumnCount.get(indexId);
                TStorageType storageType = indexIdToStorageType.get(indexId);
                List<Column> schema = indexIdToSchema.get(indexId);
                int totalTaskNum = index.getTablets().size() * replicationNum;
                MarkedCountDownLatch countDownLatch = new MarkedCountDownLatch(totalTaskNum);
                AgentBatchTask batchTask = new AgentBatchTask();
                for (Tablet tablet : index.getTablets()) {
                    long tabletId = tablet.getId();
                    for (Replica replica : tablet.getReplicas()) {
                        long backendId = replica.getBackendId();
                        countDownLatch.addMark(backendId, tabletId);
                        CreateReplicaTask task = new CreateReplicaTask(backendId, dbId, tableId,
                                                                       partitionId, indexId, tabletId,
                                                                       shortKeyColumnCount, schemaHash,
                                                                       version, versionHash,
                                                                       keysType,
                                                                       storageType, storageMedium,
                                                                       schema, bfColumns, bfFpp,
                                                                       countDownLatch);
                        batchTask.addTask(task);
                        // add to AgentTaskQueue for handling finish report.
                        // not for resending task
                        AgentTaskQueue.addTask(task);
                    }
                }
                AgentTaskExecutor.submit(batchTask);

                // estimate timeout
                long timeout = Config.tablet_create_timeout_second * 1000L * totalTaskNum;
                try {
                    ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.warn("InterruptedException: ", e);
                    ok = false;
                }

                if (!ok) {
                    errMsg = "Failed to create partition[" + partitionName + "]. Timeout";
                    // clear tasks
                    List<AgentTask> tasks = batchTask.getAllTasks();
                    for (AgentTask task : tasks) {
                        AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
                    }

                    Collection<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                    String idStr = Joiner.on(", ").join(unfinishedMarks);
                    LOG.warn("{}. unfinished marks: {}", errMsg, idStr);
                    throw new DdlException(errMsg);
                }
            } else {
                // do nothing
                // restore task will be done in RestoreJob
            }

            if (index.getId() != baseIndexId) {
                // add rollup index to partition
                partition.createRollupIndex(index);
            }
        } // end for indexMap

        return partition;
    }

    // Create olap table and related base index synchronously.
    private Table createOlapTable(Database db, CreateTableStmt stmt, boolean isRestore) throws DdlException {
        String tableName = stmt.getTableName();
        LOG.debug("begin create olap table: {}", tableName);

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        validateColumns(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo = null;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            // gen partition id first
            if (partitionDesc instanceof RangePartitionDesc) {
                RangePartitionDesc rangeDesc = (RangePartitionDesc) partitionDesc;
                for (SingleRangePartitionDesc desc : rangeDesc.getSingleRangePartitionDescs()) {
                    long partitionId = getNextId();
                    partitionNameToId.put(desc.getPartitionName(), partitionId);
                }
            }
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId);
        } else {
            long partitionId = getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        // get keys type
        KeysDesc keysDesc = stmt.getKeysDesc();
        Preconditions.checkNotNull(keysDesc);
        KeysType keysType = keysDesc.getKeysType();

        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(baseSchema);

        // calc short key column count
        short shortKeyColumnCount = Catalog.calcShortKeyColumnCount(baseSchema, stmt.getProperties());
        LOG.debug("create table[{}] short key column count: {}", tableName, shortKeyColumnCount);

        // create table
        long tableId = Catalog.getInstance().getNextId();
        OlapTable olapTable = new OlapTable(tableId, tableName, baseSchema, keysType, partitionInfo, distributionInfo);

        // set base index info to table
        // this should be done before create partition.
        // get base index storage type. default is COLUMN
        Map<String, String> properties = stmt.getProperties();
        TStorageType baseIndexStorageType = null;
        try {
            baseIndexStorageType = PropertyAnalyzer.analyzeStorageType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // check storage type if has null column
        boolean hasNullColumn = false;
        for (Column column : baseSchema) {
            if (column.isAllowNull()) {
                hasNullColumn = true;
                break;
            }
        }
        if (hasNullColumn && baseIndexStorageType != TStorageType.COLUMN) {
            throw new DdlException("Only column table support null columns");
        }

        Preconditions.checkNotNull(baseIndexStorageType);
        long baseIndexId = olapTable.getId();
        olapTable.setStorageTypeToIndex(baseIndexId, baseIndexStorageType);

        // analyze bloom filter columns
        Set<String> bfColumns = null;
        double bfFpp = 0;
        try {
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema);
            if (bfColumns != null && baseIndexStorageType == TStorageType.ROW) {
                throw new DdlException("Only column table support bloom filter index");
            }
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }

            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.default_bloom_filter_fpp;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }

            olapTable.setBloomFilterInfo(bfColumns, bfFpp);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // set index schema
        int schemaVersion = 0;
        try {
            schemaVersion = PropertyAnalyzer.analyzeSchemaVersion(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        int schemaHash = Util.schemaHash(schemaVersion, baseSchema, bfColumns, bfFpp);
        olapTable.setIndexSchemaInfo(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash,
                shortKeyColumnCount);

        // analyze version info
        Pair<Long, Long> versionInfo = null;
        try {
            versionInfo = PropertyAnalyzer.analyzeVersionInfo(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(versionInfo);

        // a set to record every new tablet created when create table
        // if failed in any step, use this set to do clear things
        Set<Long> tabletIdSet = new HashSet<Long>();

        Table returnTable = null;
        // create partition
        try {
            if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                // this is a 1-level partitioned table
                // use table name as partition name
                String partitionName = tableName;
                long partitionId = partitionNameToId.get(partitionName);

                // check data property first
                DataProperty dataProperty = null;
                try {
                    dataProperty = PropertyAnalyzer.analyzeDataProperty(stmt.getProperties(),
                                                                        DataProperty.DEFAULT_HDD_DATA_PROPERTY);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
                Preconditions.checkNotNull(dataProperty);
                partitionInfo.setDataProperty(partitionId, dataProperty);

                // analyze replication num
                short replicationNum = FeConstants.default_replication_num;
                try {
                    replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
                partitionInfo.setReplicationNum(partitionId, replicationNum);

                // create partition
                Partition partition = createPartitionWithIndices(db.getClusterName(), db.getId(),
                                                                 olapTable.getId(),
                                                                 partitionId, partitionName,
                                                                 olapTable.getIndexIdToShortKeyColumnCount(),
                                                                 olapTable.getIndexIdToSchemaHash(),
                                                                 olapTable.getIndexIdToStorageType(),
                                                                 olapTable.getIndexIdToSchema(),
                                                                 keysType,
                                                                 distributionInfo,
                                                                 dataProperty.getStorageMedium(),
                                                                 replicationNum,
                                                                 versionInfo, bfColumns, bfFpp,
                                                                 tabletIdSet, isRestore);
                olapTable.addPartition(partition);
            } else if (partitionInfo.getType() == PartitionType.RANGE) {
                try {
                    // just for remove entries in stmt.getProperties(),
                    // and then check if there still has unknown properties
                    PropertyAnalyzer.analyzeDataProperty(stmt.getProperties(), DataProperty.DEFAULT_HDD_DATA_PROPERTY);
                    PropertyAnalyzer.analyzeReplicationNum(properties, FeConstants.default_replication_num);

                    if (properties != null && !properties.isEmpty()) {
                        // here, all properties should be checked
                        throw new DdlException("Unknown properties: " + properties);
                    }
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }

                // this is a 2-level partitioned tables
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                for (Map.Entry<String, Long> entry : partitionNameToId.entrySet()) {
                    DataProperty dataProperty = rangePartitionInfo.getDataProperty(entry.getValue());
                    Partition partition = createPartitionWithIndices(db.getClusterName(), db.getId(), olapTable.getId(),
                                                                     entry.getValue(), entry.getKey(),
                                                                     olapTable.getIndexIdToShortKeyColumnCount(),
                                                                     olapTable.getIndexIdToSchemaHash(),
                                                                     olapTable.getIndexIdToStorageType(),
                                                                     olapTable.getIndexIdToSchema(),
                                                                     keysType, distributionInfo,
                                                                     dataProperty.getStorageMedium(),
                                                                     partitionInfo.getReplicationNum(entry.getValue()),
                                                                     versionInfo, bfColumns, bfFpp,
                                                                     tabletIdSet, isRestore);
                    olapTable.addPartition(partition);
                }
            } else {
                throw new DdlException("Unsupport partition method: " + partitionInfo.getType().name());
            }

            if (isRestore) {
                // ATTN: do not add this table to db.
                // if add, replica info may be removed when handling tablet
                // report,
                // cause be does not have real data file
                returnTable = olapTable;
                LOG.info("successfully create table[{};{}] to restore", tableName, tableId);
            } else {
                if (!db.createTableWithLock(olapTable, false, stmt.isSetIfNotExists())) {
                    // TODO(cmy): add error code timeout;
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                }
                LOG.info("successfully create table[{};{}]", tableName, tableId);
            }
        } catch (DdlException e) {
            for (Long tabletId : tabletIdSet) {
                Catalog.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }

        return returnTable;
    }

    private Table createMysqlTable(Database db, CreateTableStmt stmt, boolean isRestore) throws DdlException {
        String tableName = stmt.getTableName();

        List<Column> columns = stmt.getColumns();

        long tableId = Catalog.getInstance().getNextId();
        MysqlTable mysqlTable = new MysqlTable(tableId, tableName, columns, stmt.getProperties());

        Table returnTable = null;
        if (isRestore) {
            returnTable = mysqlTable;
            LOG.info("successfully create table[{}-{}] to restore", tableName, tableId);
        } else {
            if (!db.createTableWithLock(mysqlTable, false, stmt.isSetIfNotExists())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exist");
            }
            LOG.info("successfully create table[{}-{}]", tableName, tableId);
        }

        return returnTable;
    }

    private Table createKuduTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        // 1. create kudu schema
        List<ColumnSchema> kuduColumns = null;
        List<Column> baseSchema = stmt.getColumns();
        kuduColumns = KuduUtil.generateKuduColumn(baseSchema);
        Schema kuduSchema = new Schema(kuduColumns);

        CreateTableOptions kuduCreateOpts = new CreateTableOptions();

        // 2. distribution
        Preconditions.checkNotNull(stmt.getDistributionDesc());
        HashDistributionDesc hashDistri = (HashDistributionDesc) stmt.getDistributionDesc();
        kuduCreateOpts.addHashPartitions(hashDistri.getDistributionColumnNames(), hashDistri.getBuckets());
        KuduPartition hashPartition = KuduPartition.createHashPartition(hashDistri.getDistributionColumnNames(),
                                                                        hashDistri.getBuckets());

        // 3. partition, if exist
        KuduPartition rangePartition = null;
        if (stmt.getPartitionDesc() != null) {
            RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) stmt.getPartitionDesc();
            List<KuduRange> kuduRanges = Lists.newArrayList();
            KuduUtil.setRangePartitionInfo(kuduSchema, rangePartitionDesc, kuduCreateOpts, kuduRanges);
            rangePartition = KuduPartition.createRangePartition(rangePartitionDesc.getPartitionColNames(), kuduRanges);
        }

        Map<String, String> properties = stmt.getProperties();
        // 4. replication num
        short replicationNum = FeConstants.default_replication_num;
        try {
            replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        kuduCreateOpts.setNumReplicas(replicationNum);

        // 5. analyze kudu master addr
        String kuduMasterAddrs = Config.kudu_master_addresses;
        try {
            kuduMasterAddrs = PropertyAnalyzer.analyzeKuduMasterAddr(properties, kuduMasterAddrs);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        if (properties != null && !properties.isEmpty()) {
            // here, all properties should be checked
            throw new DdlException("Unknown properties: " + properties);
        }

        // 6. create table
        LOG.info("create table: {} in kudu with kudu master: [{}]", tableName, kuduMasterAddrs);
        KuduClient client = new KuduClient.KuduClientBuilder(Config.kudu_master_addresses).build();
        org.apache.kudu.client.KuduTable table = null;
        try {
            client.createTable(tableName, kuduSchema, kuduCreateOpts);
            table = client.openTable(tableName);
        } catch (KuduException e) {
            LOG.warn("failed to create kudu table: {}", tableName, e);
            ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, e.getMessage());
        }

        // 7. add table to catalog
        long tableId = getNextId();
        KuduTable kuduTable = new KuduTable(tableId, baseSchema, table);
        kuduTable.setRangeParititon(rangePartition);
        kuduTable.setHashPartition(hashPartition);
        kuduTable.setMasterAddrs(kuduMasterAddrs);
        kuduTable.setKuduTableId(table.getTableId());

        if (!db.createTableWithLock(kuduTable, false, stmt.isSetIfNotExists())) {
            // try to clean the obsolate table
            try {
                client.deleteTable(tableName);
            } catch (KuduException e) {
                LOG.warn("failed to delete table {} after failed creating table", tableName, e);
            }
            ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
        }

        LOG.info("successfully create table[{}-{}]", tableName, tableId);

        return kuduTable;
    }

    private Table createBrokerTable(Database db, CreateTableStmt stmt, boolean isRestore) throws DdlException {
        String tableName = stmt.getTableName();

        List<Column> columns = stmt.getColumns();

        long tableId = Catalog.getInstance().getNextId();
        BrokerTable brokerTable = new BrokerTable(tableId, tableName, columns, stmt.getProperties());
        brokerTable.setBrokerProperties(stmt.getExtProperties());

        Table returnTable = null;
        if (isRestore) {
            returnTable = brokerTable;
            LOG.info("successfully create table[{}-{}] to restore", tableName, tableId);
        } else {
            if (!db.createTableWithLock(brokerTable, false, stmt.isSetIfNotExists())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exist");
            }
            LOG.info("successfully create table[{}-{}]", tableName, tableId);
        }

        return returnTable;
    }

    public static void getDdlStmt(Table table, List<String> createTableStmt, List<String> addPartitionStmt,
            List<String> createRollupStmt, boolean separatePartition, short replicationNum) {
        StringBuilder sb = new StringBuilder();

        // 1. create table
        // 1.1 view
        if (table.getType() == TableType.VIEW) {
            View view = (View) table;
            sb.append("CREATE VIEW `").append(table.getName()).append("` AS ").append(view.getInlineViewDef());
            sb.append(";");
            createTableStmt.add(sb.toString());
            return;
        }

        // 1.2 other table type
        sb.append("CREATE ");
        if (table.getType() == TableType.KUDU || table.getType() == TableType.MYSQL) {
            sb.append("EXTERNAL ");
        }
        sb.append("TABLE ");
        sb.append("`").append(table.getName()).append("` (\n");
        int idx = 0;
        for (Column column : table.getBaseSchema()) {
            if (idx++ != 0) {
                sb.append(",\n");
            }
            sb.append(" ").append(column.toSql());
        }
        sb.append("\n) ENGINE=");
        sb.append(table.getType().name());

        if (table.getType() == TableType.OLAP) {
            OlapTable olapTable = (OlapTable) table;

            // keys
            sb.append("\n").append(olapTable.getKeysType().toSql()).append("(");
            List<String> keysColumnNames = Lists.newArrayList();
            for (Column column : olapTable.getBaseSchema()) {
                if (column.isKey()) {
                    keysColumnNames.add("`" + column.getName() + "`");
                }
            }
            sb.append(Joiner.on(", ").join(keysColumnNames)).append(")");

            // partition
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            List<Long> partitionId = null;
            if (separatePartition) {
                partitionId = Lists.newArrayList();
            }
            sb.append("\n").append(partitionInfo.toSql(olapTable, partitionId));

            // distribution
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            sb.append("\n").append(distributionInfo.toSql());

            // properties
            sb.append("\nPROPERTIES (\n");

            // 1. storage type
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE).append("\" = \"");
            TStorageType storageType = olapTable
                    .getStorageTypeByIndexId(olapTable.getIndexIdByName(olapTable.getName()));
            sb.append(storageType.name()).append("\"");

            // 2. bloom filter
            Set<String> bfColumnNames = olapTable.getCopiedBfColumns();
            if (bfColumnNames != null) {
                sb.append(",\n \"").append(PropertyAnalyzer.PROPERTIES_BF_COLUMNS).append("\" = \"");
                sb.append(Joiner.on(", ").join(olapTable.getCopiedBfColumns())).append("\"");
            }

            if (separatePartition) {
                // 3. version info
                sb.append(",\n \"").append(PropertyAnalyzer.PROPERTIES_VERSION_INFO).append("\" = \"");
                Partition partition = null;
                if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    partition = olapTable.getPartition(olapTable.getName());
                } else {
                    Preconditions.checkState(partitionId.size() == 1);
                    partition = olapTable.getPartition(partitionId.get(0));
                }
                sb.append(Joiner.on(",").join(partition.getCommittedVersion(), partition.getCommittedVersionHash()))
                        .append("\"");
            }

            if (replicationNum > 0) {
                sb.append(",\n \"").append(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM).append("\" = \"");
                sb.append(replicationNum).append("\"");
            }

            sb.append("\n);");
        } else if (table.getType() == TableType.MYSQL) {
            MysqlTable mysqlTable = (MysqlTable) table;
            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"host\" = \"").append(mysqlTable.getHost()).append("\",\n");
            sb.append("\"port\" = \"").append(mysqlTable.getPort()).append("\",\n");
            sb.append("\"user\" = \"").append(mysqlTable.getUserName()).append("\",\n");
            sb.append("\"password\" = \"").append(mysqlTable.getPasswd()).append("\",\n");
            sb.append("\"database\" = \"").append(mysqlTable.getMysqlDatabaseName()).append("\",\n");
            sb.append("\"table\" = \"").append(mysqlTable.getMysqlTableName()).append("\"\n");
            sb.append(");");
        } else if (table.getType() == TableType.KUDU) {
            KuduTable kuduTable = (KuduTable) table;
            org.apache.kudu.client.KuduTable kTable = kuduTable.getKuduTable();
            if (kTable == null) {
                // real kudu table is not found
                return;
            }

            // keys
            sb.append("\n").append(KeysType.PRIMARY_KEYS.toSql()).append("(");
            List<String> keysColumnNames = Lists.newArrayList();
            for (Column column : kuduTable.getBaseSchema()) {
                if (column.isKey()) {
                    keysColumnNames.add("`" + column.getName() + "`");
                }
            }
            sb.append(Joiner.on(", ").join(keysColumnNames)).append(")");

            // partition
            KuduPartition rangePartition = kuduTable.getRangePartition();
            if (rangePartition != null) {
                sb.append("\n").append(rangePartition);
            }

            // distribution
            KuduPartition hashPartition = kuduTable.getHashPartition();
            sb.append("\n").append(hashPartition);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_KUDU_MASTER_ADDRS).append("\" = \"");
            sb.append(kuduTable.getMasterAddrs()).append("\")");
        } else if (table.getType() == TableType.BROKER) {
            BrokerTable brokerTable = (BrokerTable) table;
            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"broker_name\" = \"").append(brokerTable.getBrokerName()).append("\",\n");
            sb.append("\"path\" = \"").append(Joiner.on(",").join(brokerTable.getEncodedPaths())).append("\",\n");
            sb.append("\"column_separator\" = \"").append(brokerTable.getReadableColumnSeparator()).append("\",\n");
            sb.append("\"line_delimiter\" = \"").append(brokerTable.getReadableLineDelimiter()).append("\",\n");
            sb.append(")");
            if (!brokerTable.getBrokerProperties().isEmpty()) {
                sb.append("\nBROKER PROPERTIES (\n");
                sb.append(new PrintableMap<>(brokerTable.getBrokerProperties(), " = ", true, true).toString());
                sb.append("\n)");
            }

            sb.append(";");
        }

        createTableStmt.add(sb.toString());
        // 2. add partition
        if (separatePartition && (table instanceof OlapTable)
                && ((OlapTable) table).getPartitionInfo().getType() == PartitionType.RANGE
                && ((OlapTable) table).getPartitions().size() > 1) {
            OlapTable olapTable = (OlapTable) table;
            RangePartitionInfo partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            boolean first = true;
            for (Map.Entry<Long, Range<PartitionKey>> entry : partitionInfo.getSortedRangeMap()) {
                if (first) {
                    first = false;
                    continue;
                }
                sb = new StringBuilder();
                Partition partition = olapTable.getPartition(entry.getKey());
                sb.append("ALTER TABLE ").append(table.getName());
                sb.append(" ADD PARTITION ").append(partition.getName()).append(" VALUES LESS THAN ");
                sb.append(entry.getValue().upperEndpoint().toSql());

                sb.append("(\"version_info\" = \"");
                sb.append(Joiner.on(",").join(partition.getCommittedVersion(), partition.getCommittedVersionHash()))
                        .append("\"");
                if (replicationNum > 0) {
                    sb.append(", \"replication_num\" = \"").append(replicationNum).append("\"");
                }

                sb.append(");");
                addPartitionStmt.add(sb.toString());
            }
        }

        // 3. rollup
        if (createRollupStmt != null && (table instanceof OlapTable)) {
            OlapTable olapTable = (OlapTable) table;
            for (Map.Entry<Long, List<Column>> entry : olapTable.getIndexIdToSchema().entrySet()) {
                if (entry.getKey() == olapTable.getId()) {
                    continue;
                }
                sb = new StringBuilder();
                String indexName = olapTable.getIndexNameById(entry.getKey());
                sb.append("ALTER TABLE ").append(table.getName()).append(" ADD ROLLUP ").append(indexName);
                sb.append("(");

                for (int i = 0; i < entry.getValue().size(); i++) {
                    Column column = entry.getValue().get(i);
                    sb.append(column.getName());
                    if (i != entry.getValue().size() - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(");");
                createRollupStmt.add(sb.toString());
            }
        }
    }

    public void replayCreateTable(String dbName, Table table) {
        Database db = this.nameToDb.get(dbName);
        db.createTableWithLock(table, true, false);

        if (!isCheckpointThread()) {
            // add to inverted index
            if (table.getType() == TableType.OLAP) {
                TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                OlapTable olapTable = (OlapTable) table;
                long dbId = db.getId();
                long tableId = table.getId();
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices()) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash);
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            for (Replica replica : tablet.getReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                            }
                        }
                    }
                } // end for partitions
            }
        }

    }

    private void createTablets(String clusterName, MaterializedIndex index, ReplicaState replicaState,
            DistributionInfo distributionInfo, long version, long versionHash, short replicationNum,
            TabletMeta tabletMeta, Set<Long> tabletIdSet) throws DdlException {
        Preconditions.checkArgument(replicationNum > 0);

        DistributionInfoType distributionInfoType = distributionInfo.getType();
        if (distributionInfoType == DistributionInfoType.RANDOM || distributionInfoType == DistributionInfoType.HASH) {
            for (int i = 0; i < distributionInfo.getBucketNum(); ++i) {
                // create a new tablet with random chosen backends
                Tablet tablet = new Tablet(getNextId());

                // add tablet to inverted index first
                index.addTablet(tablet, tabletMeta);
                tabletIdSet.add(tablet.getId());

                // create replicas for tablet with random chosen backends
                List<Long> chosenBackendIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(replicationNum, true,
                        true, clusterName);
                if (chosenBackendIds == null) {
                    throw new DdlException("Failed to find enough host in all backends. need: " + replicationNum);
                }
                Preconditions.checkState(chosenBackendIds.size() == replicationNum);
                for (long backendId : chosenBackendIds) {
                    long replicaId = getNextId();
                    Replica replica = new Replica(replicaId, backendId, replicaState, version, versionHash);
                    tablet.addReplica(replica);
                }
            }
        } else {
            throw new DdlException("Unknown distribution type: " + distributionInfoType);
        }
    }

    // Drop table
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        Database db = null;
        Table table = null;
        readLock();
        try {
            // check database
            db = this.nameToDb.get(dbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }
        } finally {
            readUnlock();
        }

        db.writeLock();
        try {
            table = db.getTable(tableName);
            if (table == null) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop table[{}] which does not exist", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }
            }

            // Check if a view
            if (stmt.isView()) {
                if (!(table instanceof View)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "VIEW");
                }
            } else {
                if (table instanceof View) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "TABLE");
                }
            }

            unprotectDropTable(db, table.getId());
        } finally {
            db.writeUnlock();
        }

        DropInfo info = new DropInfo(db.getId(), table.getId(), -1L);
        editLog.logDropTable(info);

        LOG.info("finish drop table[{}] from db[{}]", tableName, dbName);
    }

    public boolean unprotectDropTable(Database db, long tableId) {
        Table table = db.getTable(tableId);
        // delete from db meta
        if (table == null) {
            return false;
        }

        if (table.getType() == TableType.OLAP) {
            OlapTable olapTable = (OlapTable) table;
            cancelJobsWithTable(db, olapTable);
        } else if (table.getType() == TableType.KUDU) {
            KuduTable kuduTable = (KuduTable) table;
            KuduClient client = KuduUtil.createKuduClient(kuduTable.getMasterAddrs());
            try {
                // open the table again to check if it is the same table
                org.apache.kudu.client.KuduTable kTable = client.openTable(kuduTable.getName());
                if (!kTable.getTableId().equalsIgnoreCase(kuduTable.getKuduTableId())) {
                    LOG.warn("kudu table {} is changed before replay. skip it", kuduTable.getName());
                } else {
                    client.deleteTable(kuduTable.getName());
                }
            } catch (KuduException e) {
                LOG.warn("failed to delete kudu table {} when replay", kuduTable.getName(), e);
            }
        }

        db.dropTable(table.getName());
        Catalog.getCurrentRecycleBin().recycleTable(db.getId(), table);

        LOG.info("finished dropping table[{}] in db[{}]", table.getName(), db.getName());
        return true;
    }

    public void replayDropTable(Database db, long tableId) {
        db.writeLock();
        try {
            unprotectDropTable(db, tableId);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayEraseTable(long tableId) throws DdlException {
        Catalog.getCurrentRecycleBin().replayEraseTable(tableId);
    }

    public void replayRecoverTable(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        db.writeLock();
        try {
            Catalog.getCurrentRecycleBin().replayRecoverTable(db, info.getTableId());
        } finally {
            db.writeUnlock();
        }
    }

    private void cancelJobsWithTable(Database db, OlapTable olapTable) {
        // remove related jobs
        RollupHandler rollupHandler = Catalog.getInstance().getRollupHandler();
        rollupHandler.cancelWithTable(olapTable);
        SchemaChangeHandler schemaChangeHandler = Catalog.getInstance().getSchemaChangeHandler();
        schemaChangeHandler.cancelWithTable(olapTable);
        Clone clone = Catalog.getInstance().getCloneInstance();
        clone.cancelCloneJob(olapTable);
    }

    public void handleJobsWhenDeleteReplica(long tableId, long partitionId, long indexId, long tabletId, long replicaId,
            long backendId) {
        // rollup
        getRollupHandler().removeReplicaRelatedTask(tableId, partitionId, indexId, tabletId, backendId);

        // schema change
        getSchemaChangeHandler().removeReplicaRelatedTask(tableId, tabletId, replicaId, backendId);

        // task
        AgentTaskQueue.removeReplicaRelatedTasks(backendId, tabletId);
    }

    public void unprotectAddReplica(ReplicaPersistInfo info) {
        Database db = getDb(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
        Partition partition = olapTable.getPartition(info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        Tablet tablet = materializedIndex.getTablet(info.getTabletId());
        Replica replica = new Replica(info.getReplicaId(), info.getBackendId(), info.getVersion(),
                info.getVersionHash(), info.getDataSize(), info.getRowCount(), ReplicaState.NORMAL);
        tablet.addReplica(replica);
    }

    public void replayAddReplica(ReplicaPersistInfo info) {
        Database db = getDb(info.getDbId());
        db.writeLock();
        try {
            unprotectAddReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    public void unprotectDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDb(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
        Partition partition = olapTable.getPartition(info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        Tablet tablet = materializedIndex.getTablet(info.getTabletId());
        tablet.deleteReplicaByBackendId(info.getBackendId());
    }

    public void replayDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDb(info.getDbId());
        db.writeLock();
        try {
            unprotectDeleteReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayAddFrontend(Frontend fe) {
        writeLock();
        try {
            if (checkFeExist(fe.getHost(), fe.getPort()) != null) {
                LOG.warn("Fe {} already exist.", fe);
                return;
            }
            frontends.add(fe);
        } finally {
            writeUnlock();
        }
    }

    public void replayDropFrontend(Frontend frontend) {
        writeLock();
        try {
            Frontend fe = checkFeExist(frontend.getHost(), frontend.getPort());
            if (fe == null) {
                LOG.error(frontend.toString() + " does not exist.");
                return;
            }
            frontends.remove(fe);
            removedFrontends.add(fe);
        } finally {
            writeUnlock();
        }
    }

    public int getClusterId() {
        return this.clusterId;
    }

    public Database getDb(String name) {
        readLock();
        try {
            if (nameToDb.containsKey(name)) {
                return nameToDb.get(name);
            } else if (name.equalsIgnoreCase(InfoSchemaDb.getDatabaseName())) {
                return nameToDb.get(InfoSchemaDb.getDatabaseName());
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    public Database getDb(long dbId) {
        readLock();
        try {
            if (idToDb.containsKey(dbId)) {
                return idToDb.get(dbId);
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    public EditLog getEditLog() {
        return editLog;
    }

    // Get the next available, need't lock because of nextId is atomic.
    public long getNextId() {
        long id = nextId.getAndIncrement();
        editLog.logSaveNextId(id);
        return id;
    }

    public List<String> getDbNames() {
        readLock();
        try {
            List<String> dbNames = Lists.newArrayList(nameToDb.keySet());
            return dbNames;
        } finally {
            readUnlock();
        }
    }


    public List<String> getClusterDbNames(String clusterName) throws AnalysisException {
        readLock();
        try {
            final Cluster cluster = nameToCluster.get(clusterName);
            if (cluster == null) {
                throw new AnalysisException("No cluster selected");
            }    
            List<String> dbNames = Lists.newArrayList(cluster.getDbNames());
            return dbNames;
        } finally {
            readUnlock();
        }    
    } 

    public List<Long> getDbIds() {
        readLock();
        try {
            List<Long> dbIds = Lists.newArrayList(idToDb.keySet());
            return dbIds;
        } finally {
            readUnlock();
        }
    }

    public HashMap<Long, TStorageMedium> getPartitionIdToStorageMediumMap() {
        HashMap<Long, TStorageMedium> storageMediumMap = new HashMap<Long, TStorageMedium>();

        // record partition which need to change storage medium
        // dbId -> (tableId -> partitionId)
        HashMap<Long, Multimap<Long, Long>> changedPartitionsMap = new HashMap<Long, Multimap<Long, Long>>();
        long currentTimeMs = System.currentTimeMillis();
        List<Long> dbIds = getDbIds();

        for (long dbId : dbIds) {
            Database db = getDb(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while doing backend report", dbId);
                continue;
            }

            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    long tableId = table.getId();
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    for (Partition partition : olapTable.getPartitions()) {
                        long partitionId = partition.getId();
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        Preconditions.checkNotNull(dataProperty);
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs) {
                            // expire. change to HDD.
                            // record and change when holding write lock
                            Multimap<Long, Long> multimap = changedPartitionsMap.get(dbId);
                            if (multimap == null) {
                                multimap = HashMultimap.create();
                                changedPartitionsMap.put(dbId, multimap);
                            }
                            multimap.put(tableId, partitionId);
                        } else {
                            storageMediumMap.put(partitionId, dataProperty.getStorageMedium());
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                db.readUnlock();
            }
        } // end for dbs

        // handle data property changed
        for (Long dbId : changedPartitionsMap.keySet()) {
            Database db = getDb(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while checking backend storage medium", dbId);
                continue;
            }
            Multimap<Long, Long> tableIdToPartitionIds = changedPartitionsMap.get(dbId);

            // use try lock to avoid blocking a long time.
            // if block too long, backend report rpc will timeout.
            if (!db.tryWriteLock(Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOG.warn("try get db {} writelock but failed when hecking backend storage medium", dbId);
                continue;
            }
            Preconditions.checkState(db.isWriteLockHeldByCurrentThread());
            try {
                for (Long tableId : tableIdToPartitionIds.keySet()) {
                    Table table = db.getTable(tableId);
                    if (table == null) {
                        continue;
                    }
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();

                    Collection<Long> partitionIds = tableIdToPartitionIds.get(tableId);
                    for (Long partitionId : partitionIds) {
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            continue;
                        }
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs) {
                            // expire. change to HDD.
                            partitionInfo.setDataProperty(partition.getId(),
                                                          DataProperty.DEFAULT_HDD_DATA_PROPERTY);
                            storageMediumMap.put(partitionId, TStorageMedium.HDD);
                            LOG.debug("partition[{}-{}-{}] storage medium changed from SSD to HDD",
                                      dbId, tableId, partitionId);

                            // log
                            ModifyPartitionInfo info =
                                    new ModifyPartitionInfo(db.getId(), olapTable.getId(),
                                                            partition.getId(),
                                                            DataProperty.DEFAULT_HDD_DATA_PROPERTY,
                                                            (short) -1);
                            editLog.logModifyPartition(info);
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                db.writeUnlock();
            }
        } // end for dbs
        return storageMediumMap;
    }

    public ConsistencyChecker getConsistencyChecker() {
        return this.consistencyChecker;
    }

    public Alter getAlterInstance() {
        return this.alter;
    }

    public SchemaChangeHandler getSchemaChangeHandler() {
        return (SchemaChangeHandler) this.alter.getSchemaChangeHandler();
    }

    public RollupHandler getRollupHandler() {
        return (RollupHandler) this.alter.getRollupHandler();
    }

    public SystemHandler getClusterHandler() {
        return (SystemHandler) this.alter.getClusterHandler();
    }

    public BackupHandler getBackupHandler() {
        return this.backupHandler;
    }

    public UserPropertyMgr getUserMgr() {
        return this.userPropertyMgr;
    }

    public Load getLoadInstance() {
        return this.load;
    }

    public ExportMgr getExportMgr() {
        return this.exportMgr;
    }

    public Clone getCloneInstance() {
        return this.clone;
    }

    public long getReplayedJournalId() {
        return this.replayedJournalId;
    }

    public HAProtocol getHaProtocol() {
        return this.haProtocol;
    }

    public Long getMaxJournalId() {
        return this.editLog.getMaxJournalId();
    }

    public long getEpoch() {
        return this.epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public FrontendNodeType getRole() {
        return this.role;
    }

    public Pair<String, Integer> getHelperNode() {
        return this.helperNode;
    }

    public Pair<String, Integer> getSelfNode() {
        return this.selfNode;
    }

    public FrontendNodeType getFeType() {
        return this.feType;
    }

    public void setFeType(FrontendNodeType type) {
        this.feType = type;
    }

    public int getMasterRpcPort() {
        if (feType == FrontendNodeType.UNKNOWN || feType == FrontendNodeType.MASTER && !canWrite) {
            return 0;
        }
        return this.masterRpcPort;
    }

    public void setMasterRpcPort(int port) {
        this.masterRpcPort = port;
    }

    public int getMasterHttpPort() {
        if (feType == FrontendNodeType.UNKNOWN || feType == FrontendNodeType.MASTER && !canWrite) {
            return 0;
        }
        return this.masterHttpPort;
    }

    public void setMasterHttpPort(int port) {
        this.masterHttpPort = port;
    }

    public String getMasterIp() {
        if (feType == FrontendNodeType.UNKNOWN || feType == FrontendNodeType.MASTER && !canWrite) {
            return null;
        }
        return this.masterIp;
    }

    public void setMasterIp(String ip) {
        this.masterIp = ip;
    }

    public boolean canWrite() {
        return this.canWrite;
    }

    public boolean canRead() {
        return this.canRead;
    }

    public void setMetaDir(String metaDir) {
        this.metaDir = metaDir;
    }

    public boolean isElectable() {
        return this.isElectable;
    }

    public void setIsMaster(boolean isMaster) {
        this.isMaster = isMaster;
    }

    public boolean isMaster() {
        return this.isMaster;
    }

    public void setSynchronizedTime(long time) {
        this.synchronizedTimeMs = time;
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    public void setNextId(long id) {
        if (nextId.get() < id) {
            nextId.set(id);
        }
    }

    public void setHaProtocol(HAProtocol protocol) {
        this.haProtocol = protocol;
    }

    public void setJournalVersion(int version) {
        this.journalVersion = version;
    }

    // Get current journal meta data version
    public int getJournalVersion() {
        return this.journalVersion;
    }

    // Get current journal meta data version
    public int getImageVersion() {
        return this.imageVersion;
    }

    public static short calcShortKeyColumnCount(List<Column> columns, Map<String, String> properties)
            throws DdlException {
        List<Column> indexColumns = new ArrayList<Column>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (column.isKey()) {
                indexColumns.add(column);
            }
        }
        LOG.debug("index column size: {}", indexColumns.size());
        Preconditions.checkArgument(indexColumns.size() > 0);

        // figure out shortKeyColumnCount
        short shortKeyColumnCount = (short) -1;
        try {
            shortKeyColumnCount = PropertyAnalyzer.analyzeShortKeyColumnCount(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (shortKeyColumnCount != (short) -1) {
            // use user specified short key column count
            if (shortKeyColumnCount <= 0) {
                throw new DdlException("Invalid short key: " + shortKeyColumnCount);
            }

            if (shortKeyColumnCount > indexColumns.size()) {
                throw new DdlException("Short key is too large. should less than: " + indexColumns.size());
            }

            for (int pos = 0; pos < shortKeyColumnCount; pos++) {
                if (indexColumns.get(pos).getDataType() == PrimitiveType.VARCHAR && pos != shortKeyColumnCount - 1) {
                    throw new DdlException("Varchar should not in the middle of short keys.");
                }
            }
        } else {
            /*
             * Calc short key column count. NOTE: short key column count is
             * calculated as follow: 1. All index column are taking into
             * account. 2. Max short key column count is Min(Num of
             * indexColumns, META_MAX_SHORT_KEY_NUM). 3. Short key list can
             * contains at most one VARCHAR column. And if contains, it should
             * be at the last position of the short key list.
             */
            shortKeyColumnCount = 1;
            int shortKeySizeByte = 0;
            Column firstColumn = indexColumns.get(0);
            if (firstColumn.getDataType() != PrimitiveType.VARCHAR) {
                shortKeySizeByte = firstColumn.getOlapColumnIndexSize();
                int maxShortKeyColumnCount = Math.min(indexColumns.size(), FeConstants.shortkey_max_column_count);
                for (int i = 1; i < maxShortKeyColumnCount; i++) {
                    Column column = indexColumns.get(i);
                    shortKeySizeByte += column.getOlapColumnIndexSize();
                    if (shortKeySizeByte > FeConstants.shortkey_maxsize_bytes) {
                        break;
                    }
                    if (column.getDataType() == PrimitiveType.VARCHAR) {
                        ++shortKeyColumnCount;
                        break;
                    }
                    ++shortKeyColumnCount;
                }
            }
            // else
            // first column type is VARCHAR
            // use only first column as shortKey
            // do nothing here

        } // end calc shortKeyColumnCount

        return shortKeyColumnCount;
    }

    /*
     * used for handling AlterTableStmt (for client is the ALTER TABLE command).
     * including SchemaChangeHandler and RollupHandler
     */
    public void alterTable(AlterTableStmt stmt) throws DdlException, InternalException {
        this.alter.processAlterTable(stmt);
    }

    /*
     * used for handling CacnelAlterStmt (for client is the CANCEL ALTER
     * command). including SchemaChangeHandler and RollupHandler
     */
    public void cancelAlter(CancelAlterTableStmt stmt) throws DdlException {
        if (stmt.getAlterType() == AlterType.ROLLUP) {
            this.getRollupHandler().cancel(stmt);
        } else if (stmt.getAlterType() == AlterType.COLUMN) {
            this.getSchemaChangeHandler().cancel(stmt);
        } else {
            throw new DdlException("Cancel " + stmt.getAlterType() + " does not implement yet");
        }
    }

    /*
     * used for handling backup opt
     */
    public void backup(BackupStmt stmt) throws DdlException {
        getBackupHandler().process(stmt);
    }

    public void restore(RestoreStmt stmt) throws DdlException {
        getBackupHandler().process(stmt);
    }

    public void cancelBackup(CancelBackupStmt stmt) throws DdlException {
        getBackupHandler().cancel(stmt);
    }

    public void renameTable(Database db, OlapTable table, TableRenameClause tableRenameClause) throws DdlException {
        if (table.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + table.getState());
        }

        String tableName = table.getName();
        String newTableName = tableRenameClause.getNewTableName();
        if (tableName.equals(newTableName)) {
            throw new DdlException("Same table name");
        }

        // check if name is already used
        if (db.getTable(newTableName) != null) {
            throw new DdlException("Table name[" + newTableName + "] is already used");
        }

        table.setName(newTableName);

        db.dropTable(tableName);
        db.createTable(table);

        TableInfo tableInfo = TableInfo.createForTableRename(db.getId(), table.getId(), newTableName);
        editLog.logTableRename(tableInfo);
        LOG.info("rename table[{}] to {}", tableName, newTableName);
    }

    public void replayRenameTable(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        String newTableName = tableInfo.getNewTableName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            String tableName = table.getName();
            db.dropTable(tableName);
            table.setName(newTableName);
            db.createTable(table);

            LOG.info("replay rename table[{}] to {}", tableName, newTableName);
        } finally {
            db.writeUnlock();
        }
    }

    public void renameRollup(Database db, OlapTable table, RollupRenameClause renameClause) throws DdlException {
        if (table.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + table.getState());
        }

        String rollupName = renameClause.getRollupName();
        // check if it is base table name
        if (rollupName.equals(table.getName())) {
            throw new DdlException("Using ALTER TABLE RENAME to change table name");
        }

        String newRollupName = renameClause.getNewRollupName();
        if (rollupName.equals(newRollupName)) {
            throw new DdlException("Same rollup name");
        }

        Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
        if (indexNameToIdMap.get(rollupName) == null) {
            throw new DdlException("Rollup index[" + rollupName + "] does not exists");
        }

        // check if name is already used
        if (indexNameToIdMap.get(newRollupName) != null) {
            throw new DdlException("Rollup name[" + newRollupName + "] is already used");
        }

        long indexId = indexNameToIdMap.remove(rollupName);
        indexNameToIdMap.put(newRollupName, indexId);

        // log
        TableInfo tableInfo = TableInfo.createForRollupRename(db.getId(), table.getId(), indexId, newRollupName);
        editLog.logRollupRename(tableInfo);
        LOG.info("rename rollup[{}] to {}", rollupName, newRollupName);
    }

    public void replayRenameRollup(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long indexId = tableInfo.getIndexId();
        String newRollupName = tableInfo.getNewRollupName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            String rollupName = table.getIndexNameById(indexId);
            Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
            indexNameToIdMap.remove(rollupName);
            indexNameToIdMap.put(newRollupName, indexId);

            LOG.info("replay rename rollup[{}] to {}", rollupName, newRollupName);
        } finally {
            db.writeUnlock();
        }
    }

    public void renamePartition(Database db, OlapTable table, PartitionRenameClause renameClause) throws DdlException {
        if (table.getState() != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + table.getState());
        }

        if (table.getPartitionInfo().getType() != PartitionType.RANGE) {
            throw new DdlException("Table[" + table.getName() + "] is single partitioned. "
                    + "no need to rename partition name.");
        }

        String partitionName = renameClause.getPartitionName();
        String newPartitionName = renameClause.getNewPartitionName();
        if (partitionName.equalsIgnoreCase(newPartitionName)) {
            throw new DdlException("Same partition name");
        }

        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition[" + partitionName + "] does not exists");
        }

        // check if name is already used
        if (table.getPartition(newPartitionName) != null) {
            throw new DdlException("Partition name[" + newPartitionName + "] is already used");
        }

        table.renamePartition(partitionName, newPartitionName);

        // log
        TableInfo tableInfo = TableInfo.createForPartitionRename(db.getId(), table.getId(), partition.getId(),
                newPartitionName);
        editLog.logPartitionRename(tableInfo);
        LOG.info("rename partition[{}] to {}", partitionName, newPartitionName);
    }

    public void replayRenamePartition(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long partitionId = tableInfo.getPartitionId();
        String newPartitionName = tableInfo.getNewPartitionName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            Partition partition = table.getPartition(partitionId);
            table.renamePartition(partition.getName(), newPartitionName);

            LOG.info("replay rename partition[{}] to {}", partition.getName(), newPartitionName);
        } finally {
            db.writeUnlock();
        }
    }

    public void renameColumn(Database db, OlapTable table, ColumnRenameClause renameClause) throws DdlException {
        throw new DdlException("not implmented");
    }

    public void replayRenameColumn(TableInfo tableInfo) throws DdlException {
        throw new DdlException("not implmented");
    }

    /*
     * used for handling AlterClusterStmt (for client is the ALTER CLUSTER
     * command). including AlterClusterHandler
     */
    public void alterCluster(AlterSystemStmt stmt) throws DdlException, InternalException {
        this.alter.processAlterCluster(stmt);
    }

    public void cancelAlterCluster(CancelAlterSystemStmt stmt) throws DdlException {
        this.alter.getClusterHandler().cancel(stmt);
    }

    /*
     * generate and check columns' order and key's existence
     */
    private void validateColumns(List<Column> columns) throws DdlException {
        if (columns.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        boolean encounterValue = false;
        boolean hasKey = false;
        for (Column column : columns) {
            if (column.isKey()) {
                if (encounterValue) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_OLAP_KEY_MUST_BEFORE_VALUE);
                }
                hasKey = true;
            } else {
                encounterValue = true;
            }
        }

        if (!hasKey) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_KEYS);
        }
    }

    // Change current database of this session.
    public void changeDb(ConnectContext ctx, String dbName) throws DdlException {
        if (getDb(dbName) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        if (!userPropertyMgr.checkAccess(ctx.getUser(), dbName, AccessPrivilege.READ_ONLY)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_DB_ACCESS_DENIED, ctx.getUser(), dbName);
        }
        ctx.setDatabase(dbName);
    }

    // for test only
    public void clear() {
        if (SingletonHolder.INSTANCE.idToDb != null) {
            SingletonHolder.INSTANCE.idToDb.clear();
        }
        if (SingletonHolder.INSTANCE.nameToDb != null) {
            SingletonHolder.INSTANCE.nameToDb.clear();
        }
        if (load.getIdToLoadJob() != null) {
            load.getIdToLoadJob().clear();
            // load = null;
        }

        SingletonHolder.INSTANCE.getRollupHandler().unprotectedGetAlterJobs().clear();
        SingletonHolder.INSTANCE.getSchemaChangeHandler().unprotectedGetAlterJobs().clear();
        System.gc();
    }

    public void createView(CreateViewStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTable();

        // check if db exists
        Database db = this.getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check if table exists in db
        db.readLock();
        try {
            if (db.getTable(tableName) != null) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create view[{}] which already exists", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
        } finally {
            db.readUnlock();
        }

        List<Column> columns = stmt.getColumns();

        long tableId = Catalog.getInstance().getNextId();
        View newView = new View(tableId, tableName, columns);
        newView.setInlineViewDef(stmt.getInlineViewDef());
        newView.setOriginalViewDef(stmt.getInlineViewDef());
        try {
            newView.init();
        } catch (InternalException e) {
            throw new DdlException(e.getMessage());
        }

        if (!db.createTableWithLock(newView, false, stmt.isSetIfNotExists())) {
            throw new DdlException("Failed to create view[" + tableName + "].");
        }
        LOG.info("successfully create view[" + tableName + "-" + newView.getId() + "]");
    }

    /**
     * Returns the function that best matches 'desc' that is registered with the
     * catalog using 'mode' to check for matching. If desc matches multiple
     * functions in the catalog, it will return the function with the strictest
     * matching mode. If multiple functions match at the same matching mode,
     * ties are broken by comparing argument types in lexical order. Argument
     * types are ordered by argument precision (e.g. double is preferred over
     * float) and then by alphabetical order of argument type name, to guarantee
     * deterministic results.
     */
    public Function getFunction(Function desc, Function.CompareMode mode) {
        return functionSet.getFunction(desc, mode);
    }

    public void alterUser(AlterUserStmt stmt) throws DdlException {
        getUserMgr().alterUser(stmt);
    }

    public boolean checkWhiteList(String user, String remoteIp) {
        return getUserMgr().checkWhiltListAccess(user, remoteIp);
    }

    public List<List<String>> showWhiteList(String user) {
        return getUserMgr().showWhiteList(user);
    }

    /**
     * create cluster
     *
     * @param stmt
     * @throws DdlException
     */
    public void createCluster(CreateClusterStmt stmt) throws DdlException {
        final String clusterName = stmt.getClusterName();
        writeLock();
        try {
            if (nameToCluster.containsKey(clusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_HAS_EXIST, clusterName);
            } else {
                List<Long> backendList = Lists.newArrayList();
                backendList = systemInfo.createCluster(clusterName, stmt.getInstanceNum());
                if (backendList != null) {
                    final long id = getNextId();
                    final Cluster cluster = new Cluster();
                    cluster.setName(clusterName);
                    cluster.setId(id);
                    cluster.setBackendIdList(backendList);
                    unprotectCreateCluster(cluster);
                    if (clusterName.equals(SystemInfoService.DEFAULT_CLUSTER)) {
                        for (Database db : idToDb.values()) {
                            if (db.getClusterName().equals(SystemInfoService.DEFAULT_CLUSTER)) {
                                cluster.addDb(db.getName(), db.getId());
                            }
                        }
                    }
                    editLog.logCreateCluster(cluster);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_BE_NOT_ENOUGH);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    private void unprotectCreateCluster(Cluster cluster) {
        if (cluster.getName().equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            if (cluster.getBackendIdList().isEmpty()) {
                isDefaultClusterCreated = true;
                // ignore default_cluster
                return;
            }
        }
        final Iterator<Long> iterator = cluster.getBackendIdList().iterator();
        while (iterator.hasNext()) {
            final Long id = iterator.next();
            final Backend backend = systemInfo.getBackend(id);
            backend.setOwnerClusterName(cluster.getName());
            backend.setBackendState(BackendState.using);
        }
        idToCluster.put(cluster.getId(), cluster);
        nameToCluster.put(cluster.getName(), cluster);
        final InfoSchemaDb db = new InfoSchemaDb(cluster.getName());
        db.setClusterName(cluster.getName());
        unprotectCreateDb(db);

        if (cluster.getName().equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            isDefaultClusterCreated = true;
            for (String dbName : cluster.getDbNames()) {
                if (dbName.equalsIgnoreCase(ClusterNamespace.getDbFullName(SystemInfoService.DEFAULT_CLUSTER,
                                                                           InfoSchemaDb.getDatabaseName()))) {
                    continue;
                }
                Database otherDb = nameToDb.get(dbName);
                if (otherDb == null) {
                    LOG.error("failed to get db {} when replay create default cluster", dbName);
                    continue;
                }
                otherDb.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
            }
        }
    }

    /**
     * replay create cluster
     *
     * @param cluster
     */
    public void replayCreateCluster(Cluster cluster) {
        writeLock();
        try {
            unprotectCreateCluster(cluster);
        } finally {
            writeUnlock();
        }
    }

    /**
     * drop cluster and cluster's db must be have deleted
     *
     * @param stmt
     * @throws DdlException
     */
    public void dropCluster(DropClusterStmt stmt) throws DdlException {
        writeLock();
        try {
            final Cluster cluster = nameToCluster.get(stmt.getClusterName());
            final String clusterName = stmt.getClusterName();
            if (cluster == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
            }
            final List<Backend> backends = systemInfo.getClusterBackends(clusterName);
            for (Backend backend : backends) {
                if (backend.isDecommissioned() && backend.getDecommissionType() == DecomissionType.SystemDecomission) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_ALTER_BE_IN_SYSTEM_DECOMMISSION, clusterName);
                }

                if (backend.isDecommissioned() && backend.getDecommissionType() == DecomissionType.ClusterDecomission) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_ALTER_BE_IN_CLUSTER_DECOMMISSION, clusterName);
                }
            }

            // check if there still have database undropped, except for information_schema db
            if (cluster.getDbNames().size() > 1) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DELETE_DB_EXIST, clusterName);
            }

            final ClusterInfo info = new ClusterInfo(clusterName, nameToCluster.get(clusterName).getId());
            systemInfo.releaseBackends(info.getClusterName(), true);
            unprotectDropCluster(info, true);
            editLog.logDropCluster(info);
        } finally {
            writeUnlock();
        }

    }

    private void unprotectDropCluster(ClusterInfo info, boolean log) {
        systemInfo.releaseBackends(info.getClusterName(), log);
        idToCluster.remove(info.getClusterId());
        nameToCluster.remove(info.getClusterName());
        final Database infoSchemaDb = nameToDb.get(InfoSchemaDb.getFullInfoSchemaDbName(info.getClusterName()));
        nameToDb.remove(infoSchemaDb.getName());
        idToDb.remove(infoSchemaDb.getId());
    }

    public void replayDropCluster(ClusterInfo info) {
        writeLock();
        unprotectDropCluster(info, false);
        writeUnlock();
    }

    public void replayUpdateCluster(ClusterInfo info) {
        writeLock();
        final Cluster cluster = nameToCluster.get(info.getClusterName());
        cluster.setBackendIdList(info.getBackendIdList());
        writeUnlock();
    }

    /**
     * modify cluster: Expansion or shrink
     *
     * @param stmt
     * @throws DdlException
     */
    public void processModityCluster(AlterClusterStmt stmt) throws DdlException {
        final String clusterName = stmt.getAlterClusterName();
        final int newInstanceNum = stmt.getInstanceNum();
        writeLock();
        try {
            if (!nameToCluster.containsKey(clusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
            }
            final int oldInstanceNum = nameToCluster.get(clusterName).getBackendIdList().size();

            if (newInstanceNum > oldInstanceNum) {
                final List<Long> backendList = systemInfo.calculateExpansionBackends(clusterName,
                        newInstanceNum - oldInstanceNum);
                if (backendList == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_BE_NOT_ENOUGH);
                }
                final Cluster cluster = nameToCluster.get(clusterName);
                cluster.addBackends(backendList);
                final ClusterInfo ci = new ClusterInfo(clusterName, nameToCluster.get(clusterName).getId());
                ci.setBackendIdList(cluster.getBackendIdList());
                editLog.logModifyCluster(ci);
            } else if (newInstanceNum < oldInstanceNum) {
                final List<Long> backendList = systemInfo.calculateDecommissionBackends(clusterName,
                        oldInstanceNum - newInstanceNum);
                if (backendList == null || backendList.size() == 0) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_BACKEND_ERROR);
                }

                for (Long id : backendList) {
                    final Backend backend = systemInfo.getBackend(id);
                    if (backend.isDecommissioned()) {
                        if (backend.getDecommissionType() == DecomissionType.ClusterDecomission) {
                            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_ALTER_BE_IN_CLUSTER_DECOMMISSION,
                                    clusterName);
                        } else {
                            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_ALTER_BE_IN_SYSTEM_DECOMMISSION,
                                    clusterName);
                        }
                    }
                }

                List<String> hostPortList = Lists.newArrayList();
                for (Long id : backendList) {
                    final Backend backend = systemInfo.getBackend(id);
                    hostPortList.add(new StringBuilder().append(backend.getHost()).append(":")
                            .append(backend.getHeartbeatPort()).toString());
                }
                // cluster modify reused system Decommission, but add
                // DecommissionType to
                // not drop backend when finish decommission
                final DecommissionBackendClause clause = new DecommissionBackendClause(hostPortList);
                try {
                    clause.analyze(null);
                    clause.setType(DecomissionType.ClusterDecomission);
                    AlterSystemStmt alterStmt = new AlterSystemStmt(clause);
                    alterStmt.setClusterName(clusterName);
                    this.alter.processAlterCluster(alterStmt);
                } catch (AnalysisException e) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_BACKEND_ERROR);
                }

            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_ALTER_BE_NO_CHANGE, newInstanceNum);
            }

        } finally {
            writeUnlock();
        }
    }

    /**
     *
     * @param ci
     * @throws DdlException
     */
    private void unprotectModifyCluster(ClusterInfo ci) throws DdlException {
        if (ci.getNewInstanceNum() > ci.getInstanceNum()) {
            final List<Long> backendList = systemInfo.calculateExpansionBackends(ci.getClusterName(),
                    ci.getNewInstanceNum() - ci.getInstanceNum());
            if (backendList == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_BE_NOT_ENOUGH);
            }
            final Cluster cluster = nameToCluster.get(ci.getClusterName());
            cluster.addBackends(backendList);
        } else if (ci.getNewInstanceNum() < ci.getInstanceNum()) {
            final List<Long> backendList = systemInfo.calculateDecommissionBackends(ci.getClusterName(),
                    ci.getInstanceNum() - ci.getNewInstanceNum());
            List<String> hostPortList = Lists.newArrayList();
            for (Long id : backendList) {
                final Backend backend = systemInfo.getBackend(id);
                hostPortList.add(backend.getHost());
                hostPortList.add(String.valueOf(backend.getBePort()));
            }
            // cluster modify reused system Decommission, but add
            // DecommissionType to
            // not drop backend when finish decommission
            final DecommissionBackendClause clause = new DecommissionBackendClause(hostPortList);
            clause.setType(DecomissionType.ClusterDecomission);
            AlterSystemStmt stmt = new AlterSystemStmt(clause);
            stmt.setClusterName(ci.getClusterName());

            this.alter.processAlterCluster(stmt);
        } else {

        }
    }

    /**
     * replay modify cluster
     *
     * @param ci
     * @throws DdlException
     */
    public void replayModifyCluster(ClusterInfo ci) throws DdlException {
        writeLock();
        unprotectModifyCluster(ci);
        writeUnlock();
    }

    /**
     *
     * @param ctx
     * @param clusterName
     * @throws DdlException
     */
    public void changeCluster(ConnectContext ctx, String clusterName) throws DdlException {
        if (!nameToCluster.containsKey(clusterName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
        }
        if (!userPropertyMgr.isAdmin(ctx.getUser())) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_AUTHORITY);
        }
        ctx.setCluster(clusterName);
    }

    /**
     * migrate db to link des cluster
     *
     * @param stmt
     * @throws DdlException
     */
    public void migrateDb(MigrateDbStmt stmt) throws DdlException {
        final String srcClusterName = stmt.getSrcCluster();
        final String desClusterName = stmt.getDesCluster();
        final String srcDbName = stmt.getSrcDb();
        final String desDbName = stmt.getDesDb();

        writeLock();
        try {
            if (!nameToCluster.containsKey(srcClusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_SRC_CLUSTER_NO_EXIT, srcClusterName);
            }
            if (!nameToCluster.containsKey(desClusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DES_CLUSTER_NO_EXIT, desClusterName);
            }

            if (srcClusterName.equals(desClusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_MIGRATE_SAME_CLUSTER);
            }

            final Cluster srcCluster = this.nameToCluster.get(srcClusterName);
            if (!srcCluster.containDb(srcDbName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_SRC_DB_NO_EXIT, srcDbName);
            }
            final Cluster desCluster = this.nameToCluster.get(desClusterName);
            if (!desCluster.containLink(desDbName, srcDbName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_MIGRATION_NO_LINK, srcDbName, desDbName);
            }

            final Database db = nameToDb.get(srcDbName);
            final int maxReplica = getDbMaxReplicaNum(db);
            if (maxReplica > desCluster.getBackendIdList().size()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_MIGRATE_BE_NOT_ENOUGH, desClusterName);
            }
            if (db.getDbState() == DbState.LINK) {
                final BaseParam param = new BaseParam();
                param.addStringParam(desDbName);
                param.addLongParam(db.getId());
                param.addStringParam(srcDbName);
                param.addStringParam(desClusterName);
                param.addStringParam(srcClusterName);
                nameToDb.remove(db.getName());
                srcCluster.removeDb(db.getName(), db.getId());
                desCluster.removeLinkDb(param);
                desCluster.addDb(desDbName, db.getId());
                db.writeLock();
                db.setDbState(DbState.MOVE);
                db.setClusterName(desClusterName);
                db.setName(desDbName);
                db.setAttachDb(srcDbName);
                db.writeUnlock();
                editLog.logMigrateCluster(param);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_MIGRATION_NO_LINK, srcDbName, desDbName);
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * return max replicationNum in db
     * @param db
     * @return
     */
    private int getDbMaxReplicaNum(Database db) {
        int ret = 0;
        final Set<String> tableNames = db.getTableNamesWithLock();
        for (String tableName : tableNames) {
            try {
                db.readLock();
                Table table = db.getTable(tableName);
                if (table == null || table.getType() != TableType.OLAP) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                for (Partition partition : olapTable.getPartitions()) {
                    long partitionId = partition.getId();
                    short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                    if (ret < replicationNum) {
                        ret = replicationNum;
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
        return ret;
    }

    public void replayMigrateDb(BaseParam param) {
        final String desDbName = param.getStringParam();
        final String srcDbName = param.getStringParam(1);
        final String desClusterName = param.getStringParam(2);
        final String srcClusterName = param.getStringParam(3);
        try {
            writeLock();
            final Cluster desCluster = this.nameToCluster.get(desClusterName);
            final Cluster srcCluster = this.nameToCluster.get(srcClusterName);
            final Database db = nameToDb.get(srcDbName);
            if (db.getDbState() == DbState.LINK) {
                nameToDb.remove(db.getName());
                srcCluster.removeDb(db.getName(), db.getId());
                desCluster.removeLinkDb(param);
                desCluster.addDb(param.getStringParam(), db.getId());

                db.writeLock();
                db.setName(desDbName);
                db.setAttachDb(srcDbName);
                db.setDbState(DbState.MOVE);
                db.setClusterName(desClusterName);
                db.writeUnlock();
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayLinkDb(BaseParam param) {
        final String desClusterName = param.getStringParam(2);
        final String srcDbName = param.getStringParam(1);
        final String desDbName = param.getStringParam();

        try {
            writeLock();
            final Cluster desCluster = this.nameToCluster.get(desClusterName);
            final Database srcDb = nameToDb.get(srcDbName);
            srcDb.writeLock();
            srcDb.increaseRef();
            srcDb.setDbState(DbState.LINK);
            srcDb.setAttachDb(desDbName);
            srcDb.writeUnlock();
            desCluster.addLinkDb(param);
            nameToDb.put(desDbName, srcDb);
        } finally {
            writeUnlock();
        }
    }

    /**
     * link src db to des db ,and increase src db ref。 we use java's quotation
     * Mechanism to realize db hard links
     *
     * @param stmt
     * @throws DdlException
     */
    public void linkDb(LinkDbStmt stmt) throws DdlException {
        final String srcClusterName = stmt.getSrcCluster();
        final String desClusterName = stmt.getDesCluster();
        final String srcDbName = stmt.getSrcDb();
        final String desDbName = stmt.getDesDb();
        writeLock();
        try {
            if (!nameToCluster.containsKey(srcClusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_SRC_CLUSTER_NO_EXIT, srcClusterName);
            }

            if (!nameToCluster.containsKey(desClusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DES_CLUSTER_NO_EXIT, desClusterName);
            }

            if (srcClusterName.equals(desClusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_MIGRATE_SAME_CLUSTER);
            }

            if (nameToDb.containsKey(desDbName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, desDbName);
            }

            final Cluster srcCluster = this.nameToCluster.get(srcClusterName);
            final Cluster desCluster = this.nameToCluster.get(desClusterName);

            if (!srcCluster.containDb(srcDbName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_SRC_DB_NO_EXIT, srcDbName);
            }
            final Database srcDb = nameToDb.get(srcDbName);

            if (srcDb.getDbState() != DbState.NORMAL) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                        ClusterNamespace.getDbNameFromFullName(srcDbName));
            }

            srcDb.writeLock();
            srcDb.increaseRef();
            srcDb.setDbState(DbState.LINK);
            srcDb.setAttachDb(desDbName);
            srcDb.writeUnlock();

            final long id = getNextId();
            final BaseParam param = new BaseParam();
            param.addStringParam(desDbName);
            param.addStringParam(srcDbName);
            param.addLongParam(id);
            param.addLongParam(srcDb.getId());
            param.addStringParam(desClusterName);
            param.addStringParam(srcClusterName);
            desCluster.addLinkDb(param);
            nameToDb.put(desDbName, srcDb);
            editLog.logLinkCluster(param);
        } finally {
            writeUnlock();
        }
    }

    public Cluster getCluster(String cluster) {
        return nameToCluster.get(cluster);
    }

    public List<String> getClusterNames() {
        return new ArrayList<String>(nameToCluster.keySet());
    }

    /**
     * get migrate progress , when finish migration, next clonecheck will reset dbState
     * @return
     */
    public Set<BaseParam> getMigrations() {
        final Set<BaseParam> infos = Sets.newHashSet();
        for (Database db : nameToDb.values()) {
            if (db.getDbState() == DbState.MOVE) {
                int tabletTotal = 0;
                int tabletQuorum = 0;
                final Set<Long> ids = Sets.newHashSet(systemInfo.getClusterBackendIds(db.getClusterName()));
                final Set<String> tableNames = db.getTableNamesWithLock();
                for (String tableName : tableNames) {
                    db.readLock();
                    try {
                        Table table = db.getTable(tableName);
                        if (table == null || table.getType() != TableType.OLAP) {
                            continue;
                        }

                        OlapTable olapTable = (OlapTable) table;
                        for (Partition partition : olapTable.getPartitions()) {
                            final short replicationNum = olapTable.getPartitionInfo()
                                    .getReplicationNum(partition.getId());
                            for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                                if (materializedIndex.getState() != IndexState.NORMAL) {
                                    continue;
                                }
                                for (Tablet tablet : materializedIndex.getTablets()) {
                                    int replicaNum = 0;
                                    int quorum = replicationNum / 2 + 1;
                                    for (Replica replica : tablet.getReplicas()) {
                                        if (replica.getState() != ReplicaState.CLONE
                                                && ids.contains(replica.getBackendId())) {
                                            replicaNum++;
                                        }
                                    }
                                    if (replicaNum > quorum) {
                                        replicaNum = quorum;
                                    }

                                    tabletQuorum = tabletQuorum + replicaNum;
                                    tabletTotal = tabletTotal + quorum;
                                }
                            }
                        }
                    } finally {
                        db.readUnlock();
                    }
                }
                final BaseParam info = new BaseParam();
                info.addStringParam(db.getClusterName());
                info.addStringParam(db.getAttachDb());
                info.addStringParam(db.getName());
                final float percentage = tabletTotal > 0 ? (float) tabletQuorum / (float) tabletTotal : 0f;
                info.addFloatParam(percentage);
                infos.add(info);
                db.getDbState();
            }
        }

        return infos;
    }

    public long loadCluster(DataInputStream dis, long checksum) throws IOException, DdlException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            int dbCount = dis.readInt();
            checksum ^= dbCount;
            for (long i = 0; i < dbCount; ++i) {
                final Cluster cluster = new Cluster();
                cluster.readFields(dis);
                checksum ^= cluster.getId();
                final InfoSchemaDb db = new InfoSchemaDb(cluster.getName());
                db.setClusterName(cluster.getName());
                idToDb.put(db.getId(), db);
                nameToDb.put(db.getName(), db);
                cluster.addDb(db.getName(), db.getId());
                idToCluster.put(cluster.getId(), cluster);
                nameToCluster.put(cluster.getName(), cluster);
            }
        }
        return checksum;
    }

    private void initDefaultCluster() {
        final List<Long> backendList = Lists.newArrayList();
        final List<Backend> defaultClusterBackends = systemInfo.getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
        for (Backend backend : defaultClusterBackends) {
            backendList.add(backend.getId());
        }

        final long id = getNextId();
        final Cluster cluster = new Cluster();
        cluster.setName(SystemInfoService.DEFAULT_CLUSTER);
        cluster.setId(id);

        if (backendList.size() != 0) {
            // make sure one host hold only one backend.
            Set<String> beHost = Sets.newHashSet();
            for (Backend be : defaultClusterBackends) {
                if (beHost.contains(be.getHost())) {
                    // we can not handle this situation automatically.
                    LOG.error("found more than one backends in same host: {}", be.getHost());
                    System.exit(-1);
                } else {
                    beHost.add(be.getHost());
                }
            }

            // we create default_cluster only if we had existing backends.
            // this could only happend when we upgrade Palo from version 2.4 to 3.x.
            cluster.setBackendIdList(backendList);
            unprotectCreateCluster(cluster);
            for (Database db : idToDb.values()) {
                db.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
                cluster.addDb(db.getName(), db.getId());
            }
        }
        // no matter default_cluster is created or not,
        // mark isDefaultClusterCreated as true
        isDefaultClusterCreated = true;
        editLog.logCreateCluster(cluster);
    }

    public void replayUpdateDb(DatabaseInfo info) {
        final Database db = nameToDb.get(info.getDbName());
        db.setClusterName(info.getClusterName());
        db.setDbState(info.getDbState());
    }

    public long saveCluster(DataOutputStream dos, long checksum) throws IOException {
        final int clusterCount = idToCluster.size();
        checksum ^= clusterCount;
        dos.writeInt(clusterCount);
        for (Map.Entry<Long, Cluster> entry : idToCluster.entrySet()) {
            long clusterId = entry.getKey();
            if (clusterId >= NEXT_ID_INIT_VALUE) {
                checksum ^= clusterId;
                final Cluster cluster = entry.getValue();
                cluster.write(dos);
            }
        }
        return checksum;
    }

    public long saveBrokers(DataOutputStream dos, long checksum) throws IOException {
        Map<String, List<BrokerAddress>> addressListMap = brokerMgr.getAddressListMap();
        int size = addressListMap.size();
        checksum ^= size;
        dos.writeInt(size);

        for (Map.Entry<String, List<BrokerAddress>> entry : addressListMap.entrySet()) {
            Text.writeString(dos, entry.getKey());
            final List<BrokerAddress> addrs = entry.getValue();
            size = addrs.size();
            checksum ^= size;
            dos.writeInt(size);
            for (BrokerAddress addr : addrs) {
                addr.write(dos);
            }
        }

        return checksum;
    }

    public long loadBrokers(DataInputStream dis, long checksum) throws IOException, DdlException {
        if (getJournalVersion() >= FeMetaVersion.VERSION_31) {
            int count = dis.readInt();
            checksum ^= count;
            for (long i = 0; i < count; ++i) {
                String brokerName = Text.readString(dis);
                int size = dis.readInt();
                checksum ^= size;
                List<BrokerAddress> addrs = Lists.newArrayList();
                for (int j = 0; j < size; j++) {
                    BrokerAddress addr = BrokerAddress.readIn(dis);
                    addrs.add(addr);
                }
                brokerMgr.replayAddBrokers(brokerName, addrs);
            }
            LOG.info("finished replay brokerMgr from image");
        }
        return checksum;
    }

    public void replayUpdateClusterAndBackends(UpdateClusterAndBackends info) {
        for (long id : info.getBackendList()) {
            final Backend backend = systemInfo.getBackend(id);
            writeLock();
            try {
                final Cluster cluster = nameToCluster.get(backend.getOwnerClusterName());
                cluster.removeBackend(id);
            } finally {
                writeUnlock();  
            }
            backend.setDecommissioned(false);
            backend.clearClusterName();
            backend.setBackendState(BackendState.free);            
        }
    }

}
