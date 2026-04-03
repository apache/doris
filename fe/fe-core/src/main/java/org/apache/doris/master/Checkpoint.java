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

package org.apache.doris.master;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.CheckpointException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.HttpURLUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.monitor.jvm.JvmService;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.MetaCleaner;
import org.apache.doris.persist.Storage;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.system.Frontend;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Checkpoint daemon is running on master node. handle the checkpoint work for palo.
 */
public class Checkpoint extends MasterDaemon {
    public static final Logger LOG = LogManager.getLogger(Checkpoint.class);
    private static final int PUT_TIMEOUT_SECOND = 3600;
    private static final int CONNECT_TIMEOUT_SECOND = 1;
    private static final int READ_TIMEOUT_SECOND = 1;

    private Env env;
    private String imageDir;
    private EditLog editLog;
    private int memoryNotEnoughCount = 0;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Checkpoint(EditLog editLog) {
        super("leaderCheckpointer", FeConstants.checkpoint_interval_second * 1000L);
        this.imageDir = Env.getServingEnv().getImageDir();
        this.editLog = editLog;
    }

    public static class NullOutputStream extends OutputStream {
        public void write(byte[] b, int off, int len) throws IOException {
        }

        public void write(int b) throws IOException {
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            doCheckpoint();
        } catch (CheckpointException e) {
            LOG.warn("failed to do checkpoint.", e);
        }
    }

    // public for unit test, so that we can trigger checkpoint manually.
    // DO NOT call it manually outside the unit test.
    public synchronized void doCheckpoint() throws CheckpointException {
        if (!Config.enable_checkpoint) {
            LOG.warn("checkpoint is disabled. please enable the config 'enable_checkpoint'.");
            return;
        }

        if (!Env.getServingEnv().isHttpReady()) {
            LOG.info("Http server is not ready.");
            return;
        }
        long imageVersion = 0;
        long checkPointVersion = 0;
        Storage storage = null;
        try {
            storage = new Storage(imageDir);
            // get max image version
            imageVersion = storage.getLatestImageSeq();
            long latestImageCreateTime = storage.getLatestImageCreateTime();
            // get max finalized journal id
            checkPointVersion = editLog.getFinalizedJournalId();
            LOG.info("last checkpoint journal id: {}, create timestamp: {}. current finalized journal id: {}",
                    imageVersion, latestImageCreateTime, checkPointVersion);
            if (imageVersion < checkPointVersion) {
                LOG.info("Trigger checkpoint since last checkpoint journal id: {} is less than "
                        + "current finalized journal id: {}", imageVersion, checkPointVersion);
            } else if (Config.isCloudMode() && Config.cloud_checkpoint_image_stale_threshold_seconds > 0
                    && latestImageCreateTime > 0 && ((System.currentTimeMillis() - latestImageCreateTime)
                    >= Config.cloud_checkpoint_image_stale_threshold_seconds * 1000L)) {
                // No new finalized journals beyond the latest image.
                // But in cloud mode, we may still want to force a checkpoint if the latest image file is expired.
                // This helps that image can keep the newer table version, partition version, tablet stats.
                LOG.info("Trigger checkpoint in cloud mode because latest image is expired. "
                        + "latestImageSeq: {}, latestImageCreateTime: {}", imageVersion, latestImageCreateTime);
            } else {
                return;
            }
        } catch (Throwable e) {
            LOG.warn("Save image failed: " + e.getMessage(), e);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_WRITE_FAILED.increase(1L);
            }
            return;
        }

        try {
            checkMemoryEnoughToDoCheckpoint();
        } catch (Throwable t) {
            LOG.warn("Save image failed: " + t.getMessage(), t);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_WRITE_FAILED.increase(1L);
            }
            return;
        }

        // generate new image file
        long replayedJournalId = -1;
        LOG.info("begin to generate new image: image.{}", checkPointVersion);
        env = Env.getCurrentEnv();
        env.setEditLog(editLog);
        createStaticFieldForCkpt();
        boolean exceptionCaught = false;
        String latestImageFilePath = null;
        try {
            env.loadImage(imageDir);
            env.replayJournal(checkPointVersion);
            if (env.getReplayedJournalId() != checkPointVersion) {
                throw new CheckpointException(
                        String.format("checkpoint version should be %d," + " actual replayed journal id is %d",
                                checkPointVersion, env.getReplayedJournalId()));
            }
            env.postProcessAfterMetadataReplayed(false);
            postProcessCloudMetadata();
            latestImageFilePath = env.saveImage();
            replayedJournalId = env.getReplayedJournalId();

            // destroy checkpoint catalog, reclaim memory
            env = null;
            Env.destroyCheckpoint();
            destroyStaticFieldForCkpt();

            // Load image to verify if the newly generated image file is valid
            // If success, do all the following jobs
            // If failed, just return
            env = Env.getCurrentEnv();
            createStaticFieldForCkpt();
            env.loadImage(imageDir);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_WRITE_SUCCESS.increase(1L);
            }
            LOG.info("checkpoint finished save image.{}", replayedJournalId);
        } catch (Throwable e) {
            exceptionCaught = true;
            LOG.warn("Save image failed: " + e.getMessage(), e);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_WRITE_FAILED.increase(1L);
            }
            throw new CheckpointException(e.getMessage(), e);
        } finally {
            // destroy checkpoint catalog, reclaim memory
            env = null;
            Env.destroyCheckpoint();
            destroyStaticFieldForCkpt();
            // if new image generated && exception caught, delete the latest image here
            // delete the newest image file, cuz it is invalid
            if ((!Strings.isNullOrEmpty(latestImageFilePath)) && exceptionCaught) {
                MetaCleaner cleaner = new MetaCleaner(Config.meta_dir + "/image");
                try {
                    cleaner.cleanTheLatestInvalidImageFile(latestImageFilePath);
                    if (MetricRepo.isInit) {
                        MetricRepo.COUNTER_IMAGE_CLEAN_SUCCESS.increase(1L);
                    }
                } catch (Throwable t) {
                    LOG.warn("Delete old image failed: " + t.getMessage(), t);
                    if (MetricRepo.isInit) {
                        MetricRepo.COUNTER_IMAGE_CLEAN_FAILED.increase(1L);
                    }
                }
            }
        }

        // push image file to all the other non master nodes
        // DO NOT get other nodes from HaProtocol, because node may not in bdbje replication group yet.
        List<Frontend> allFrontends = Env.getServingEnv().getFrontends(null);
        int successPushed = 0;
        int otherNodesCount = 0;
        if (!allFrontends.isEmpty()) {
            otherNodesCount = allFrontends.size() - 1; // skip master itself
            for (Frontend fe : allFrontends) {
                String host = fe.getHost();
                if (host.equals(Env.getServingEnv().getMasterHost())) {
                    // skip master itself
                    continue;
                }
                int port = Config.http_port;

                String url = "http://" + NetUtils.getHostPortInAccessibleFormat(host, port) + "/put?version=" + replayedJournalId
                        + "&port=" + port;
                LOG.info("Put image:{}", url);

                try {
                    ResponseBody responseBody = MetaHelper.doGet(url, PUT_TIMEOUT_SECOND * 1000, Object.class);
                    if (responseBody.getCode() == RestApiStatusCode.OK.code) {
                        successPushed++;
                    } else {
                        LOG.warn("Failed when pushing image file. url = {},responseBody = {}", url, responseBody);
                    }
                } catch (IOException e) {
                    LOG.warn("Exception when pushing image file. url = {}", url, e);
                }
            }

            LOG.info("push image.{} to other nodes. totally {} nodes, push succeeded {} nodes",
                    replayedJournalId, otherNodesCount, successPushed);
        }
        if (successPushed == otherNodesCount) {
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_PUSH_SUCCESS.increase(1L);
            }
        } else {
            LOG.warn("Push image failed: totally {} nodes, push succeeded {} nodes", otherNodesCount, successPushed);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_PUSH_FAILED.increase(1L);
            }
        }

        lock.writeLock().lock();
        try {
            deleteOldJournalsAndImages(successPushed, otherNodesCount, allFrontends, storage, checkPointVersion);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void deleteOldJournalsAndImages(int successPushed, int otherNodesCount, List<Frontend> allFrontends,
            Storage storage, long checkPointVersion) {
        // Delete old journals
        // only do this when the new image succeed in pushing to other nodes
        if (successPushed == otherNodesCount) {
            try {
                long minOtherNodesJournalId = Long.MAX_VALUE;
                // Actually, storage.getLatestValidatedImageSeq returns number before this
                // checkpoint.
                long deleteVersion = storage.getLatestValidatedImageSeq();
                if (successPushed > 0) {
                    for (Frontend fe : allFrontends) {
                        String host = fe.getHost();
                        if (host.equals(Env.getServingEnv().getMasterHost())) {
                            // skip master itself
                            continue;
                        }
                        int port = Config.http_port;
                        String idURL;
                        HttpURLConnection conn = null;
                        try {
                            /*
                             * get current replayed journal id of each non-master nodes.
                             * when we delete bdb database, we cannot delete db newer than
                             * any non-master node's current replayed journal id. otherwise,
                             * this lagging node can never get the deleted journal.
                             */
                            idURL = "http://" + NetUtils.getHostPortInAccessibleFormat(host, port) + "/journal_id";
                            conn = HttpURLUtil.getConnectionWithNodeIdent(idURL);
                            conn.setConnectTimeout(CONNECT_TIMEOUT_SECOND * 1000);
                            conn.setReadTimeout(READ_TIMEOUT_SECOND * 1000);
                            String idString = conn.getHeaderField("id");
                            long id = Long.parseLong(idString);
                            if (minOtherNodesJournalId > id) {
                                minOtherNodesJournalId = id;
                            }
                        } catch (Throwable e) {
                            throw new CheckpointException(String.format("Exception when getting current replayed"
                                    + " journal id. host=%s, port=%d", host, port), e);
                        } finally {
                            if (conn != null) {
                                conn.disconnect();
                            }
                        }
                    }
                    deleteVersion = Math.min(minOtherNodesJournalId, deleteVersion);
                }

                editLog.deleteJournals(deleteVersion + 1);
                if (MetricRepo.isInit) {
                    MetricRepo.COUNTER_EDIT_LOG_CLEAN_SUCCESS.increase(1L);
                    MetricRepo.COUNTER_CURRENT_EDIT_LOG_SIZE_BYTES.reset();
                    MetricRepo.COUNTER_EDIT_LOG_CURRENT.update(editLog.getEditLogNum());
                }
                LOG.info("journals <= {} are deleted. image version {}, other nodes min version {}",
                        deleteVersion, checkPointVersion, minOtherNodesJournalId);
            } catch (Throwable t) {
                LOG.warn("Delete old edit log failed: " + t.getMessage(), t);
                if (MetricRepo.isInit) {
                    MetricRepo.COUNTER_EDIT_LOG_CLEAN_FAILED.increase(1L);
                }
            }
        }

        // Delete old image files
        MetaCleaner cleaner = new MetaCleaner(Config.meta_dir + "/image");
        try {
            cleaner.clean();
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_CLEAN_SUCCESS.increase(1L);
            }
        } catch (Throwable e) {
            LOG.warn("Master delete old image file fail.", e);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_IMAGE_CLEAN_FAILED.increase(1L);
            }
        }
    }

    // Some classes use static variables to store information,
    // and we need to generate new temporary objects for these static variables
    // during the checkpoint process to cope with changes made to these variables
    // during the checkpoint process
    private void createStaticFieldForCkpt() {
        VariableMgr.createDefaultSessionVariableForCkpt();
    }

    private void destroyStaticFieldForCkpt() {
        VariableMgr.destroyDefaultSessionVariableForCkpt();
    }

    /*
     * Check whether can we do the checkpoint due to the memory used percent.
     */
    private void checkMemoryEnoughToDoCheckpoint() throws CheckpointException {
        long memUsedPercent = getMemoryUsedPercent();
        LOG.info("get jvm memory used percent: {} %", memUsedPercent);

        if (memUsedPercent <= Config.metadata_checkpoint_memory_threshold || Config.force_do_metadata_checkpoint) {
            memoryNotEnoughCount = 0;
            return;
        }

        memoryNotEnoughCount += 1;
        if (memoryNotEnoughCount != Config.checkpoint_manual_gc_threshold) {
            throw new CheckpointException(String.format(
                    "the memory used percent %d exceed the checkpoint memory threshold: %d, exceeded count: %d",
                    memUsedPercent, Config.metadata_checkpoint_memory_threshold, memoryNotEnoughCount));
        }

        LOG.warn("the 'not enough memory count' has reached the manual gc threshold {}",
                Config.checkpoint_manual_gc_threshold);
        System.gc();
        checkMemoryEnoughToDoCheckpoint();
    }

    /*
     * Get the used percent of jvm memory pool.
     * If old mem pool does not found(It probably should not happen), use heap mem usage instead.
     * heap mem is slightly larger than old mem pool usage.
     */
    private long getMemoryUsedPercent() {
        JvmService jvmService = new JvmService();
        JvmStats jvmStats = jvmService.stats();
        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        MemoryPool oldMemPool = null;
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase("old")) {
                oldMemPool = memPool;
                break;
            }
        }
        if (oldMemPool != null) {
            long used = oldMemPool.getUsed().getBytes();
            long max = oldMemPool.getMax().getBytes();
            return used * 100 / max;
        } else {
            LOG.warn("failed to get jvm old mem pool, use heap usage instead");
            long used = jvmStats.getMem().getHeapUsed().getBytes();
            long max = jvmStats.getMem().getHeapMax().getBytes();
            return used * 100 / max;
        }
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    private void postProcessCloudMetadata() {
        if (Config.isNotCloudMode()) {
            return;
        }
        Env servingEnv = Env.getServingEnv();
        if (servingEnv == null) {
            LOG.warn("serving env is null, skip process cloud metadata for checkpoint");
            return;
        }
        long start = System.currentTimeMillis();
        for (Database db : env.getInternalCatalog().getDbs()) {
            Database servingDb = servingEnv.getInternalCatalog().getDbNullable(db.getId());
            if (servingDb == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("serving db is null. dbId: {}, dbName: {}", db.getId(), db.getFullName());
                }
                continue;
            }

            for (Table table : db.getTables()) {
                Table servingTable = servingDb.getTableNullable(table.getId());
                if (servingTable == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("serving table is null. dbId: {}, table: {}", db.getId(), table);
                    }
                    continue;
                }
                if (!(table instanceof OlapTable) || !(servingTable instanceof OlapTable)) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                OlapTable servingOlapTable = (OlapTable) servingTable;

                List<Partition> partitions = olapTable.getAllPartitions();
                for (Partition partition : partitions) {
                    Partition servingPartition = servingOlapTable.getPartition(partition.getId());
                    if (servingPartition == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("serving partition is null. tableId: {}, partitionId: {}", table.getId(),
                                    partition.getId());
                        }
                        continue;
                    }
                    // set tablet stats
                    setTabletStats(table.getId(), partition, servingPartition);
                }
            }
        }
        LOG.info("post process cloud metadata for checkpoint finished. cost {} ms", System.currentTimeMillis() - start);
    }

    private void setTabletStats(long tableId, Partition partition, Partition servingPartition) {
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            MaterializedIndex servingIndex = servingPartition.getIndex(index.getId());
            if (servingIndex == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("serving index is null. tableId: {}, partitionId: {}, indexId: {}", tableId,
                            partition.getId(), index.getId());
                }
                continue;
            }
            for (Tablet tablet : index.getTablets()) {
                Tablet servingTablet = servingIndex.getTablet(tablet.getId());
                if (servingTablet == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("serving tablet is null. tableId: {}, partitionId: {}, indexId: {}, tabletId: {}",
                                tableId, partition.getId(), index.getId(), tablet.getId());
                    }
                    continue;
                }
                for (Replica replica : tablet.getReplicas()) {
                    Replica servingReplica = servingTablet.getReplicaById(replica.getId());
                    if (servingReplica == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("serving replica is null. tableId: {}, partitionId: {}, indexId: {}, "
                                            + "tabletId: {}, replicaId: {}", tableId, partition.getId(), index.getId(),
                                    tablet.getId(), replica.getId());
                        }
                        continue;
                    }
                    replica.setDataSize(servingReplica.getDataSize());
                    replica.setRowsetCount(servingReplica.getRowsetCount());
                    replica.setSegmentCount(servingReplica.getSegmentCount());
                    replica.setRowCount(servingReplica.getRowCount());
                    replica.setLocalInvertedIndexSize(servingReplica.getLocalInvertedIndexSize());
                    replica.setLocalSegmentSize(servingReplica.getLocalSegmentSize());
                }
            }
        }
    }
}
