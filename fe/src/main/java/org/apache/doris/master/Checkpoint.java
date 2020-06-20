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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.monitor.jvm.JvmService;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.MetaCleaner;
import org.apache.doris.persist.Storage;
import org.apache.doris.system.Frontend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

/**
 * Checkpoint daemon is running on master node. handle the checkpoint work for palo. 
 */
public class Checkpoint extends MasterDaemon {
    public static final Logger LOG = LogManager.getLogger(Checkpoint.class);
    private static final int PUT_TIMEOUT_SECOND = 3600;
    private static final int CONNECT_TIMEOUT_SECOND = 1;
    private static final int READ_TIMEOUT_SECOND = 1;
    
    private Catalog catalog;
    private String imageDir;
    private EditLog editLog;

    public Checkpoint(EditLog editLog) {
        super("leaderCheckpointer", FeConstants.checkpoint_interval_second * 1000L);
        this.imageDir = Catalog.getServingCatalog().getImageDir();
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
        long imageVersion = 0;
        long checkPointVersion = 0;
        Storage storage = null;
        try {
            storage = new Storage(imageDir);
            // get max image version
            imageVersion = storage.getImageSeq();
            // get max finalized journal id
            checkPointVersion = editLog.getFinalizedJournalId();
            LOG.info("checkpoint imageVersion {}, checkPointVersion {}", imageVersion, checkPointVersion);
            if (imageVersion >= checkPointVersion) {
                return;
            }
        } catch (IOException e) {
            LOG.error("Does not get storage info", e);
            return;
        }
        
        if (!checkMemoryEnoughToDoCheckpoint()) {
            return;
        }
       
        long replayedJournalId = -1;
        // generate new image file
        LOG.info("begin to generate new image: image.{}", checkPointVersion);
        catalog = Catalog.getCurrentCatalog();
        catalog.setEditLog(editLog);
        try {
            catalog.loadImage(imageDir);
            catalog.replayJournal(checkPointVersion);
            if (catalog.getReplayedJournalId() != checkPointVersion) {
                LOG.error("checkpoint version should be {}, actual replayed journal id is {}",
                          checkPointVersion, catalog.getReplayedJournalId());
                return;
            }
            catalog.fixBugAfterMetadataReplayed(false);

            catalog.saveImage();
            replayedJournalId = catalog.getReplayedJournalId();
            if (MetricRepo.isInit.get()) {
                MetricRepo.COUNTER_IMAGE_WRITE.increase(1L);
            }
            LOG.info("checkpoint finished save image.{}", replayedJournalId);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Exception when generate new image file", e);
            return;
        } finally {
            // destroy checkpoint catalog, reclaim memory
            catalog = null;
            Catalog.destroyCheckpoint(); 
        }
        
        // push image file to all the other non master nodes
        // DO NOT get other nodes from HaProtocol, because node may not in bdbje replication group yet.
        List<Frontend> allFrontends = Catalog.getServingCatalog().getFrontends(null);
        int successPushed = 0;
        int otherNodesCount = 0;
        if (!allFrontends.isEmpty()) {
            otherNodesCount = allFrontends.size() - 1; // skip master itself
            for (Frontend fe : allFrontends) {
                String host = fe.getHost();
                if (host.equals(Catalog.getServingCatalog().getMasterIp())) {
                    // skip master itself
                    continue;
                }
                int port = Config.http_port;
                
                String url = "http://" + host + ":" + port + "/put?version=" + replayedJournalId
                        + "&port=" + port;
                LOG.info("Put image:{}", url);

                try {
                    MetaHelper.getRemoteFile(url, PUT_TIMEOUT_SECOND * 1000, new NullOutputStream());
                    successPushed++;
                } catch (IOException e) {
                    LOG.error("Exception when pushing image file. url = {}", url, e);
                }
            }
            
            LOG.info("push image.{} to other nodes. totally {} nodes, push successed {} nodes",
                     replayedJournalId, otherNodesCount, successPushed);
        }
        
        // Delete old journals
        if (successPushed == otherNodesCount) {
            long minOtherNodesJournalId = Long.MAX_VALUE;
            long deleteVersion = checkPointVersion;
            if (successPushed > 0) {
                for (Frontend fe : allFrontends) {
                    String host = fe.getHost();
                    if (host.equals(Catalog.getServingCatalog().getMasterIp())) {
                        // skip master itself
                        continue;
                    }
                    int port = Config.http_port;
                    URL idURL;
                    HttpURLConnection conn = null;
                    try {
                        /*
                         * get current replayed journal id of each non-master nodes.
                         * when we delete bdb database, we cannot delete db newer than
                         * any non-master node's current replayed journal id. otherwise,
                         * this lagging node can never get the deleted journal.
                         */
                        idURL = new URL("http://" + host + ":" + port + "/journal_id");
                        conn = (HttpURLConnection) idURL.openConnection();
                        conn.setConnectTimeout(CONNECT_TIMEOUT_SECOND * 1000);
                        conn.setReadTimeout(READ_TIMEOUT_SECOND * 1000);
                        String idString = conn.getHeaderField("id");
                        long id = Long.parseLong(idString);
                        if (minOtherNodesJournalId > id) {
                            minOtherNodesJournalId = id;
                        }
                    } catch (IOException e) {
                        LOG.error("Exception when getting current replayed journal id. host={}, port={}",
                                host, port, e);
                        minOtherNodesJournalId = 0;
                        break;
                    } finally {
                        if (conn != null) {
                            conn.disconnect();
                        }
                    }
                }
                deleteVersion = Math.min(minOtherNodesJournalId, checkPointVersion);
            }
            editLog.deleteJournals(deleteVersion + 1);
            if (MetricRepo.isInit.get()) {
                MetricRepo.COUNTER_IMAGE_PUSH.increase(1L);
            }
            LOG.info("journals <= {} are deleted. image version {}, other nodes min version {}", 
                     deleteVersion, checkPointVersion, minOtherNodesJournalId);
        }
        
        // Delete old image files
        MetaCleaner cleaner = new MetaCleaner(Config.meta_dir + "/image");
        try {
            cleaner.clean();
        } catch (IOException e) {
            LOG.error("Master delete old image file fail.", e);
        }
    
    }
    
    /*
     * Check whether can we do the checkpoint due to the memory used percent.
     */
    private boolean checkMemoryEnoughToDoCheckpoint() {
        long memUsedPercent = getMemoryUsedPercent();
        LOG.info("get jvm memory used percent: {} %", memUsedPercent);

        if (memUsedPercent > Config.metadata_checkopoint_memory_threshold && !Config.force_do_metadata_checkpoint) {
            LOG.warn("the memory used percent {} exceed the checkpoint memory threshold: {}",
                    memUsedPercent, Config.metadata_checkopoint_memory_threshold);
            return false;
        }
       
        return true;
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

}
