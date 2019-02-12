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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.MetaCleaner;
import org.apache.doris.persist.Storage;
import org.apache.doris.system.Frontend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * Checkpoint daemon is running on master node. handle the checkpoint work for palo. 
 */
public class Checkpoint extends Daemon {
    public static final Logger LOG = LogManager.getLogger(Checkpoint.class);
    private static final int PUT_TIMEOUT_SECOND = 3600;
    private static final int CONNECT_TIMEOUT_SECOND = 1;
    private static final int READ_TIMEOUT_SECOND = 1;
    private static final int AVERAGE_PARTITION_SIZE = 64;
    private static final int AVERAGE_INDEX_SIZE = 56;
    private static final int AVERAGE_TABLET_SIZE = 16;
    private static final int AVERAGE_REPLICA_SIZE = 56;
    private static final int AVERAGE_LOADJOB_SIZE = 168;
    private static final int MAX_MEMORY_USAGE_RATE = 90;
    
    private Catalog catalog;
    private String imageDir;
    private EditLog editLog;

    public Checkpoint(EditLog editLog) throws IOException {
        this.imageDir = Catalog.IMAGE_DIR;
        this.editLog = editLog;
    }

    public static class NullOutputStream extends OutputStream {
        public void write(byte[] b, int off, int len) throws IOException {
        }

        public void write(int b) throws IOException {
        }
    }

    protected void runOneCycle() {
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
        catalog = Catalog.getCheckpoint();
        catalog.setEditLog(editLog);
        try {
            catalog.loadImage(imageDir);
            catalog.replayJournal(checkPointVersion);
            if (catalog.getReplayedJournalId() != checkPointVersion) {
                LOG.error("checkpoint version should be {}, actual replayed journal id is {}",
                          checkPointVersion, catalog.getReplayedJournalId());
                return;
            }
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
        List<Frontend> allFrontends = Catalog.getInstance().getFrontends(null);
        int successPushed = 0;
        int otherNodesCount = 0;
        if (!allFrontends.isEmpty()) {
            otherNodesCount = allFrontends.size() - 1; // skip master itself
            for (Frontend fe : allFrontends) {
                String host = fe.getHost();
                if (host.equals(Catalog.getInstance().getMasterIp())) {
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
                    if (host.equals(Catalog.getInstance().getMasterIp())) {
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
                deleteVersion = (minOtherNodesJournalId > checkPointVersion)
                        ? checkPointVersion : minOtherNodesJournalId;
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
    
    private boolean checkMemoryEnoughToDoCheckpoint() {
        List<String> dbNames = Catalog.getInstance().getDbNames();
        if (dbNames == null || dbNames.isEmpty()) {
            return true;
        }
        
        int totalPartitionNum = 0;
        int totalIndexNum = 0;
        int totalTabletNum = 0;
        int totalReplicaNum = 0;
        long stillNeedMemorySize = 0;
        
        for (String name : dbNames) {
            Database db = Catalog.getInstance().getDb(name);
            if (db == null) {
                continue;
            }

            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    for (Partition partition : olapTable.getPartitions()) {
                        totalPartitionNum++;
                        for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                            totalIndexNum++;
                            for (Tablet tablet : materializedIndex.getTablets()) {
                                totalTabletNum++;
                                int replicaNum = tablet.getReplicas().size();
                                totalReplicaNum += replicaNum;
                            } // end for tablets
                        } // end for indices
                    } // end for partitions
                } // end for tables
            } finally {
                db.readUnlock();
            }
        } // end for dbs
        
        int totalLoadJobNum = Catalog.getInstance().getLoadInstance().getLoadJobNumber();
        stillNeedMemorySize += totalPartitionNum * AVERAGE_PARTITION_SIZE;
        stillNeedMemorySize += totalIndexNum * AVERAGE_INDEX_SIZE;
        stillNeedMemorySize += totalTabletNum * AVERAGE_TABLET_SIZE;
        stillNeedMemorySize += totalReplicaNum * AVERAGE_REPLICA_SIZE;
        stillNeedMemorySize += totalLoadJobNum * AVERAGE_LOADJOB_SIZE;
        
        MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        /*
         *  use memoryUsage.getUsed() * 2 instead of memoryUsage.getUsed() + stillNeedMemorySize?
         *  stillNeedMemorySize is less than the actual memory needed to do checkpoint 
         */
        if (memoryUsage.getMax() * MAX_MEMORY_USAGE_RATE / 100 < memoryUsage.getUsed() + stillNeedMemorySize) {
            LOG.warn("memory is not enough to do checkpoint. Committed memroy {} Bytes, used memory {} Bytes.",
                    memoryUsage.getCommitted(), memoryUsage.getUsed());
            return false;
        }
        return true;
    }

}
