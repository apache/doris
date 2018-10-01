// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.task;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.DistributionInfo;
import com.baidu.palo.catalog.DistributionInfo.DistributionInfoType;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.common.LoadException;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.load.FailMsg.CancelType;
import com.baidu.palo.load.Load;
import com.baidu.palo.load.LoadChecker;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.load.LoadJob.JobState;
import com.baidu.palo.load.PartitionLoadInfo;
import com.baidu.palo.load.TableLoadInfo;
import com.baidu.palo.load.TabletLoadInfo;
import com.baidu.palo.thrift.TEtlState;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

public abstract class LoadEtlTask extends MasterTask {
    private static final Logger LOG = LogManager.getLogger(LoadEtlTask.class);

    protected static final String QUALITY_FAIL_MSG = "quality not good enough to cancel";
    public static final String DPP_NORMAL_ALL = "dpp.norm.ALL";
    public static final String DPP_ABNORMAL_ALL = "dpp.abnorm.ALL";

    protected final LoadJob job;
    protected final Load load;
    protected Database db;

    public LoadEtlTask(LoadJob job) {
        super();
        this.job = job;
        this.signature = job.getId();
        this.load = Catalog.getInstance().getLoadInstance();
    }

    protected String getErrorMsg() {
        return "etl job fail";
    }

    @Override
    protected void exec() {
        // check job state
        if (job.getState() != JobState.ETL) {
            return;
        }
        
        // check timeout
        if (LoadChecker.checkTimeout(job)) {
            load.cancelLoadJob(job, CancelType.TIMEOUT, "etl timeout to cancel");
            return;
        }
        
        // check db
        long dbId = job.getDbId();
        db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            load.cancelLoadJob(job, CancelType.ETL_RUN_FAIL, "db does not exist. id: " + dbId);
            return;
        }

        // update etl job status
        if (job.getProgress() != 100) {
            try {
                updateEtlStatus();
            } catch (LoadException e) {
                CancelType cancelType = CancelType.ETL_RUN_FAIL;
                if (e.getMessage().equals(QUALITY_FAIL_MSG)) {
                    cancelType = CancelType.ETL_QUALITY_UNSATISFIED;
                }
                LOG.debug("update etl status fail, msg: {}. job: {}", e.getMessage(), job.toString());
                load.cancelLoadJob(job, cancelType, e.getMessage());
                return;
            }
        }

        // check partition is loading
        if (job.getProgress() == 100) {
            tryUpdateLoading();
        }
    }

    private void updateEtlStatus() throws LoadException {
        if (!updateJobEtlStatus()) {
            throw new LoadException("update job etl status fail");
        }

        TEtlState state = job.getEtlJobStatus().getState();
        switch (state) {
            case FINISHED:
                processEtlFinished();
                break;
            case CANCELLED:
                throw new LoadException(getErrorMsg());
            case RUNNING:
                processEtlRunning();
                break;
            default:
                LOG.warn("wrong etl job state: {}", state.name());
                break;
        }
    }
    
    private void processEtlFinished() throws LoadException {
        // check data quality when etl finished
        if (!checkDataQuality(job.getMaxFilterRatio())) {
            throw new LoadException(QUALITY_FAIL_MSG);
        }

        // get etl file map
        Map<String, Pair<String, Long>> filePathMap = getFilePathMap();

        // init tablet load info
        Map<Long, TabletLoadInfo> idToTabletLoadInfo = getTabletLoadInfos(filePathMap);
        job.setIdToTabletLoadInfo(idToTabletLoadInfo);

        // update job
        job.setProgress(100);
        job.setEtlFinishTimeMs(System.currentTimeMillis());
        job.getEtlJobStatus().setFileMap(null);
        LOG.info("etl job finished. job: {}", job);
    }

    private void tryUpdateLoading() {
        // check job has loading partitions
        Map<Long, TableLoadInfo> idToTableLoadInfo = job.getIdToTableLoadInfo();
        Set<Long> partitionIds = Sets.newHashSet();
        for (TableLoadInfo tableLoadInfo : idToTableLoadInfo.values()) {
            Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = tableLoadInfo.getIdToPartitionLoadInfo();
            for (Entry<Long, PartitionLoadInfo> entry : idToPartitionLoadInfo.entrySet()) {
                PartitionLoadInfo partitionLoadInfo = entry.getValue();
                if (partitionLoadInfo.isNeedLoad()) {
                    partitionIds.add(entry.getKey());
                }
            }
        }
        if (!load.addLoadingPartitions(partitionIds)) {
            LOG.info("load job has unfinished loading partitions. job: {}, job partitions: {}", job, partitionIds);
            return;
        }
        
        // new version and version hash
        try {
            for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
                long tableId = tableEntry.getKey();
                OlapTable table = null;
                db.readLock();
                try {
                    table = (OlapTable) db.getTable(tableId);
                } finally {
                    db.readUnlock();
                }
                if (table == null) {
                    throw new MetaNotFoundException("table does not exist. id: " + tableId);
                }
                
                TableLoadInfo tableLoadInfo = tableEntry.getValue();
                Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = tableLoadInfo.getIdToPartitionLoadInfo();
                for (Map.Entry<Long, PartitionLoadInfo> entry : idToPartitionLoadInfo.entrySet()) {
                    long partitionId = entry.getKey();
                    PartitionLoadInfo partitionLoadInfo = entry.getValue();
                    if (!partitionLoadInfo.isNeedLoad()) {
                        continue;
                    }
                    
                    db.readLock();
                    try {
                        Partition partition = table.getPartition(partitionId);
                        if (partition == null) {
                            throw new MetaNotFoundException("partition does not exist. id: " + partitionId);
                        } 
                        
                        partitionLoadInfo.setVersion(partition.getCommittedVersion() + 1);
                        partitionLoadInfo.setVersionHash(Math.abs(new Random().nextLong()));
                    } finally {
                        db.readUnlock();
                    }

                    LOG.info("load job id: {}, label: {}, partition info: {}-{}-{}, partition load info: {}",
                            job.getId(), job.getLabel(), db.getId(), tableId, partitionId, partitionLoadInfo);
                }
            }
        } catch (MetaNotFoundException e) {
            // remove loading partitions
            load.removeLoadingPartitions(partitionIds);
            load.cancelLoadJob(job, CancelType.ETL_RUN_FAIL, e.getMessage());
            return;
        }
        
        // update job to loading
        if (load.updateLoadJobState(job, JobState.LOADING)) {
            LOG.info("update job state to loading success. job: {}", job);
        } else {
            // remove loading partitions
            load.removeLoadingPartitions(partitionIds);
        }
    }
    
    protected String getPartitionIndexBucketString(String filePath) throws LoadException {
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        // label.partitionId.indexId.bucket
        String[] fileNameArr = fileName.split("\\.");
        if (fileNameArr.length != 4) {
            throw new LoadException("etl file name format error, name: " + fileName);
        }
        
        String partitionIndexBucket = fileName.substring(fileName.indexOf(".") + 1);
        return partitionIndexBucket;
    }
    
    protected Map<Long, TabletLoadInfo> getTabletLoadInfos(Map<String, Pair<String, Long>> filePathMap)
            throws LoadException {
        Map<Long, TabletLoadInfo> idToTabletLoadInfo = Maps.newHashMap();
        boolean hasLoadFiles = false;
        
        // create tablet load info
        Map<Long, TableLoadInfo> idToTableLoadInfo = job.getIdToTableLoadInfo();
        for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
            long tableId = tableEntry.getKey();
            OlapTable table = null;
            db.readLock();
            try {
                table = (OlapTable) db.getTable(tableId);
            } finally {
                db.readUnlock();
            }
            if (table == null) {
                throw new LoadException("table does not exist. id: " + tableId);
            }
            
            TableLoadInfo tableLoadInfo = tableEntry.getValue();
            for (Entry<Long, PartitionLoadInfo> partitionEntry : tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                long partitionId = partitionEntry.getKey();
                boolean needLoad = false;
                db.readLock();
                try {
                    Partition partition = table.getPartition(partitionId);
                    if (partition == null) {
                        throw new LoadException("partition does not exist. id: " + partitionId);
                    }
                    
                    DistributionInfo distributionInfo = partition.getDistributionInfo();
                    DistributionInfoType distributionType = distributionInfo.getType();
                    if (distributionType != DistributionInfoType.RANDOM
                            && distributionType != DistributionInfoType.HASH) {
                        throw new LoadException("unknown distribution type. type: " + distributionType.name());
                    }
                    
                    for (MaterializedIndex materializedIndex : partition.getMaterializedIndices()) {
                        long indexId = materializedIndex.getId();
                        int tabletIndex = 0;
                        for (Tablet tablet : materializedIndex.getTablets()) {
                            long bucket = tabletIndex++;
                            String tableViewBucket = String.format("%d.%d.%d", partitionId, indexId, bucket);
                            String filePath = null;
                            long fileSize = -1;
                            if (filePathMap.containsKey(tableViewBucket)) {
                                Pair<String, Long> filePair = filePathMap.get(tableViewBucket);
                                filePath = filePair.first;
                                fileSize = filePair.second;

                                needLoad = true;
                                hasLoadFiles = true;
                            }
                            
                            TabletLoadInfo tabletLoadInfo = new TabletLoadInfo(filePath, fileSize);
                            idToTabletLoadInfo.put(tablet.getId(), tabletLoadInfo);
                        }
                    }
                    
                    // partition might have no load data
                    partitionEntry.getValue().setNeedLoad(needLoad);
                } finally {
                    db.readUnlock();
                }
            }
        }
        
        // all partitions might have no load data
        if (!hasLoadFiles) {
            throw new LoadException("all partitions have no load data");
        }

        return idToTabletLoadInfo;
    }

    protected boolean checkDataQuality(double maxFilterRatio) {
        Map<String, String> counters = job.getEtlJobStatus().getCounters();
        if (!counters.containsKey(DPP_NORMAL_ALL) || !counters.containsKey(DPP_ABNORMAL_ALL)) {
            return true;
        }
        
        long normalNum = Long.parseLong(counters.get(DPP_NORMAL_ALL));
        long abnormalNum = Long.parseLong(counters.get(DPP_ABNORMAL_ALL));
        if (abnormalNum > (abnormalNum + normalNum) * maxFilterRatio) {
            return false;
        }

        return true;
    }
    
    protected abstract boolean updateJobEtlStatus();
    protected abstract void processEtlRunning() throws LoadException;
    protected abstract Map<String, Pair<String, Long>> getFilePathMap() throws LoadException;
}
