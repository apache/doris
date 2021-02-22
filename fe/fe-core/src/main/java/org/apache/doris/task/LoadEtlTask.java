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

package org.apache.doris.task;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadChecker;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.load.PartitionLoadInfo;
import org.apache.doris.load.TableLoadInfo;
import org.apache.doris.load.TabletLoadInfo;
import org.apache.doris.thrift.TEtlState;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;

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
        this.load = Catalog.getCurrentCatalog().getLoadInstance();
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
        db = Catalog.getCurrentCatalog().getDb(dbId);
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
        
        // new version and version hash
        try {
            for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
                long tableId = tableEntry.getKey();
                OlapTable table = (OlapTable) db.getTable(tableId);
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
                    
                    table.readLock();
                    try {
                        Partition partition = table.getPartition(partitionId);
                        if (partition == null) {
                            throw new MetaNotFoundException("partition does not exist. id: " + partitionId);
                        }
                        // yiguolei: real time load do not need get version here
                    } finally {
                        table.readUnlock();
                    }

                    LOG.info("load job id: {}, label: {}, partition info: {}-{}-{}, partition load info: {}",
                            job.getId(), job.getLabel(), db.getId(), tableId, partitionId, partitionLoadInfo);
                }
            }
        } catch (MetaNotFoundException e) {
            // remove loading partitions
            // yiguolei: partition ids is only used to check if there is a loading job running on a partition
            // it is useless in real time load since it could run concurrently
            // load.removeLoadingPartitions(partitionIds);
            load.cancelLoadJob(job, CancelType.ETL_RUN_FAIL, e.getMessage());
            return;
        }
        
        // update job to loading
        if (load.updateLoadJobState(job, JobState.LOADING)) {
            LOG.info("update job state to loading success. job: {}", job);
        } else {
            // remove loading partitions
            // yiguolei: do not need remove any more, since we have not add it into
            // load.removeLoadingPartitions(partitionIds);
            LOG.warn("update job state to loading failed. job: {}", job);
            if (job.getTransactionId() > 0) {
                LOG.warn("there maybe remaining transaction id {} in transaction table", job.getTransactionId());
            }
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
            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                throw new LoadException("table does not exist. id: " + tableId);
            }

            table.readLock();
            try {
                TableLoadInfo tableLoadInfo = tableEntry.getValue();
                for (Entry<Long, PartitionLoadInfo> partitionEntry : tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                    long partitionId = partitionEntry.getKey();
                    boolean needLoad = false;

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
                    
                    // yiguolei: how to deal with filesize == -1?
                    for (MaterializedIndex materializedIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
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

                }
            } finally {
                table.readUnlock();
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
