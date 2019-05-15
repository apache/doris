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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.EtlSubmitResult;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.TableLoadInfo;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

// Making a pull load job to some tasks
public class PullLoadPendingTask extends LoadPendingTask {
    private static final Logger LOG = LogManager.getLogger(PullLoadPendingTask.class);

    private PullLoadJob pullLoadJob = null;

    public PullLoadPendingTask(LoadJob job) {
        super(job);
    }

    @Override
    protected void createEtlRequest() throws Exception {
        long jobDeadlineMs = -1;
        if (job.getTimeoutSecond() > 0) {
            jobDeadlineMs = job.getCreateTimeMs() + job.getTimeoutSecond() * 1000;
        }
        List<PullLoadTask> pullLoadTaskList = Lists.newArrayList();
        // we need to make sure that the 'Plan' used the correct schema version,
        // So, we generate task plan here

        // first we should get file status outside the lock
        // table id -> file status
        Map<Long, List<List<TBrokerFileStatus>>> fileStatusMap = Maps.newHashMap();
        // table id -> total file num
        Map<Long, Integer> fileNumMap = Maps.newHashMap();
        getAllFileStatus(fileStatusMap, fileNumMap);

        db.readLock();
        try {
            int nextTaskId = 1;
            // tableId -> BrokerFileGroups
            for (Map.Entry<Long, List<BrokerFileGroup>> entry :
                    job.getPullLoadSourceInfo().getIdToFileGroups().entrySet()) {
                long tableId = entry.getKey();
                OlapTable table  = (OlapTable) db.getTable(tableId);
                if (table == null) {
                    throw new DdlException("Unknown table(" + tableId + ") in database(" + db.getFullName() + ")");
                }

                // Generate pull load task, one
                PullLoadTask task = new PullLoadTask(
                        job.getId(), nextTaskId, db, table,
                        job.getBrokerDesc(), entry.getValue(), jobDeadlineMs, job.getExecMemLimit());
                task.init(fileStatusMap.get(tableId), fileNumMap.get(tableId));
                pullLoadTaskList.add(task);
                nextTaskId++;

                // add schema hash to table load info
                TableLoadInfo tableLoadInfo = job.getTableLoadInfo(entry.getKey());
                tableLoadInfo.addAllSchemaHash(table.getIndexIdToSchemaHash());
            }
        } finally {
            db.readUnlock();
        }

        pullLoadJob = new PullLoadJob(job, pullLoadTaskList);
    }

    @Override
    protected EtlSubmitResult submitEtlJob(int retry) {
        Catalog.getInstance().getPullLoadJobMgr().submit(pullLoadJob);
        return new EtlSubmitResult(new TStatus(TStatusCode.OK), null);
    }

    private void getAllFileStatus(Map<Long, List<List<TBrokerFileStatus>>> fileStatusMap,
            Map<Long, Integer> fileNumMap)
            throws UserException {
        for (Map.Entry<Long, List<BrokerFileGroup>> entry : job.getPullLoadSourceInfo().getIdToFileGroups().entrySet()) {
            long tableId = entry.getKey();

            List<List<TBrokerFileStatus>> fileStatusList = Lists.newArrayList();
            int filesAdded = 0;
            List<BrokerFileGroup> fileGroups = entry.getValue();
            for (BrokerFileGroup fileGroup : fileGroups) {
                List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
                for (String path : fileGroup.getFilePaths()) {
                    BrokerUtil.parseBrokerFile(path, job.getBrokerDesc(), fileStatuses);
                }
                fileStatusList.add(fileStatuses);
                filesAdded += fileStatuses.size();
                for (TBrokerFileStatus fstatus : fileStatuses) {
                    LOG.info("pull load job: {}. Add file status is {}", job.getId(), fstatus);
                }
            }

            fileStatusMap.put(tableId, fileStatusList);
            fileNumMap.put(tableId, filesAdded);
        }
    }
}
