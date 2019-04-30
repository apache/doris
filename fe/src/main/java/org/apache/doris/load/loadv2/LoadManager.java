/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.task.MasterTaskExecutor;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * The broker and mini load jobs(v2) are included in this class.
 */
public class LoadManager {
    private Map<Long, LoadJob> idToLoadJob = Maps.newConcurrentMap();
    private Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Maps.newConcurrentMap();
    private MasterTaskExecutor taskExecutor = new MasterTaskExecutor(10);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /**
     * This method will be invoked by the broker load(v2) now.
     * @param stmt
     * @throws DdlException
     */
    public void createLoadJobFromStmt(LoadStmt stmt) throws DdlException {
        Database database = checkDb(stmt.getLabel().getDbName());
        long dbId = database.getId();
        writeLock();
        try {
            isLabelUsed(dbId, stmt.getLabel().getLabelName());
            if (stmt.getBrokerDesc() == null) {
                throw new DdlException("LoadManager only support the broker load.");
            }
            BrokerLoadJob brokerLoadJob = BrokerLoadJob.fromLoadStmt(stmt, this);
            addLoadJob(brokerLoadJob);
        } finally {
            writeUnlock();
        }
    }

    private void addLoadJob(LoadJob loadJob) {
        idToLoadJob.put(loadJob.getId(), loadJob);
        long dbId = loadJob.getDbId();
        if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
            dbIdToLabelToLoadJobs.put(loadJob.getDbId(), new ConcurrentHashMap<>());
        }
        if (!dbIdToLabelToLoadJobs.get(dbId).containsKey(loadJob.getLabel())) {
            dbIdToLabelToLoadJobs.get(dbId).put(loadJob.getLabel(), new ArrayList<>());
        }
        dbIdToLabelToLoadJobs.get(dbId).get(loadJob.getLabel()).add(loadJob);
    }

    public List<LoadJob> getLoadJobByState(JobState jobState) {
        return idToLoadJob.values().stream()
                .filter(entity -> entity.getState() == jobState)
                .collect(Collectors.toList());
    }

    private Database checkDb(String dbName) throws DdlException {
        // get db
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }
        return db;
    }

    /**
     * step1: if label has been used in old load jobs which belong to load class
     * step2: if label has been used in v2 load jobs
     *
     * @param dbId
     * @param label
     * @throws DdlException throw exception when label has been used by an unfinished job.
     */
    private void isLabelUsed(long dbId, String label)
            throws DdlException {
        // if label has been used in old load jobs
        Catalog.getCurrentCatalog().getLoadInstance().isLabelUsed(dbId, label);
        // if label has been used in v2 of load jobs
        if (dbIdToLabelToLoadJobs.containsKey(dbId)) {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs.containsKey(label)) {
                List<LoadJob> labelLoadJobs = labelToLoadJobs.get(label);
                if (labelLoadJobs.stream().filter(entity -> !entity.isFinished()).count() != 0) {
                    throw new LabelAlreadyUsedException(label);
                }
            }
        }
    }

    public void submitTask(LoadTask loadTask) {
        taskExecutor.submit(loadTask);
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }
}
