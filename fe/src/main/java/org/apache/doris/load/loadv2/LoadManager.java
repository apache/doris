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
import org.apache.doris.task.MasterTaskExecutor;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/*
This class contains all of broker and mini load job(v2).
 */
public class LoadManager {
    private Map<Long, LoadJob> idToLoadJob = Maps.newConcurrentMap();
    private MasterTaskExecutor pendingTaskExecutor =
            new MasterTaskExecutor(Config.load_pending_thread_num_high_priority);
    private MasterTaskExecutor loadingTaskExecutor = new MasterTaskExecutor(10);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /*
    Only broker stmt will call this method now.
     */
    public void createLoadJob(LoadStmt stmt) throws DdlException {
        checkDb(stmt.getLabel().getDbName());
        if (stmt.getBrokerDesc() != null) {
            BrokerLoadJob brokerLoadJob = BrokerLoadJob.fromLoadStmt(stmt);
            idToLoadJob.put(brokerLoadJob.getId(), brokerLoadJob);
        } else {
            throw new DdlException("LoadManager only support the broker load.");
        }
    }

    public List<LoadJob> getLoadJobByState(JobState jobState) {
        return idToLoadJob.values().stream()
                .filter(entity -> entity.getState() == jobState)
                .collect(Collectors.toList());
    }

    public List<LoadJob> getNeedScheduleJobs() {
        return getLoadJobByState(JobState.PENDING).stream()
                .filter(entity -> !entity.hasPendingTask())
                .collect(Collectors.toList());
    }

    private void checkDb(String dbName) throws DdlException {
        // get db
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }
    }

    public void submitPendingTask(LoadPendingTask loadPendingTask) {
        pendingTaskExecutor.submit(loadPendingTask);
    }

    public void submitLoadingTask(LoadLoadingTask loadLoadingTask) {
        loadingTaskExecutor.submit(loadLoadingTask);
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
