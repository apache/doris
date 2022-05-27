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

package org.apache.doris.load.update;

import org.apache.doris.analysis.UpdateStmt;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UpdateManager {
    private final boolean enableConcurrentUpdate = Config.enable_concurrent_update;
    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private Map<Long, List<UpdateStmtExecutor>> tableIdToCurrentUpdate = Maps.newConcurrentMap();

    private void writeLock() {
        rwLock.writeLock().lock();
    }

    private void writeUnlock() {
        rwLock.writeLock().unlock();
    }

    public void handleUpdate(UpdateStmt updateStmt) throws UserException {
        UpdateStmtExecutor updateStmtExecutor = addUpdateExecutor(updateStmt);
        try {
            updateStmtExecutor.execute();
        } finally {
            removeUpdateExecutor(updateStmtExecutor);
        }
    }

    private UpdateStmtExecutor addUpdateExecutor(UpdateStmt updateStmt) throws AnalysisException, DdlException {
        writeLock();
        try {
            List<UpdateStmtExecutor> currentUpdateList = tableIdToCurrentUpdate.get(updateStmt.getTargetTable().getId());
            if (!enableConcurrentUpdate && currentUpdateList != null && currentUpdateList.size() > 0) {
                throw new DdlException("There is an update operation in progress for the current table. "
                        + "Please try again later, or set enable_concurrent_update in fe.conf to true");
            }
            UpdateStmtExecutor updateStmtExecutor = UpdateStmtExecutor.fromUpdateStmt(updateStmt);
            if (currentUpdateList == null) {
                currentUpdateList = Lists.newArrayList();
                tableIdToCurrentUpdate.put(updateStmtExecutor.getTargetTableId(), currentUpdateList);
            }
            currentUpdateList.add(updateStmtExecutor);
            return updateStmtExecutor;
        } finally {
            writeUnlock();
        }
    }

    private void removeUpdateExecutor(UpdateStmtExecutor updateStmtExecutor) {
        writeLock();
        try {
            List<UpdateStmtExecutor> currentUpdateList = tableIdToCurrentUpdate.get(updateStmtExecutor.getTargetTableId());
            if (currentUpdateList == null) {
                return;
            }
            currentUpdateList.remove(updateStmtExecutor);
        } finally {
            writeUnlock();
        }
    }
}
