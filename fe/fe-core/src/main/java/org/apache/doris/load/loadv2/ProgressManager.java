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

package org.apache.doris.load.loadv2;

import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * ProgressManager manage the progress of loading and exporting tasks
 */
public class ProgressManager {
    private static final Logger LOG = LogManager.getLogger(LoadManager.class);

    private Map<Long, Progress> idToProgress = Maps.newConcurrentMap();

    public void registerProgress(Long id, int scannerNum) {
        LOG.info("create " + id + " with initial scannerNum " + scannerNum);
        idToProgress.remove(id);
        idToProgress.put(id, new Progress(scannerNum));
    }

    public void registerProgressSimple(Long id) {
        registerProgress(id, 0);
    }

    public void updateProgress(Long id, TUniqueId queryId, TUniqueId fragmentId, int finishedScannerNum) {
        Progress progress = idToProgress.get(id);
        if (progress != null) {
            progress.updateFinishedScanNums(queryId, fragmentId, finishedScannerNum);
        } else {
            LOG.warn("progress[" + id + "] missing meta infomartion");
        }
    }

    public void addTotalScanNums(Long id, int num) {
        Progress progress = idToProgress.get(id);
        if (progress != null) {
            progress.addTotalScanNums(num);
        }
    }

    public void deregisterProgress(Long id) {
        // TODO: deregister the progress
        idToProgress.remove(id);
    }

    public Progress getProgressClass(Long id) {
        return idToProgress.get(id);
    }

    public double getProgress(Long id) {
        return idToProgress.get(id).getProgress();
    }

    static class Progress {
        private Table<TUniqueId, TUniqueId, Integer> finishedScanNums = HashBasedTable.create();
        private int totalScanNums = 0;

        public synchronized void addTotalScanNums(int num) {
            totalScanNums += num;
        }

        public synchronized void updateFinishedScanNums(TUniqueId queryId, TUniqueId fragmentId, int finishedScanNum) {
            if (finishedScanNums.contains(queryId, fragmentId)) {
                finishedScanNums.put(queryId, fragmentId, finishedScanNum);
            } else {
                finishedScanNums.put(queryId, fragmentId, finishedScanNum);
            }
        }

        public int getTotalScanNums() {
            return totalScanNums;
        }

        public int getFinishedScanNums() {
            int result = 0;
            for (Integer v : finishedScanNums.values()) {
                result += v;
            }
            return result;
        }

        public double getProgress() {
            return getFinishedScanNums() * 100 / (double) totalScanNums;
        }

        public Progress(int totalScanNums) {
            this.totalScanNums = totalScanNums;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Finished/Total: ");
            sb.append(getFinishedScanNums());
            sb.append("/");
            sb.append(totalScanNums);
            sb.append(" => ");
            sb.append(getProgress());
            sb.append("%");
            return sb.toString();
        }
    }
}
