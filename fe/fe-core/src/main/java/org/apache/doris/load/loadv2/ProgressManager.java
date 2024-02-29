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
    private static final Logger LOG = LogManager.getLogger(ProgressManager.class);

    private Map<String, Progress> idToProgress = Maps.newConcurrentMap();

    public void registerProgress(String id, int scannerNum) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("create {} with initial scannerNum {}", id, scannerNum);
        }
        idToProgress.remove(id);
        idToProgress.put(id, new Progress(scannerNum));
    }

    public void registerProgressSimple(String id) {
        registerProgress(id, 0);
    }

    public void removeProgress(String id) {
        idToProgress.remove(id);
    }

    public void updateProgress(String id, TUniqueId queryId, TUniqueId fragmentId, int finishedScannerNum) {
        Progress progress = idToProgress.get(id);
        if (progress != null) {
            progress.updateFinishedScanNums(queryId, fragmentId, finishedScannerNum);
        } else {
            LOG.warn("progress[" + id + "] missing meta information");
        }
    }

    public void addTotalScanNums(String id, int num) {
        Progress progress = idToProgress.get(id);
        if (progress != null) {
            progress.addTotalScanNums(num);
        }
    }

    public String getProgressInfo(String id) {
        String progressInfo = "Unknown id: " + id;
        Progress progress = idToProgress.get(id);
        if (progress != null) {
            int finish = progress.getFinishedScanNums();
            int total = progress.getTotalScanNums();
            String currentProgress = String.format("%.2f", progress.getProgress());
            progressInfo = currentProgress + "% (" + finish + "/" + total + ")";
        }
        return progressInfo;
    }

    static class Progress {
        // one job have multiple query, and the query can be divided into
        // separate fragments. finished scan ranges reported from BE is bound
        // to the query, so we need to store them all to save status.
        // table: queryId -> fragmentId -> scan ranges
        private Table<TUniqueId, TUniqueId, Integer> finishedScanNums = HashBasedTable.create();
        private int totalScanNums = 0;

        public synchronized void addTotalScanNums(int num) {
            totalScanNums += num;
        }

        public synchronized void updateFinishedScanNums(TUniqueId queryId, TUniqueId fragmentId, int finishedScanNum) {
            finishedScanNums.put(queryId, fragmentId, finishedScanNum);
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
            // if no scan range found, the progress should be finished(100%)
            if (totalScanNums == 0) {
                return 100.0;
            }
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
