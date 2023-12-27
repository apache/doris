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

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LoadStatistic {
    // number of rows processed on BE, this number will be updated periodically by query report.
    // A load job may has several load tasks(queries), and each task has several fragments.
    // each fragment will report independently.
    // load task id -> fragment id -> rows count
    private Table<TUniqueId, TUniqueId, Long> counterTbl = HashBasedTable.create();

    // load task id -> fragment id -> load bytes
    private Table<TUniqueId, TUniqueId, Long> loadBytes = HashBasedTable.create();

    // load task id -> unfinished backend id list
    private Map<TUniqueId, List<Long>> unfinishedBackendIds = Maps.newHashMap();
    // load task id -> all backend id list
    private Map<TUniqueId, List<Long>> allBackendIds = Maps.newHashMap();

    private Map<String, String> counters = new HashMap<>();

    // number of file to be loaded
    public int fileNum = 0;
    public long totalFileSizeB = 0;

    // init the statistic of specified load task
    public synchronized void initLoad(TUniqueId loadId, Set<TUniqueId> fragmentIds, List<Long> relatedBackendIds) {
        counterTbl.rowMap().remove(loadId);
        for (TUniqueId fragId : fragmentIds) {
            counterTbl.put(loadId, fragId, 0L);
        }
        loadBytes.rowMap().remove(loadId);
        for (TUniqueId fragId : fragmentIds) {
            loadBytes.put(loadId, fragId, 0L);
        }
        allBackendIds.put(loadId, relatedBackendIds);
        // need to get a copy of relatedBackendIds, so that when we modify the "relatedBackendIds" in
        // allBackendIds, the list in unfinishedBackendIds will not be changed.
        unfinishedBackendIds.put(loadId, Lists.newArrayList(relatedBackendIds));
    }

    public synchronized void removeLoad(TUniqueId loadId) {
        counterTbl.rowMap().remove(loadId);
        loadBytes.rowMap().remove(loadId);
        unfinishedBackendIds.remove(loadId);
        allBackendIds.remove(loadId);
    }

    public synchronized void updateLoadProgress(long backendId, TUniqueId loadId, TUniqueId fragmentId,
                                                long rows, long bytes, boolean isDone) {
        if (counterTbl.contains(loadId, fragmentId)) {
            counterTbl.put(loadId, fragmentId, rows);
        }

        if (loadBytes.contains(loadId, fragmentId)) {
            loadBytes.put(loadId, fragmentId, bytes);
        }
        if (isDone && unfinishedBackendIds.containsKey(loadId)) {
            unfinishedBackendIds.get(loadId).remove(backendId);
        }
    }

    public synchronized long getScannedRows() {
        long total = 0;
        for (long rows : counterTbl.values()) {
            total += rows;
        }
        return total;
    }

    public synchronized long getLoadBytes() {
        long total = 0;
        for (long bytes : loadBytes.values()) {
            total += bytes;
        }
        return total;
    }

    public Map<String, String> getCounters() {
        // TODO: add extra statistics to counters
        return counters;
    }

    public synchronized String toJson() {
        long total = 0;
        for (long rows : counterTbl.values()) {
            total += rows;
        }
        long totalBytes = 0;
        for (long bytes : loadBytes.values()) {
            totalBytes += bytes;
        }

        Map<String, Object> details = Maps.newHashMap();
        details.put("ScannedRows", total);
        details.put("LoadBytes", totalBytes);
        details.put("FileNumber", fileNum);
        details.put("FileSize", totalFileSizeB);
        details.put("TaskNumber", counterTbl.rowMap().size());
        details.put("Unfinished backends", getPrintableMap(unfinishedBackendIds));
        details.put("All backends", getPrintableMap(allBackendIds));
        Gson gson = new Gson();
        return gson.toJson(details);
    }

    private Map<String, List<Long>> getPrintableMap(Map<TUniqueId, List<Long>> map) {
        Map<String, List<Long>> newMap = Maps.newHashMap();
        for (Map.Entry<TUniqueId, List<Long>> entry : map.entrySet()) {
            newMap.put(DebugUtil.printId(entry.getKey()), entry.getValue());
        }
        return newMap;
    }
}
