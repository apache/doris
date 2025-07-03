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

package org.apache.doris.qe;

import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TErrorTabletInfo;
import org.apache.doris.thrift.TTabletCommitInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LoadContext {

    private volatile String trackingUrl;
    private volatile long transactionId;
    private volatile String label;
    private final List<String> exportFiles = Lists.newCopyOnWriteArrayList();
    private final Map<String, String> loadCounters = Maps.newLinkedHashMap();
    private final List<String> deltaUrls = Lists.newCopyOnWriteArrayList();
    private final List<TErrorTabletInfo> errorTabletInfos = Lists.newCopyOnWriteArrayList();

    // in pipelinex, the commit info may be duplicate, so we remove the duplicate ones
    // key: backendsId
    // values: tabletId
    private final Map<Pair<Long, Long>, TTabletCommitInfo> commitInfoMap = Maps.newLinkedHashMap();

    public synchronized Map<String, String> getLoadCounters() {
        return ImmutableMap.copyOf(loadCounters);
    }

    public synchronized void updateLoadCounters(Map<String, String> newLoadCounters) {
        long numRowsNormal = 0L;
        String value = this.loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
        if (value != null) {
            numRowsNormal = Long.parseLong(value);
        }
        long numRowsAbnormal = 0L;
        value = this.loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
        if (value != null) {
            numRowsAbnormal = Long.parseLong(value);
        }
        long numRowsUnselected = 0L;
        value = this.loadCounters.get(LoadJob.UNSELECTED_ROWS);
        if (value != null) {
            numRowsUnselected = Long.parseLong(value);
        }

        // new load counters
        value = newLoadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
        if (value != null) {
            numRowsNormal += Long.parseLong(value);
        }
        value = newLoadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
        if (value != null) {
            numRowsAbnormal += Long.parseLong(value);
        }
        value = newLoadCounters.get(LoadJob.UNSELECTED_ROWS);
        if (value != null) {
            numRowsUnselected += Long.parseLong(value);
        }

        this.loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, Long.toString(numRowsNormal));
        this.loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, Long.toString(numRowsAbnormal));
        this.loadCounters.put(LoadJob.UNSELECTED_ROWS, Long.toString(numRowsUnselected));
    }

    public List<String> getDeltaUrls() {
        return Utils.fastToImmutableList(deltaUrls);
    }

    public void updateDeltaUrls(List<String> deltaUrls) {
        if (!deltaUrls.isEmpty()) {
            this.deltaUrls.addAll(deltaUrls);
        }
    }

    public synchronized void updateCommitInfos(List<TTabletCommitInfo> commitInfos) {
        // distinct commit info in the map
        for (TTabletCommitInfo commitInfo : commitInfos) {
            this.commitInfoMap.put(Pair.of(commitInfo.backendId, commitInfo.tabletId), commitInfo);
        }
    }

    public synchronized List<TTabletCommitInfo> getCommitInfos() {
        return Utils.fastToImmutableList(commitInfoMap.values());
    }

    public void updateTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void updateTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public String getLabel() {
        return label;
    }

    public void updateLabel(String label) {
        this.label = label;
    }

    public void addExportFiles(List<String> files) {
        this.exportFiles.addAll(files);
    }

    public List<String> getExportFiles() {
        return exportFiles;
    }

    public synchronized void updateErrorTabletInfos(List<TErrorTabletInfo> errorTabletInfos) {
        if (this.errorTabletInfos.size() <= Config.max_error_tablet_of_broker_load) {
            this.errorTabletInfos.addAll(errorTabletInfos.stream().limit(Config.max_error_tablet_of_broker_load
                    - this.errorTabletInfos.size()).collect(Collectors.toList()));
        }
    }

    public List<TErrorTabletInfo> getErrorTabletInfos() {
        return errorTabletInfos;
    }
}
