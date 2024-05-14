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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.plugin.audit.AuditEvent;
import org.apache.doris.thrift.TQueryStatistics;
import org.apache.doris.thrift.TReportWorkloadRuntimeStatusParams;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadRuntimeStatusMgr extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(WorkloadRuntimeStatusMgr.class);
    private Map<Long, Map<String, TQueryStatistics>> beToQueryStatsMap = Maps.newConcurrentMap();
    private Map<Long, Long> beLastReportTime = Maps.newConcurrentMap();
    private Map<String, Long> queryLastReportTime = Maps.newConcurrentMap();
    private final ReentrantReadWriteLock queryAuditEventLock = new ReentrantReadWriteLock();
    private List<AuditEvent> queryAuditEventList = Lists.newLinkedList();

    public WorkloadRuntimeStatusMgr() {
        super("workload-runtime-stats-thread", Config.workload_runtime_status_thread_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        // 1 merge be query statistics
        Map<String, TQueryStatistics> queryStatisticsMap = getQueryStatisticsMap();

        // 2 log query audit
        List<AuditEvent> auditEventList = getQueryNeedAudit();
        for (AuditEvent auditEvent : auditEventList) {
            TQueryStatistics queryStats = queryStatisticsMap.get(auditEvent.queryId);
            if (queryStats != null) {
                auditEvent.scanRows = queryStats.scan_rows;
                auditEvent.scanBytes = queryStats.scan_bytes;
                auditEvent.peakMemoryBytes = queryStats.max_peak_memory_bytes;
                auditEvent.cpuTimeMs = queryStats.cpu_ms;
                auditEvent.shuffleSendBytes = queryStats.shuffle_send_bytes;
                auditEvent.shuffleSendRows = queryStats.shuffle_send_rows;
            }
            Env.getCurrentAuditEventProcessor().handleAuditEvent(auditEvent);
        }

        // 3 clear beToQueryStatsMap when be report timeout
        clearReportTimeoutBeStatistics();
    }

    public void submitFinishQueryToAudit(AuditEvent event) {
        queryAuditEventLogWriteLock();
        try {
            event.pushToAuditLogQueueTime = System.currentTimeMillis();
            queryAuditEventList.add(event);
        } finally {
            queryAuditEventLogWriteUnlock();
        }
    }

    public List<AuditEvent> getQueryNeedAudit() {
        List<AuditEvent> ret = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        queryAuditEventLogWriteLock();
        try {
            int queryAuditLogTimeout = Config.query_audit_log_timeout_ms;
            Iterator<AuditEvent> iter = queryAuditEventList.iterator();
            while (iter.hasNext()) {
                AuditEvent ae = iter.next();
                if (currentTime - ae.pushToAuditLogQueueTime > queryAuditLogTimeout) {
                    ret.add(ae);
                    iter.remove();
                } else {
                    break;
                }
            }
        } finally {
            queryAuditEventLogWriteUnlock();
        }
        return ret;
    }

    public void updateBeQueryStats(TReportWorkloadRuntimeStatusParams params) {
        if (!params.isSetBackendId()) {
            LOG.warn("be report workload runtime status but without beid");
            return;
        }
        if (!params.isSetQueryStatisticsMap()) {
            LOG.warn("be report workload runtime status but without query stats map");
            return;
        }
        long beId = params.backend_id;
        Map<String, TQueryStatistics> queryIdMap = beToQueryStatsMap.get(beId);
        beLastReportTime.put(beId, System.currentTimeMillis());
        if (queryIdMap == null) {
            queryIdMap = Maps.newConcurrentMap();
            queryIdMap.putAll(params.query_statistics_map);
            beToQueryStatsMap.put(beId, queryIdMap);
        } else {
            long currentTime = System.currentTimeMillis();
            for (Map.Entry<String, TQueryStatistics> entry : params.query_statistics_map.entrySet()) {
                queryIdMap.put(entry.getKey(), entry.getValue());
                queryLastReportTime.put(entry.getKey(), currentTime);
            }
        }
    }

    public Map<String, TQueryStatistics> getQueryStatisticsMap() {
        // 1 merge query stats in all be
        Set<Long> beIdSet = beToQueryStatsMap.keySet();
        Map<String, TQueryStatistics> retQueryMap = Maps.newHashMap();
        for (Long beId : beIdSet) {
            Map<String, TQueryStatistics> currentQueryMap = beToQueryStatsMap.get(beId);
            Set<String> queryIdSet = currentQueryMap.keySet();
            for (String queryId : queryIdSet) {
                TQueryStatistics retQuery = retQueryMap.get(queryId);
                if (retQuery == null) {
                    retQuery = new TQueryStatistics();
                    retQueryMap.put(queryId, retQuery);
                }

                TQueryStatistics curQueryStats = currentQueryMap.get(queryId);
                mergeQueryStatistics(retQuery, curQueryStats);
            }
        }

        return retQueryMap;
    }

    public Map<Long, Map<String, TQueryStatistics>> getBeQueryStatsMap() {
        return beToQueryStatsMap;
    }

    private void mergeQueryStatistics(TQueryStatistics dst, TQueryStatistics src) {
        dst.scan_rows += src.scan_rows;
        dst.scan_bytes += src.scan_bytes;
        dst.cpu_ms += src.cpu_ms;
        dst.shuffle_send_bytes += src.shuffle_send_bytes;
        dst.shuffle_send_rows += src.shuffle_send_rows;
        if (dst.max_peak_memory_bytes < src.max_peak_memory_bytes) {
            dst.max_peak_memory_bytes = src.max_peak_memory_bytes;
        }
    }

    void clearReportTimeoutBeStatistics() {
        // 1 clear report timeout be
        Set<Long> beNeedToRemove = new HashSet<>();
        Set<Long> currentBeIdSet = beToQueryStatsMap.keySet();
        Long currentTime = System.currentTimeMillis();
        for (Long beId : currentBeIdSet) {
            Long lastReportTime = beLastReportTime.get(beId);
            if (lastReportTime != null
                    && currentTime - lastReportTime > Config.be_report_query_statistics_timeout_ms) {
                beNeedToRemove.add(beId);
            }
        }
        for (Long beId : beNeedToRemove) {
            beToQueryStatsMap.remove(beId);
            beLastReportTime.remove(beId);
        }

        // 2 clear report timeout query
        Set<String> queryNeedToClear = new HashSet<>();
        Long newCurrentTime = System.currentTimeMillis();
        Set<String> queryLastReportTimeKeySet = queryLastReportTime.keySet();
        for (String queryId : queryLastReportTimeKeySet) {
            Long lastReportTime = queryLastReportTime.get(queryId);
            if (lastReportTime != null
                    && newCurrentTime - lastReportTime > Config.be_report_query_statistics_timeout_ms) {
                queryNeedToClear.add(queryId);
            }
        }

        Set<Long> beIdSet = beToQueryStatsMap.keySet();
        for (String queryId : queryNeedToClear) {
            for (Long beId : beIdSet) {
                beToQueryStatsMap.get(beId).remove(queryId);
            }
            queryLastReportTime.remove(queryId);
        }
    }

    private void queryAuditEventLogWriteLock() {
        queryAuditEventLock.writeLock().lock();
    }

    private void queryAuditEventLogWriteUnlock() {
        queryAuditEventLock.writeLock().unlock();
    }
}
