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
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.plugin.audit.AuditEvent;
import org.apache.doris.thrift.TQueryStatistics;
import org.apache.doris.thrift.TReportWorkloadRuntimeStatusParams;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// NOTE: not using a lock for beToQueryStatsMap's update because it should void global lock for all be
// this may cause in some corner case missing statistics update,for example:
// time1: clear logic judge query 1 is timeout
// time2: query 1 is update by report
// time3: clear logic remove query 1
// in this case, lost query stats is allowed. because query report time out is 60s by default,
// when this case happens, we should find why be not report for so long first.
public class WorkloadRuntimeStatusMgr extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(WorkloadRuntimeStatusMgr.class);
    private Map<Long, BeReportInfo> beToQueryStatsMap = Maps.newConcurrentMap();
    private final ReentrantReadWriteLock queryAuditEventLock = new ReentrantReadWriteLock();
    private List<AuditEvent> queryAuditEventList = Lists.newLinkedList();

    private class BeReportInfo {
        volatile long beLastReportTime;

        BeReportInfo(long beLastReportTime) {
            this.beLastReportTime = beLastReportTime;
        }

        Map<String, Pair<Long, TQueryStatistics>> queryStatsMap = Maps.newConcurrentMap();
    }

    public WorkloadRuntimeStatusMgr() {
        super("workload-runtime-stats-thread", Config.workload_runtime_status_thread_interval_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        // 1 merge be query statistics
        Map<String, TQueryStatistics> queryStatisticsMap = getQueryStatisticsMap();

        // 2 log query audit
        try {
            List<AuditEvent> auditEventList = getQueryNeedAudit();
            int missedLogCount = 0;
            int succLogCount = 0;
            for (AuditEvent auditEvent : auditEventList) {
                TQueryStatistics queryStats = queryStatisticsMap.get(auditEvent.queryId);
                if (queryStats != null) {
                    auditEvent.scanRows = queryStats.scan_rows;
                    auditEvent.scanBytes = queryStats.scan_bytes;
                    auditEvent.scanBytesFromLocalStorage = queryStats.scan_bytes_from_local_storage;
                    auditEvent.scanBytesFromRemoteStorage = queryStats.scan_bytes_from_remote_storage;
                    auditEvent.peakMemoryBytes = queryStats.max_peak_memory_bytes;
                    auditEvent.cpuTimeMs = queryStats.cpu_ms;
                    auditEvent.shuffleSendBytes = queryStats.shuffle_send_bytes;
                    auditEvent.shuffleSendRows = queryStats.shuffle_send_rows;
                }
                boolean ret = Env.getCurrentAuditEventProcessor().handleAuditEvent(auditEvent, true);
                if (!ret) {
                    missedLogCount++;
                } else {
                    succLogCount++;
                }
            }
            if (missedLogCount > 0) {
                LOG.warn("discard audit event because of log queue is full, discard num : {}, succ num : {}",
                        missedLogCount, succLogCount);
            }
        } catch (Throwable t) {
            LOG.warn("exception happens when handleAuditEvent, ", t);
        }

        // 3 clear beToQueryStatsMap when be report timeout
        clearReportTimeoutBeStatistics();
    }

    public void submitFinishQueryToAudit(AuditEvent event) {
        queryAuditEventLogWriteLock();
        try {
            if (queryAuditEventList.size() >= Config.audit_event_log_queue_size) {
                LOG.warn("audit log event queue size {} is full, this may cause audit log missed."
                                + "you can check whether qps is too high or reset audit_event_log_queue_size",
                        queryAuditEventList.size());
                return;
            }
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
        // NOTE(wb) one be sends update request one by one,
        // so there is no need a global lock for beToQueryStatsMap here,
        // just keep one be's put/remove/get is atomic operation is enough
        long currentTime = System.currentTimeMillis();
        BeReportInfo beReportInfo = beToQueryStatsMap.get(beId);
        if (beReportInfo == null) {
            beReportInfo = new BeReportInfo(currentTime);
            beToQueryStatsMap.put(beId, beReportInfo);
        } else {
            beReportInfo.beLastReportTime = currentTime;
        }
        for (Map.Entry<String, TQueryStatistics> entry : params.query_statistics_map.entrySet()) {
            beReportInfo.queryStatsMap.put(entry.getKey(), Pair.of(currentTime, (TQueryStatistics) entry.getValue()));
        }
    }

    void clearReportTimeoutBeStatistics() {
        // 1 clear report timeout be
        Set<Long> currentBeIdSet = beToQueryStatsMap.keySet();
        Long currentTime = System.currentTimeMillis();
        for (Long beId : currentBeIdSet) {
            BeReportInfo beReportInfo = beToQueryStatsMap.get(beId);
            if (currentTime - beReportInfo.beLastReportTime > Config.be_report_query_statistics_timeout_ms) {
                beToQueryStatsMap.remove(beId);
                continue;
            }
            Set<String> queryIdSet = beReportInfo.queryStatsMap.keySet();
            for (String queryId : queryIdSet) {
                Pair<Long, TQueryStatistics> pair = beReportInfo.queryStatsMap.get(queryId);
                long queryLastReportTime = pair.first;
                if (currentTime - queryLastReportTime > Config.be_report_query_statistics_timeout_ms) {
                    beReportInfo.queryStatsMap.remove(queryId);
                }
            }
        }
    }

    // NOTE: currently getQueryStatisticsMap must be called before clear beToQueryStatsMap
    // so there is no need lock or null check when visit beToQueryStatsMap
    public Map<String, TQueryStatistics> getQueryStatisticsMap() {
        // 1 merge query stats in all be
        Set<Long> beIdSet = beToQueryStatsMap.keySet();
        Map<String, TQueryStatistics> resultQueryMap = Maps.newHashMap();
        for (Long beId : beIdSet) {
            BeReportInfo beReportInfo = beToQueryStatsMap.get(beId);
            Set<String> queryIdSet = beReportInfo.queryStatsMap.keySet();
            for (String queryId : queryIdSet) {
                TQueryStatistics curQueryStats = beReportInfo.queryStatsMap.get(queryId).second;

                TQueryStatistics retQuery = resultQueryMap.get(queryId);
                if (retQuery == null) {
                    retQuery = new TQueryStatistics();
                    resultQueryMap.put(queryId, retQuery);
                }
                mergeQueryStatistics(retQuery, curQueryStats);
            }
        }

        return resultQueryMap;
    }

    private void mergeQueryStatistics(TQueryStatistics dst, TQueryStatistics src) {
        dst.scan_rows += src.scan_rows;
        dst.scan_bytes += src.scan_bytes;
        dst.scan_bytes_from_local_storage += src.scan_bytes_from_local_storage;
        dst.scan_bytes_from_remote_storage += src.scan_bytes_from_remote_storage;
        dst.cpu_ms += src.cpu_ms;
        dst.shuffle_send_bytes += src.shuffle_send_bytes;
        dst.shuffle_send_rows += src.shuffle_send_rows;
        if (dst.max_peak_memory_bytes < src.max_peak_memory_bytes) {
            dst.max_peak_memory_bytes = src.max_peak_memory_bytes;
        }
    }

    private void queryAuditEventLogWriteLock() {
        queryAuditEventLock.writeLock().lock();
    }

    private void queryAuditEventLogWriteUnlock() {
        queryAuditEventLock.writeLock().unlock();
    }
}
