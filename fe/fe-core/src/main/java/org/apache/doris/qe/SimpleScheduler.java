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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Reference;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class SimpleScheduler {
    private static final Logger LOG = LogManager.getLogger(SimpleScheduler.class);

    public static class BlackListInfo {
        public BlackListInfo() {}

        private Lock lock = new ReentrantLock();
        // Record the reason why this backend is added to black list, will be updated only once.
        private String reasonForBlackList = "";
        // Record the first time this backend is tried to be added to black list.
        private Long firstRecordBlackTimestampMs = 0L;
        // Record the last time this backend is tried to be added to black list.
        private Long lastRecordBlackTimestampMs = 0L;
        // Record the timestamp this backend is really regarded as blacked.
        private Long lastBlackTimestampMs = 0L;
        // Record the count of this backend is tried to be added to black list.
        private Long recordBlackListCount = 0L;
        // Record the backend id
        private Long backendID = 0L;

        // Try to add this backend to black list, backend is not really added to black list until
        // condition in shouldBeBlackListed is met.
        public void tryAddBlackList(String reason) {
            lock.lock();
            try {
                recordAddBlackList(reason);
                if (shouldBeBlackListed()) {
                    doAddBlackList();
                }
            } finally {
                lock.unlock();
            }
        }

        // Just update the fileds.
        private void recordAddBlackList(String reason) {
            if (firstRecordBlackTimestampMs <= 0) {
                firstRecordBlackTimestampMs = System.currentTimeMillis();
            }

            lastRecordBlackTimestampMs = System.currentTimeMillis();

            // Restart the counter if the time interval is too long
            if (lastRecordBlackTimestampMs - firstRecordBlackTimestampMs
                    >= Config.do_add_backend_black_list_threshold_seconds * 1000) {
                firstRecordBlackTimestampMs = lastRecordBlackTimestampMs;
                recordBlackListCount = 0L;
            }

            recordBlackListCount++;

            if (Strings.isNullOrEmpty(reasonForBlackList) && !Strings.isNullOrEmpty(reason)) {
                reasonForBlackList = reason;
            }
        }

        private boolean shouldBeBlackListed() {
            if (lastRecordBlackTimestampMs <= 0 || firstRecordBlackTimestampMs <= 0) {
                return false;
            }

            if (recordBlackListCount < Config.do_add_backend_black_list_threshold_count) {
                return false;
            }

            if (lastRecordBlackTimestampMs - firstRecordBlackTimestampMs
                    >= Config.do_add_backend_black_list_threshold_seconds * 1000) {
                return false;
            }

            return true;
        }

        private void doAddBlackList() {
            lastBlackTimestampMs = System.currentTimeMillis();
            Exception e = new Exception();
            String stack = org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e);
            LOG.warn("Backend is added to black list.\nInformation:\n{}\nStack:\n{}", toString(), stack);
        }

        public boolean shouldBeRemoved() {
            lock.lock();
            try {
                if (lastBlackTimestampMs <= 0) {
                    return false;
                }

                Long currentTimeStamp = System.currentTimeMillis();
                // If this backend has not been recorded as black for more than 10 secs, then regard it as normal
                if (currentTimeStamp - lastBlackTimestampMs
                        >= Config.stay_in_backend_black_list_threshold_seconds * 1000) {
                    return true;
                } else {
                    return false;
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean isBlacked() {
            lock.lock();
            try {
                return lastBlackTimestampMs > 0;
            } finally {
                lock.unlock();
            }
        }

        public String getReasonForBlackList() {
            lock.lock();
            try {
                return reasonForBlackList;
            } finally {
                lock.unlock();
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            lock.lock();
            // Convert to human readable time
            LocalDateTime firstRecordBlackTimes = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(firstRecordBlackTimestampMs), ZoneId.systemDefault());
            LocalDateTime lastRecordBlackTimes = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(lastRecordBlackTimestampMs), ZoneId.systemDefault());
            LocalDateTime lastBlackTimes = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(lastBlackTimestampMs), ZoneId.systemDefault());
            try {
                sb.append("\nbackendID: ").append(backendID).append("\n");
                sb.append("reasonForBlackList: ").append(reasonForBlackList).append("\n");
                sb.append("firstRecordBlackTimestampMs: ").append(firstRecordBlackTimes).append("\n");
                sb.append("lastRecordBlackTimestampMs: ").append(lastRecordBlackTimes).append("\n");
                sb.append("lastBlackTimestampMs: ").append(lastBlackTimes).append("\n");
                sb.append("recordBlackListCount: ").append(recordBlackListCount).append("\n");
                sb.append("Config.do_add_backend_black_list_threshold_seconds: ")
                    .append(Config.do_add_backend_black_list_threshold_seconds).append("\n");
                sb.append("Config.stay_in_backend_black_list_threshold_seconds: ")
                    .append(Config.stay_in_backend_black_list_threshold_seconds).append("\n");
                return sb.toString();
            } finally {
                lock.unlock();
            }
        }
    }

    private static AtomicLong nextId = new AtomicLong(0);

    // backend id -> (try time, reason)
    // There will be multi threads to read and modify this map.
    // But only one thread (UpdateBlacklistThread) will modify the `Pair`.
    // So using concurrent map is enough
    private static Map<Long, BlackListInfo> blacklistBackends = Maps.newConcurrentMap();
    private static UpdateBlacklistThread updateBlacklistThread;

    public static void init() {
        updateBlacklistThread = new UpdateBlacklistThread();
        updateBlacklistThread.start();
    }

    public static TNetworkAddress getHost(long backendId,
                                          List<TScanRangeLocation> locations,
                                          ImmutableMap<Long, Backend> backends,
                                          Reference<Long> backendIdRef)
            throws UserException {
        if (CollectionUtils.isEmpty(locations) || backends == null || backends.isEmpty()) {
            throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("getHost backendID={}, backendSize={}", backendId, backends.size());
        }

        Backend backend = backends.get(backendId);

        if (isAvailable(backend)) {
            backendIdRef.setRef(backendId);
            return new TNetworkAddress(backend.getHost(), backend.getBePort());
        } else {
            for (TScanRangeLocation location : locations) {
                if (location.backend_id == backendId) {
                    continue;
                }
                // choose the first alive backend(in analysis stage, the locations are random)
                Backend candidateBackend = backends.get(location.backend_id);
                if (isAvailable(candidateBackend)) {
                    backendIdRef.setRef(location.backend_id);
                    return new TNetworkAddress(candidateBackend.getHost(), candidateBackend.getBePort());
                }
            }
        }

        // no backend returned
        throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG
                + getBackendErrorMsg(locations.stream().map(l -> l.backend_id).collect(Collectors.toList()),
                        backends, locations.size()));
    }

    public static TScanRangeLocation getLocation(TScanRangeLocation minLocation,
                                          List<TScanRangeLocation> locations,
                                          ImmutableMap<Long, Backend> backends,
                                          Reference<Long> backendIdRef)
            throws UserException {
        if (CollectionUtils.isEmpty(locations) || backends == null || backends.isEmpty()) {
            throw new UserException("scan range location or candidate backends is empty");
        }
        Backend backend = backends.get(minLocation.backend_id);
        if (isAvailable(backend)) {
            backendIdRef.setRef(minLocation.backend_id);
            return minLocation;
        } else {
            for (TScanRangeLocation location : locations) {
                if (location.backend_id == minLocation.backend_id) {
                    continue;
                }
                // choose the first alive backend(in analysis stage, the locations are random)
                Backend candidateBackend = backends.get(location.backend_id);
                if (isAvailable(candidateBackend)) {
                    backendIdRef.setRef(location.backend_id);
                    return location;
                }
            }
        }

        // no backend returned
        throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG
                + getBackendErrorMsg(locations.stream().map(l -> l.backend_id).collect(Collectors.toList()),
                        backends, locations.size()));
    }

    public static TNetworkAddress getHost(ImmutableMap<Long, Backend> backends,
                                          Reference<Long> backendIdRef)
            throws UserException {
        if (backends.isEmpty()) {
            throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
        }
        long id = nextId.getAndIncrement() % backends.size();
        Map.Entry<Long, Backend> backendEntry = backends.entrySet().stream().skip(id).filter(
                e -> isAvailable(e.getValue())).findFirst().orElse(null);
        if (backendEntry == null && id > 0) {
            backendEntry = backends.entrySet().stream().limit(id).filter(
                e -> isAvailable(e.getValue())).findFirst().orElse(null);
        }
        if (backendEntry != null) {
            Backend backend = backendEntry.getValue();
            backendIdRef.setRef(backendEntry.getKey());
            return new TNetworkAddress(backend.getHost(), backend.getBePort());
        }
        // no backend returned
        throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG
                + getBackendErrorMsg(Lists.newArrayList(backends.keySet()), backends, 3));
    }

    // get the reason why backends can not be chosen.
    private static String getBackendErrorMsg(List<Long> backendIds, ImmutableMap<Long, Backend> backends, int limit) {
        List<String> res = Lists.newArrayList();
        for (int i = 0; i < backendIds.size() && i < limit; i++) {
            long beId = backendIds.get(i);
            Backend be = backends.get(beId);
            BlackListInfo blackListInfo = blacklistBackends.get(beId);
            if (be == null) {
                res.add(beId + ": not exist");
            } else if (!be.isAlive()) {
                res.add(beId + ": not alive");
            } else if (blackListInfo != null && blackListInfo.isBlacked()) {
                res.add(beId + ": in black list(" + blackListInfo.getReasonForBlackList() + ")");
            } else if (!be.isQueryAvailable()) {
                res.add(beId + ": disable query");
            } else {
                res.add(beId + ": unknown");
            }
        }
        return res.toString();
    }

    public static void addToBlacklist(Long backendID, String reason) {
        if (backendID == null || Config.disable_backend_black_list || Config.isCloudMode()) {
            LOG.warn("ignore backend black list for backend: {}, disabled: {}, is cloud: {}",
                    backendID, Config.disable_backend_black_list, Config.isCloudMode());
            return;
        }

        BlackListInfo blackListInfo = blacklistBackends.putIfAbsent(backendID, new BlackListInfo());
        if (blackListInfo == null) {
            blackListInfo = blacklistBackends.get(backendID);
        }

        blackListInfo.tryAddBlackList(reason);
    }

    public static boolean isAvailable(Backend backend) {
        if (backend == null) {
            return false;
        }
        if (!backend.isQueryAvailable()) {
            return false;
        }
        BlackListInfo blackListInfo = blacklistBackends.get(backend.getId());
        if (blackListInfo != null && blackListInfo.isBlacked()) {
            return false;
        }
        return true;
    }

    private static class UpdateBlacklistThread implements Runnable {
        private static final Logger LOG = LogManager.getLogger(UpdateBlacklistThread.class);
        private static Thread thread;

        public UpdateBlacklistThread() {
            thread = new Thread(this, "UpdateBlacklistThread");
            thread.setDaemon(true);
        }

        public void start() {
            thread.start();
        }

        @Override
        public void run() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("UpdateBlacklistThread is start to run");
            }
            while (true) {
                try {
                    Thread.sleep(1000L);
                    SystemInfoService clusterInfoService = Env.getCurrentSystemInfo();

                    Iterator<Map.Entry<Long, SimpleScheduler.BlackListInfo>>
                            iterator = blacklistBackends.entrySet().iterator();

                    while (iterator.hasNext()) {
                        Map.Entry<Long, SimpleScheduler.BlackListInfo> entry = iterator.next();
                        Long backendId = entry.getKey();
                        Backend backend = clusterInfoService.getBackend(backendId);
                        // remove from blacklist if backend does not exist anymore
                        if (backend == null) {
                            iterator.remove();
                            LOG.info("remove backend {} from black list because it does not exist", backendId);
                        } else {
                            BlackListInfo blackListInfo = entry.getValue();
                            if (backend.isAlive() || blackListInfo.shouldBeRemoved()) {
                                iterator.remove();
                                LOG.info("remove backend {} from black list. backend is alive: {}",
                                        backendId, backend.isAlive());
                            } else {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("blacklistBackends {}", blackListInfo.toString());
                                }
                            }
                        }
                    }
                } catch (Throwable ex) {
                    LOG.warn("blacklist thread exception", ex);
                }
            }
        }
    }

    public static TNetworkAddress getHostByCurrentBackend(Map<TNetworkAddress, Long> addressToBackendID) {
        long id = nextId.getAndIncrement() % addressToBackendID.size();
        return addressToBackendID.keySet().stream().skip(id).findFirst().orElse(null);
    }
}
