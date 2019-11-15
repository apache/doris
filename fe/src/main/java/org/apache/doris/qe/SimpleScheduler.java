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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Reference;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleScheduler {
    private static AtomicLong nextId = new AtomicLong(0);
    private static final Logger LOG = LogManager.getLogger(SimpleScheduler.class);

    private static Map<Long, Integer> blacklistBackends = Maps.newHashMap();
    private static Lock lock = new ReentrantLock();
    private static UpdateBlacklistThread updateBlacklistThread;

    static {
        updateBlacklistThread = new UpdateBlacklistThread();
        updateBlacklistThread.start();
    }
    
    public static TNetworkAddress getHost(long backendId, 
                                          List<TScanRangeLocation> locations,
                                          ImmutableMap<Long, Backend> backends,
                                          Reference<Long> backendIdRef) {
        if (locations == null || backends == null) {
            return null;
        }
        LOG.debug("getHost backendID={}, backendSize={}", backendId, backends.size());
        Backend backend = backends.get(backendId);
        lock.lock();
        try {
            if (backend != null && backend.isAlive() && !blacklistBackends.containsKey(backendId)) {
                backendIdRef.setRef(backendId);
                return new TNetworkAddress(backend.getHost(), backend.getBePort());
            } else {
                for (TScanRangeLocation location : locations) {
                    if (location.backend_id == backendId) {
                        continue;
                    } 
                    // choose the first alive backend(in analysis stage, the locations are random)
                    Backend candidateBackend = backends.get(location.backend_id);
                    if (candidateBackend != null && candidateBackend.isAlive()
                            && !blacklistBackends.containsKey(location.backend_id)) {
                        backendIdRef.setRef(location.backend_id);
                        return new TNetworkAddress(candidateBackend.getHost(), candidateBackend.getBePort());
                    }
                }
            } 
        } finally {
            lock.unlock();
        }
        // no backend returned
        return null;
    }
    
    public static TNetworkAddress getHost(ImmutableMap<Long, Backend> backends,
                                          Reference<Long> backendIdRef) {
        if (backends == null) {
            return null;
        }
        int backendSize = backends.size();
        if (backendSize == 0) {
            return null;
        }
        long id = nextId.getAndIncrement() % backendSize;

        List<Long> idToBackendId = Lists.newArrayList();
        idToBackendId.addAll(backends.keySet());
        Long backendId = idToBackendId.get((int) id);
        Backend backend = backends.get(backendId);
        
        if (backend != null && backend.isAlive() && !blacklistBackends.containsKey(backendId)) {
            backendIdRef.setRef(backendId);
            return new TNetworkAddress(backend.getHost(), backend.getBePort());
        } else {
            long candidateId = id + 1;  // get next candidate id
            for (int i = 0; i < backendSize; i ++, candidateId ++) {
                LOG.debug("i={} candidatedId={}", i, candidateId);
                if (candidateId >= backendSize) {
                    candidateId = 0;
                }
                if (candidateId == id) {
                    continue;
                }
                Long candidatebackendId = idToBackendId.get((int) candidateId);
                LOG.debug("candidatebackendId={}", candidatebackendId);
                Backend candidateBackend = backends.get(candidatebackendId);
                if (candidateBackend != null && candidateBackend.isAlive()
                        && !blacklistBackends.containsKey(candidatebackendId)) {
                    backendIdRef.setRef(candidatebackendId);
                    return new TNetworkAddress(candidateBackend.getHost(), candidateBackend.getBePort());
                }
            }
        }
        // no backend returned
        return null;
    }
    
    public static void updateBlacklistBackends(Long backendID) {
        if (backendID == null) {
            return;
        }
        lock.lock();
        try {
            int tryTime = FeConstants.heartbeat_interval_second + 1;
            blacklistBackends.put(backendID, tryTime);
            LOG.warn("add black list " + backendID);
        } finally {
            lock.unlock();
        }
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
            LOG.debug("UpdateBlacklistThread is start to run");
            while (true) {
                try {
                    Thread.sleep(1000L);
                    SystemInfoService clusterInfoService = Catalog.getCurrentSystemInfo();
                    LOG.debug("UpdateBlacklistThread retry begin");
                    lock.lock();
                    try {
                        Iterator<Map.Entry<Long, Integer>> iterator = blacklistBackends.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<Long, Integer> entry = iterator.next();
                            Long backendId = entry.getKey();
                            
                            // remove from blacklist if
                            // 1. backend does not exist anymore
                            // 2. backend is alive
                            if (clusterInfoService.getBackend(backendId) == null
                                    || clusterInfoService.checkBackendAvailable(backendId)) {
                                iterator.remove();
                                LOG.debug("remove backendID {} which is alive", backendId);
                            } else {
                                // 3. max try time is reach
                                Integer retryTimes = entry.getValue();
                                retryTimes = retryTimes - 1;
                                if (retryTimes <= 0) {
                                    iterator.remove();
                                    LOG.warn("remove backendID {}. reach max try time", backendId);
                                } else {
                                    entry.setValue(retryTimes);
                                    LOG.debug("blacklistBackends backendID={} retryTimes={}", backendId, retryTimes);
                                }
                            }
                        }
                    } finally {
                        lock.unlock();
                        LOG.debug("UpdateBlacklistThread retry end");
                    }
                    
                } catch (Throwable ex) {
                    LOG.warn("blacklist thread exception" + ex);
                }
            }
        }
    }
}
