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

package org.apache.doris.qe.cache;

import org.apache.doris.catalog.Env;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Use consistent hashing to find the BE corresponding to the key to
 * avoid the change of BE leading to failure to hit the Cache
 */
public class CacheCoordinator {
    private static final Logger LOG = LogManager.getLogger(CacheCoordinator.class);
    private static final int VIRTUAL_NODES = 10;
    private static final int REFRESH_NODE_TIME = 300000;
    public boolean debugModel = false;
    private Hashtable<Long, Backend> realNodes = new Hashtable<>();
    private SortedMap<Long, Backend> virtualNodes = new TreeMap<>();
    private static Lock belock = new ReentrantLock();

    private long lastRefreshTime;
    private static CacheCoordinator cachePartition;

    public static CacheCoordinator getInstance() {
        if (cachePartition == null) {
            cachePartition = new CacheCoordinator();
        }
        return cachePartition;
    }

    protected CacheCoordinator() {
    }

    /**
     * Using the consistent hash and the hi part of sqlkey to get the backend node
     *
     * @param sqlKey 128 bit's sql md5
     * @return Backend
     */
    public Backend findBackend(Types.PUniqueId sqlKey) {
        resetBackend();
        Backend virtualNode = null;
        try {
            belock.lock();
            SortedMap<Long, Backend> headMap = virtualNodes.headMap(sqlKey.getHi());
            SortedMap<Long, Backend> tailMap = virtualNodes.tailMap(sqlKey.getHi());
            int retryTimes = 0;
            while (true) {
                if (tailMap == null || tailMap.size() == 0) {
                    tailMap = headMap;
                    retryTimes += 1;
                }
                Long key = tailMap.firstKey();
                virtualNode = tailMap.get(key);
                if (SimpleScheduler.isAvailable(virtualNode)) {
                    break;
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("backend {} not alive, key {}, retry {}", virtualNode.getId(), key, retryTimes);
                    }
                    virtualNode = null;
                }
                tailMap = tailMap.tailMap(key + 1);
                retryTimes++;
                if (retryTimes >= 5) {
                    LOG.warn("find backend, reach max retry times {}", retryTimes);
                    break;
                }
            }
        } finally {
            belock.unlock();
        }
        return virtualNode;
    }

    public void resetBackend() {
        if (System.currentTimeMillis() - this.lastRefreshTime < REFRESH_NODE_TIME) {
            return;
        }
        try {
            belock.lock();
            ImmutableMap<Long, Backend> idToBackend = Env.getCurrentSystemInfo().getIdToBackend();
            if (idToBackend != null) {
                if (!debugModel) {
                    clearBackend(idToBackend);
                }
                for (Backend backend : idToBackend.values().asList()) {
                    addBackend(backend);
                }
            }
            this.lastRefreshTime = System.currentTimeMillis();
        } finally {
            belock.unlock();
        }
    }

    private void clearBackend(ImmutableMap<Long, Backend> idToBackend) {
        Iterator<Long> itr = realNodes.keySet().iterator();
        Long bid;
        while (itr.hasNext()) {
            bid = itr.next();
            if (!idToBackend.containsKey(bid)) {
                for (int i = 0; i < VIRTUAL_NODES; i++) {
                    String nodeName = String.valueOf(bid) + "::" + String.valueOf(i);
                    Types.PUniqueId nodeId = CacheBeProxy.getMd5(nodeName);
                    virtualNodes.remove(nodeId.getHi());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("remove backend id {}, virtual node name {} hashcode {}",
                                bid, nodeName, nodeId.getHi());
                    }
                }
                itr.remove();
            }
        }
    }

    public void addBackend(Backend backend) {
        if (realNodes.putIfAbsent(backend.getId(), backend) != null) {
            return;
        }
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            String nodeName = backend.getId() + "::" + i;
            Types.PUniqueId nodeId = CacheBeProxy.getMd5(nodeName);
            virtualNodes.put(nodeId.getHi(), backend);
            if (LOG.isDebugEnabled()) {
                LOG.debug("add backend id {}, virtual node name {} hashcode {}",
                        backend.getId(), nodeName, nodeId.getHi());
            }
        }
    }

    public List<Backend> getBackendList() {
        List<Backend> backendList = Lists.newArrayList();
        for (HashMap.Entry<Long, Backend> entry : realNodes.entrySet()) {
            backendList.add(entry.getValue());
        }
        return backendList;
    }
}
