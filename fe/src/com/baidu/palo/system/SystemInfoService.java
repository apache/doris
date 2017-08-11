// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.system;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.log.Log;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ClientPool;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.FeConstants;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.system.Backend.BackendState;
import com.baidu.palo.system.BackendEvent.BackendEventType;
import com.baidu.palo.thrift.HeartbeatService;
import com.baidu.palo.thrift.TBackendInfo;
import com.baidu.palo.thrift.THeartbeatResult;
import com.baidu.palo.thrift.TMasterInfo;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TStatusCode;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;

public class SystemInfoService extends Daemon {
    public static final String DEFAULT_CLUSTER = "default_cluster";
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);

    private volatile AtomicReference<ImmutableMap<Long, Backend>> idToBackendRef;
    private volatile AtomicReference<ImmutableMap<Long, HeartbeatHandler>> idToHeartbeatHandlerRef;
    private volatile AtomicReference<ImmutableMap<Long, AtomicLong>> idToReportVersionRef; // no
                                                                                           // need
                                                                                           // to
                                                                                           // persist

    private final ExecutorService executor;

    private final EventBus eventBus;

    private static volatile AtomicReference<TMasterInfo> masterInfo = new AtomicReference<TMasterInfo>();

    // last backend id used by round robin for sequential choosing backends for
    // tablet creation
    private ConcurrentHashMap<String, Long> lastBackendIdForCreationMap;
    // last backend id used by round robin for sequential choosing backends in
    // other jobs
    private ConcurrentHashMap<String, Long> lastBackendIdForOtherMap;

    private long lastBackendIdForCreation = -1;
    private long lastBackendIdForOther = -1;

    public SystemInfoService() {
        super("cluster info service", FeConstants.heartbeat_interval_second * 1000);
        idToBackendRef = new AtomicReference<ImmutableMap<Long, Backend>>(ImmutableMap.<Long, Backend> of());
        idToHeartbeatHandlerRef = new AtomicReference<ImmutableMap<Long, HeartbeatHandler>>(
                ImmutableMap.<Long, HeartbeatHandler> of());
        idToReportVersionRef = new AtomicReference<ImmutableMap<Long, AtomicLong>>(
                ImmutableMap.<Long, AtomicLong> of());

        executor = Executors.newCachedThreadPool();

        eventBus = new EventBus("backendEvent");

        lastBackendIdForCreationMap = new ConcurrentHashMap<String, Long>();
        lastBackendIdForOtherMap = new ConcurrentHashMap<String, Long>();
    }

    public EventBus getEventBus() {
        return this.eventBus;
    }

    public void setMaster(String masterHost, int masterPort, int clusterId, long epoch) {
        TMasterInfo tMasterInfo = new TMasterInfo(new TNetworkAddress(masterHost, masterPort), clusterId, epoch);
        masterInfo.set(tMasterInfo);
    }

    public void addBackends(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same backend already exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addBackend(pair.first, pair.second);
        }
    }

    // for test
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        copiedBackends.put(backend.getId(), backend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef.set(newIdToBackend);
    }
    
    private void addBackend(String host, int heartbeatPort) throws DdlException {
        Backend newBackend = new Backend(Catalog.getInstance().getNextId(), host, heartbeatPort);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef.set(newIdToBackend);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef.get());
        copiedReportVerions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVerions);
        idToReportVersionRef.set(newIdToReportVersion);

        // update idToHeartbeatHandler
        Map<Long, HeartbeatHandler> copiedHeartbeatHandlersMap = Maps.newHashMap(idToHeartbeatHandlerRef.get());
        TNetworkAddress tNetworkAddress = new TNetworkAddress(newBackend.getHost(), newBackend.getHeartbeatPort());
        HeartbeatHandler heartbeatHandler = new HeartbeatHandler(newBackend, tNetworkAddress);
        copiedHeartbeatHandlersMap.put(newBackend.getId(), heartbeatHandler);
        ImmutableMap<Long, HeartbeatHandler> newIdToHeartbeatHandler = ImmutableMap.copyOf(copiedHeartbeatHandlersMap);
        idToHeartbeatHandlerRef.set(newIdToHeartbeatHandler);

        // log
        Catalog.getInstance().getEditLog().logAddBackend(newBackend);
        LOG.info("add backend[" + newBackend.getId() + ". " + newBackend.getHost() + ":" + newBackend.getHeartbeatPort()
                + ":" + newBackend.getBePort() + ":" + newBackend.getBePort() + ":" + newBackend.getHttpPort() + "]");
    }

    public void checkBackendsExist(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check if exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("Backend does not exist[" + pair.first + ":" + pair.second + "]");
            }
        }
    }

    public void dropBackends(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("backend does not exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropBackend(pair.first, pair.second);
        }
    }

    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }

        dropBackend(backend.getHost(), backend.getHeartbeatPort());
    }

    private void dropBackend(String host, int heartbeatPort) throws DdlException {
        if (getBackendWithHeartbeatPort(host, heartbeatPort) == null) {
            throw new DdlException("backend does not exists[" + host + ":" + heartbeatPort + "]");
        }

        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);

        // publish
        eventBus.post(new BackendEvent(BackendEventType.BACKEND_DROPPED, "backend has been dropped",
                Long.valueOf(droppedBackend.getId())));

        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        copiedBackends.remove(droppedBackend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef.set(newIdToBackend);

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef.get());
        copiedReportVerions.remove(droppedBackend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVerions);
        idToReportVersionRef.set(newIdToReportVersion);

        // update idToHeartbeatHandler
        Map<Long, HeartbeatHandler> copiedHeartbeatHandlersMap = Maps.newHashMap(idToHeartbeatHandlerRef.get());
        copiedHeartbeatHandlersMap.remove(droppedBackend.getId());
        ImmutableMap<Long, HeartbeatHandler> newIdToHeartbeatHandler = ImmutableMap.copyOf(copiedHeartbeatHandlersMap);
        idToHeartbeatHandlerRef.set(newIdToHeartbeatHandler);

        // log
        Catalog.getInstance().getEditLog().logDropBackend(droppedBackend);
        LOG.info("drop {}", droppedBackend);
    }

    // only for test
    public void dropAllBackend() {
        // update idToBackend
        idToBackendRef.set(ImmutableMap.<Long, Backend> of());

        // update idToReportVersion
        idToReportVersionRef.set(ImmutableMap.<Long, AtomicLong> of());

        // update idToHeartbeatHandler
        idToHeartbeatHandlerRef.set(ImmutableMap.<Long, HeartbeatHandler> of());
    }

    public Backend getBackend(long backendId) {
        return idToBackendRef.get().get(backendId);
    }

    public boolean checkBackendAvailable(long backendId) {
        Backend backend = idToBackendRef.get().get(backendId);
        if (backend == null || !backend.isAlive() || backend.isDecommissioned()) {
            return false;
        }
        return true;
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef.get();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithBePort(String host, int bePort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef.get();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getBePort() == bePort) {
                return backend;
            }
        }
        return null;
    }

    private int getRandom(int start, int end) {
        return (int) (Math.random() * (end - start) + start);
    }

    public List<Long> getBackendIds(boolean needAlive) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef.get();
        List<Long> backendIds = new ArrayList<Long>(idToBackend.keySet());
        if (!needAlive) {
            return backendIds;
        } else {
            Iterator<Long> iter = backendIds.iterator();
            while (iter.hasNext()) {
                Backend backend = this.getBackend(iter.next());
                if (backend == null || !backend.isAlive()) {
                    iter.remove();
                }
            }
            return backendIds;
        }
    }

    /**
     * chose be to create cluster
     * 
     * @param clusterName
     * @param num
     * @return
     */
    public List<Long> createCluster(String clusterName, int num) {
        final List<Long> ret = Lists.newArrayList();
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final Map<String, List<Backend>> classMap = getHostBackendsMap(true, true, false);
       
        if (num > classMap.size()) {
            return null;
        }
        // to select host where has more free be
        int count = num;
        while (count-- > 0) {
            List<Backend> tmp = null;
            for (List<Backend> backendList : classMap.values()) {
                if (tmp == null) {
                    tmp = backendList;
                } else {
                    if (tmp.size() < backendList.size()) {
                        tmp = backendList;
                    }
                }
            }
            // random select a backend
            if (tmp != null && tmp.size() > 0) {
                ret.add(tmp.get(0).getId());
                classMap.remove(tmp.get(0).getHost());
            }
        }

        if (ret.size() != num) {
            return null;
        }

        // set be state and owner
        final Iterator<Long> iterator = ret.iterator();
        while (iterator.hasNext()) {
            final Long id = iterator.next();
            final Backend backend = copiedBackends.get(id);
            backend.setOwnerClusterName(clusterName);
            backend.setBackendState(BackendState.using);
            copiedBackends.put(backend.getId(), backend);
            Catalog.getInstance().getEditLog().logBackendStateChange(backend);
        }
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef.set(newIdToBackend);
        
        lastBackendIdForCreationMap.put(clusterName, (long) -1);
        lastBackendIdForOtherMap.put(clusterName, (long) -1);
        return ret;
    }

    
    /**
     * remove backends in cluster
     * 
     * @param backendList
     * @throws DdlException
     */
    public void releaseBackends(String clusterName, boolean log) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final List<Long> backendList = getClusterBackendIds(clusterName);
        final Iterator<Long> iterator = backendList.iterator();

        while (iterator.hasNext()) {
            final Long id = iterator.next();
            if (!copiedBackends.containsKey(id)) {
                Log.warn(" cluster contain backend that don't exit");
            } else {
                final Backend backend = copiedBackends.get(id);
                backend.setBackendState(BackendState.free);
                backend.setOwnerClusterName("");
                if (log) {
                    Catalog.getInstance().getEditLog().logBackendStateChange(backend);
                }
            }
        }
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef.set(newIdToBackend);

        lastBackendIdForCreationMap.remove(clusterName);
        lastBackendIdForOtherMap.remove(clusterName);
    }

    /**
     * select host where has least free be , be's state become free when
     * decommission finish
     * 
     * @param backendList
     * @param num
     * @return
     */
    public List<Long> calculateDecommissionBackends(String name, int num) {
        final List<Long> ret = new ArrayList<Long>();
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final Map<String, List<Backend>> classMap = getHostBackendsMap(false, false, true);
        final List<Long> backendList = getClusterBackendIds(name);
        final Map<String, List<Backend>> clusterClassMap = Maps.newHashMap();
        if (backendList.size() < num) {
            return null;
        }
        
        Iterator<Long> iterator = backendList.iterator();
        while (iterator.hasNext()) {
            final Long id = iterator.next();
            final Backend backend = copiedBackends.get(id);
            clusterClassMap.put(backend.getHost(), classMap.get(backend.getHost()));
        }
        int count = num;
        while (count-- > 0) {
            List<Backend> tmp = null;
            String host = null;
            final Iterator<Map.Entry<String, List<Backend>>> iter = clusterClassMap.entrySet().iterator();
            // to select host where has least free be
            while (iter.hasNext()) {
                final Map.Entry<String, List<Backend>> entry = (Map.Entry<String, List<Backend>>) iter.next();
                String key = (String) entry.getKey();
                List<Backend> value = (List<Backend>) entry.getValue();
                
                if (tmp == null) {
                    tmp = value;
                    host = key;
                } else {
                    if (tmp.size() > value.size()) {
                        tmp = value;
                        host = key;
                    }
                }
            }
            if (tmp != null) {
                iterator = backendList.iterator();
                while (iterator.hasNext()) {
                    final Long id = iterator.next();
                    final Backend backend = copiedBackends.get(id);
                    if (backend.getHost().equals(host)) {
                        ret.add(id);                       
                    }
                }
                clusterClassMap.remove(host);
            }        
        }

        if (ret.size() < num) {
            return null;
        }
        
        return ret;
    }

    /**
     * get expansion's backend id list
     * 
     * @param name
     * @param num
     * @return
     */
    public List<Long> calculateExpansionBackends(String name, int num) {
        final List<Long> ret = new ArrayList<Long>();
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final Map<String, List<Backend>> classMap = getHostBackendsMap(true, true, false);
        final List<Long> clusterBackends = getClusterBackendIds(name);

        Iterator<Long> iterator = clusterBackends.iterator();
        while (iterator.hasNext()) {
            final Long id = iterator.next();
            final Backend backend = copiedBackends.get(id);
            if (classMap.containsKey(backend.getHost())) {
                classMap.remove(backend.getHost());
            }
        }

        if (num > classMap.size()) {
            return null;
        }
        
        int count = num;
        while (count-- > 0) {
            List<Backend> tmp = null;
            for (List<Backend> list : classMap.values()) {
                if (tmp == null) {
                    tmp = list;
                } else {
                    if (tmp.size() < list.size()) {
                        tmp = list;
                    }
                }
            }

            // random select a backend
            if (tmp != null && tmp.size() > 0) {
                ret.add(tmp.get(0).getId());
                classMap.remove(tmp.get(0).getHost());
            }
        }

        if (ret.size() != num) {
            return null;
        }

        // set be state and owner/
        iterator = ret.iterator();
        while (iterator.hasNext()) {
            final Long id = iterator.next();
            final Backend backend = copiedBackends.get(id);
            backend.setOwnerClusterName(name);
            backend.setBackendState(BackendState.using);
            copiedBackends.put(backend.getId(), backend);
            Catalog.getInstance().getEditLog().logBackendStateChange(backend);
        }
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef.set(newIdToBackend);
        return ret;
    }

    /**
     * get cluster's backend id list
     * 
     * @param name
     * @return
     */
    public List<Backend> getClusterBackends(String name) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final List<Backend> ret = new ArrayList<Backend>();

        if (Strings.isNullOrEmpty(name)) {
            return null;
        }

        for (Backend backend : copiedBackends.values()) {
            if (name.equals(backend.getOwnerClusterName())) {
                ret.add(backend);
            }
        }
        return ret;
    }

    /**
     * get cluster's backend id list
     * 
     * @param name
     * @return
     */
    public List<Backend> getClusterBackends(String name, boolean needAlive) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final List<Backend> ret = new ArrayList<Backend>();

        if (Strings.isNullOrEmpty(name)) {
            return null;
        }

        if (needAlive) {
            for (Backend backend : copiedBackends.values()) {
                if (name.equals(backend.getOwnerClusterName())) {
                    if (backend != null && backend.isAlive()) {
                        ret.add(backend);
                    }
                }
            }
        } else {
            for (Backend backend : copiedBackends.values()) {
                if (name.equals(backend.getOwnerClusterName())) {
                    ret.add(backend);
                }
            }
        }

        return ret;
    }

    /**
     * get cluster's backend id list
     * 
     * @param name
     * @return
     */
    public List<Long> getClusterBackendIds(String name) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final List<Long> ret = new ArrayList<Long>();

        if (Strings.isNullOrEmpty(name)) {
            return null;
        }

        for (Backend backend : copiedBackends.values()) {
            if (name.equals(backend.getOwnerClusterName())) {
                ret.add(backend.getId());
            }
        }
        return ret;
    }

    /**
     * get cluster's backend id list
     * 
     * @param name
     * @return
     */
    public List<Long> getClusterBackendIds(String name, boolean needAlive) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final List<Long> ret = new ArrayList<Long>();

        if (Strings.isNullOrEmpty(name)) {
            return null;
        }

        if (needAlive) {
            for (Backend backend : copiedBackends.values()) {
                if (name.equals(backend.getOwnerClusterName())) {
                    if (backend != null && backend.isAlive()) {
                        ret.add(backend.getId());
                    }
                }
            }
        } else {
            for (Backend backend : copiedBackends.values()) {
                if (name.equals(backend.getOwnerClusterName())) {
                    ret.add(backend.getId());
                }
            }
        }

        return ret;
    }

    /**
     * return backend list in every host
     * 
     * @return
     */
    private Map<String, List<Backend>> getHostBackendsMap(boolean isAlive, boolean isFree, boolean canBeDecomission) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        final Map<String, List<Backend>> classMap = Maps.newHashMap();

        // to select backend where state is free
        for (Backend backend : copiedBackends.values()) {
            if ((isAlive && !backend.isAlive()) || (isFree && !backend.isFreeFromCluster())
                    || (!canBeDecomission && backend.isDecommissioned())) {
                continue;
            }
            if (classMap.containsKey(backend.getHost())) {
                final List<Backend> list = classMap.get(backend.getHost());
                list.add(backend);
                classMap.put(backend.getHost(), list);
            } else {
                final List<Backend> list = new ArrayList<Backend>();
                list.add(backend);
                classMap.put(backend.getHost(), list);
            }
        }
        return classMap;
    }

    // choose backends by round robin
    // return null if not enough backend
    // use synchronized to run serially
    public synchronized List<Long> seqChooseBackendIds(int backendNum, boolean needAlive, boolean isCreate,
            String clusterName) {
        long lastBackendId = -1L;

        if (clusterName.equals(DEFAULT_CLUSTER)) {
            if (isCreate) {
                lastBackendId = lastBackendIdForCreation;
            } else {
                lastBackendId = lastBackendIdForOther;
            }
        } else {
            if (isCreate) {
                if (lastBackendIdForCreationMap.containsKey(clusterName)) {
                    lastBackendId = lastBackendIdForCreationMap.get(clusterName);
                } else {
                    lastBackendId = -1;
                    lastBackendIdForCreationMap.put(clusterName, lastBackendId);
                }
            } else {             
                if (lastBackendIdForOtherMap.containsKey(clusterName)) {
                    lastBackendId = lastBackendIdForOtherMap.get(clusterName);
                } else {
                    lastBackendId = -1;
                    lastBackendIdForOtherMap.put(clusterName, lastBackendId);
                }
            }
        }

        List<Long> backendIds = Lists.newArrayList();
        final List<Backend> backends = getClusterBackends(clusterName);
        // get last backend index
        int lastBackendIndex = -1;
        int index = -1;
        for (Backend backend : backends) {
            index++;
            if (backend.getId() == lastBackendId) {
                lastBackendIndex = index;
                break;
            }
        }

        Iterator<Backend> iterator = Iterators.cycle(backends);
        index = -1;
        boolean failed = false;
        // 2 cycle at most
        int maxIndex = 2 * backends.size();
        while (iterator.hasNext() && backendIds.size() < backendNum) {
            Backend backend = iterator.next();
            index++;
            if (index <= lastBackendIndex) {
                continue;
            }

            if (index > maxIndex) {
                failed = true;
                break;
            }

            if (needAlive) {
                if (!backend.isAlive() || backend.isDecommissioned()) {
                    continue;
                }
            }

            long backendId = backend.getId();
            if (!backendIds.contains(backendId)) {
                backendIds.add(backendId);
                lastBackendId = backendId;
            } else {
                failed = true;
                break;
            }
        }

        if (clusterName.equals(DEFAULT_CLUSTER)) {
            if (isCreate) {
                lastBackendIdForCreation = lastBackendId;
            } else {
                lastBackendIdForOther = lastBackendId;
            }
        } else {
            // update last backendId
            if (isCreate) {
                lastBackendIdForCreationMap.put(clusterName, lastBackendId);
            } else {
                lastBackendIdForOtherMap.put(clusterName, lastBackendId);
            }
        }

        if (backendIds.size() != backendNum) {
            failed = true;
        }

        if (!failed) {
            return backendIds;
        }

        // debug
        for (Backend backend : backends) {
            LOG.debug("random select: {}", backend.toString());
        }

        return null;
    }

    public ImmutableMap<Long, Backend> getIdToBackend() {
        return idToBackendRef.get();
    }

    public long getBackendReportVersion(long backendId) {
        AtomicLong atomicLong = null;
        if ((atomicLong = idToReportVersionRef.get().get(backendId)) == null) {
            return -1L;
        } else {
            return atomicLong.get();
        }
    }

    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId) {
        AtomicLong atomicLong = null;
        if ((atomicLong = idToReportVersionRef.get().get(backendId)) != null) {
            Database db = Catalog.getInstance().getDb(dbId);
            if (db != null) {
                db.readLock();
                try {
                    atomicLong.set(newReportVersion);
                } finally {
                    db.readUnlock();
                }
            }
        }
    }

    public long saveBackends(DataOutputStream dos, long checksum) throws IOException {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef.get();
        int backendCount = idToBackend.size();
        checksum ^= backendCount;
        dos.writeInt(backendCount);
        for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
            long key = entry.getKey();
            checksum ^= key;
            dos.writeLong(key);
            entry.getValue().write(dos);
        }
        return checksum;
    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (int i = 0; i < count; i++) {
            long key = dis.readLong();
            checksum ^= key;
            Backend backend = Backend.read(dis);
            replayAddBackend(backend);
        }
        return checksum;
    }

    public void clear() {
        this.idToBackendRef = null;
        this.idToHeartbeatHandlerRef = null;
        this.idToReportVersionRef = null;
    }

    public void registerObserver(SystemInfoObserver observer) {
        LOG.info("register observer {} {}: ", observer.getName(), observer.getClass());
        this.eventBus.register(observer);
    }

    public void unregisterObserver(SystemInfoObserver observer) {
        this.eventBus.unregister(observer);
    }

    public static Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        String[] pair = hostPort.split(":");
        if (pair.length != 2) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        String host = pair[0];
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("Host is null");
        }

        int heartbeatPort = -1;
        try {
            // validate host
            if (!InetAddressValidator.getInstance().isValid(host)) {
                // maybe this is a hostname
                // if no IP address for the host could be found, 'getByName'
                // will throw
                // UnknownHostException
                InetAddress inetAddress = InetAddress.getByName(host);
                host = inetAddress.getHostAddress();
            }

            if (host.equals("127.0.0.1")) {
                InetAddress inetAddress = InetAddress.getLocalHost();
                host = inetAddress.getHostAddress();
            }

            // validate port
            heartbeatPort = Integer.valueOf(pair[1]);

            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return new Pair<String, Integer>(host, heartbeatPort);
        } catch (UnknownHostException e) {
            throw new AnalysisException("Unknown host: " + e.getMessage());
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
        }
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_23) {
            newBackend.setOwnerClusterName(DEFAULT_CLUSTER);
            newBackend.setBackendState(BackendState.using);
        }
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        copiedBackends.put(newBackend.getId(), newBackend);
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef.set(newIdToBackend);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef.get());
        copiedReportVerions.put(newBackend.getId(), new AtomicLong(0L));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVerions);
        idToReportVersionRef.set(newIdToReportVersion);

        // update idToHeartbeatHandler
        Map<Long, HeartbeatHandler> copiedHeartbeatHandlersMap = Maps.newHashMap(idToHeartbeatHandlerRef.get());
        TNetworkAddress tNetworkAddress = new TNetworkAddress(newBackend.getHost(), newBackend.getHeartbeatPort());
        HeartbeatHandler heartbeatHandler = new HeartbeatHandler(newBackend, tNetworkAddress);
        copiedHeartbeatHandlersMap.put(newBackend.getId(), heartbeatHandler);
        ImmutableMap<Long, HeartbeatHandler> newIdToHeartbeatHandler = ImmutableMap.copyOf(copiedHeartbeatHandlersMap);
        idToHeartbeatHandlerRef.set(newIdToHeartbeatHandler);
    }

    public void replayDropBackend(Backend backend) {
        LOG.debug("replayDropBackend: {}", backend);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef.get());
        copiedBackends.remove(backend.getId());
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef.set(newIdToBackend);

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef.get());
        copiedReportVerions.remove(backend.getId());
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVerions);
        idToReportVersionRef.set(newIdToReportVersion);

        // update idToHeartbeatHandler
        Map<Long, HeartbeatHandler> copiedHeartbeatHandlersMap = Maps.newHashMap(idToHeartbeatHandlerRef.get());
        copiedHeartbeatHandlersMap.remove(backend.getId());
        ImmutableMap<Long, HeartbeatHandler> newIdToHeartbeatHandler = ImmutableMap.copyOf(copiedHeartbeatHandlersMap);
        idToHeartbeatHandlerRef.set(newIdToHeartbeatHandler);
    }

    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        memoryBe.setBePort(be.getBePort());
        memoryBe.setAlive(be.isAlive());
        memoryBe.setDecommissioned(be.isDecommissioned());
        memoryBe.setHttpPort(be.getHttpPort());
        memoryBe.setBeRpcPort(be.getBeRpcPort());
        memoryBe.setLastUpdateMs(be.getLastUpdateMs());
        memoryBe.setLastStartTime(be.getLastStartTime());
        memoryBe.setDisks(be.getDisks());
        memoryBe.setBackendState(be.getBackendState());
        memoryBe.setOwnerClusterName(be.getOwnerClusterName());
        memoryBe.setDecommissionType(be.getDecommissionType());
    }

    public long getAvailableCapacityB() {
        long capacity = 0L;
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef.get();
        for (Backend backend : idToBackend.values()) {
            if (backend.isDecommissioned()) {
                capacity -= backend.getTotalCapacityB() - backend.getAvailableCapacityB();
            } else {
                capacity += backend.getAvailableCapacityB();
            }
        }
        return capacity;
    }

    public void checkCapacity() throws DdlException {
        if (getAvailableCapacityB() <= 0L) {
            throw new DdlException("Cluster has no available capacity");
        }
    }

    /**
     * now we will only check capacity of logic cluster when execute operation
     * 
     * @param clusterName
     * @throws DdlException
     */
    public void checkClusterCapacity(String clusterName) throws DdlException {
        if (getClusterBackends(clusterName).isEmpty()) {
            throw new DdlException("Cluster has no available capacity");
        }
    }

    @Override
    protected void runOneCycle() {
        ImmutableMap<Long, HeartbeatHandler> idToHeartbeatHandler = idToHeartbeatHandlerRef.get();
        Iterator<HeartbeatHandler> iterator = idToHeartbeatHandler.values().iterator();
        while (iterator.hasNext()) {
            HeartbeatHandler heartbeatHandler = iterator.next();
            executor.submit(heartbeatHandler);
        }
    }

    private class HeartbeatHandler implements Runnable {
        private Backend backend;
        private TNetworkAddress address;

        public HeartbeatHandler(Backend backend, TNetworkAddress networkAddress) {
            this.backend = backend;
            this.address = networkAddress;
        }

        @Override
        public void run() {
            long backendId = backend.getId();
            HeartbeatService.Client client = null;
            boolean ok = false;
            try {
                client = ClientPool.heartbeatPool.borrowObject(address);
                THeartbeatResult result = client.heartbeat(masterInfo.get());
                if (result.getStatus().getStatus_code() == TStatusCode.OK) {
                    TBackendInfo tBackendInfo = result.getBackend_info();
                    int bePort = tBackendInfo.getBe_port();
                    int httpPort = tBackendInfo.getHttp_port();
                    int beRpcPort = tBackendInfo.getBe_rpc_port();
                    backend.updateOnce(bePort, httpPort, beRpcPort);
                } else {
                    LOG.warn("failed to heartbeat backend[" + backendId + "]: " + result.getStatus().toString());
                    backend.setBad(eventBus);
                }
                ok = true;
                LOG.debug("backend[{}] host: {}, port: {}", backendId, backend.getHost(), backend.getHeartbeatPort());
            } catch (Exception e) {
                LOG.warn("backend[" + backendId + "] got Exception: ", e);
                backend.setBad(eventBus);
            } finally {
                if (ok) {
                    ClientPool.heartbeatPool.returnObject(address, client);
                } else {
                    ClientPool.heartbeatPool.invalidateObject(address, client);
                }
            }
        }
    }
}
