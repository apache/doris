// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common.proc;

import com.baidu.palo.alter.DecommissionBackendJob.DecommissionType;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.cluster.Cluster;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.util.ListComparator;
import com.baidu.palo.common.util.TimeUtils;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class BackendsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BackendId").add("Cluster").add("IP").add("HostName").add("HeartbeatPort")
            .add("BePort").add("HttpPort").add("LastStartTime").add("LastHeartbeat").add("Alive")
            .add("SystemDecommissioned").add("ClusterDecommissioned").add("TabletNum").add("FreeSpace")
            .build();

    public static final int IP_INDEX = 2;
    public static final int HOSTNAME_INDEX = 3;

    private SystemInfoService clusterInfoService;

    public BackendsProcDir(SystemInfoService clusterInfoService) {
        this.clusterInfoService = clusterInfoService;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(clusterInfoService);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        final List<List<String>> backendInfos = getBackendInfos();
        for (List<String> backendInfo : backendInfos) {
            List<String> oneInfo = new ArrayList<String>(backendInfo.size());
            for (String info : backendInfo) {
                oneInfo.add(info);
            }
            result.addRow(oneInfo);
        }
        return result;
    }

    /**
     * get all backends of system
     * @return
     */
    public static List<List<String>> getBackendInfos() {
        return getClusterBackendInfos(null);
    }
   
    /**
     * get backends of cluster
     * @param clusterName
     * @return
     */ 
    public static List<List<String>> getClusterBackendInfos(String clusterName) {
        final SystemInfoService clusterInfoService = Catalog.getCurrentSystemInfo();
        List<List<String>> backendInfos = new LinkedList<List<String>>();
        List<Long> backendIds = null;
        if (!Strings.isNullOrEmpty(clusterName)) {
            final Cluster cluster = Catalog.getInstance().getCluster(clusterName);
            // root not in any cluster
            if (null == cluster) {
                return backendInfos;
            }
            backendIds = cluster.getBackendIdList();
        } else {
            backendIds = clusterInfoService.getBackendIds(false);
            if (backendIds == null) {
                return backendInfos;
            }
        }
        List<List<Comparable>> comparableBackendInfos = new LinkedList<List<Comparable>>();
        for (long backendId : backendIds) {
            Backend backend = clusterInfoService.getBackend(backendId);
            if (backend == null) {
                continue;
            }

            String ip = "N/A";
            String hostName = "N/A";
            try {
                InetAddress address = InetAddress.getByName(backend.getHost());
                hostName = address.getHostName();
            } catch (UnknownHostException e) {
                continue;
            }

            Integer tabletNum = Catalog.getCurrentInvertedIndex().getTabletNumByBackendId(backendId);
            List<Comparable> backendInfo = Lists.newArrayList();
            backendInfo.add(String.valueOf(backendId));
            backendInfo.add(backend.getOwnerClusterName());
            backendInfo.add(backend.getHost());
            if (Strings.isNullOrEmpty(clusterName)) {
                backendInfo.add(hostName);
                backendInfo.add(String.valueOf(backend.getHeartbeatPort()));
                backendInfo.add(String.valueOf(backend.getBePort()));
                backendInfo.add(String.valueOf(backend.getHttpPort()));
            }
            backendInfo.add(TimeUtils.longToTimeString(backend.getLastStartTime()));
            backendInfo.add(TimeUtils.longToTimeString(backend.getLastUpdateMs()));
            backendInfo.add(String.valueOf(backend.isAlive()));
            if (backend.isDecommissioned() && backend.getDecommissionType() == DecommissionType.ClusterDecommission) {
                backendInfo.add(String.valueOf("false"));
                backendInfo.add(String.valueOf("true"));
            } else if (backend.isDecommissioned()
                    && backend.getDecommissionType() == DecommissionType.SystemDecommission) {
                backendInfo.add(String.valueOf("true"));
                backendInfo.add(String.valueOf("false"));
            } else {
                backendInfo.add(String.valueOf("false"));
                backendInfo.add(String.valueOf("false"));
            }
            backendInfo.add(tabletNum.toString());

            double free = 0.0;
            if (backend.getTotalCapacityB() <= 0) {
                free = 0.0;
            } else {
                free = (double) backend.getAvailableCapacityB() * 100 / backend.getTotalCapacityB();
            }
            backendInfo.add(String.format("%.2f", free) + " %");

            comparableBackendInfos.add(backendInfo);
        }
         
        // sort by cluster name, host name
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(1, 3);
        Collections.sort(comparableBackendInfos, comparator);

        for (List<Comparable> backendInfo : comparableBackendInfos) {
            List<String> oneInfo = new ArrayList<String>(backendInfo.size());
            for (Comparable element : backendInfo) {
                oneInfo.add(element.toString());
            }
            backendInfos.add(oneInfo);
        }
        
        return backendInfos;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String beIdStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(beIdStr)) {
            throw new AnalysisException("Backend id is null");
        }

        long backendId = -1L;
        try {
            backendId = Long.valueOf(beIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid backend id format: " + beIdStr);
        }

        Backend backend = clusterInfoService.getBackend(backendId);
        if (backend == null) {
            throw new AnalysisException("Backend[" + backendId + "] does not exist.");
        }

        return new BackendProcNode(backend);
    }

}
