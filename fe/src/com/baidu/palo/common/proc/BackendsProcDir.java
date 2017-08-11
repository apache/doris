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

import com.baidu.palo.alter.DecommissionBackendJob.DecomissionType;
import com.baidu.palo.catalog.Catalog;
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
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("Cluster")
            .add("BackendId").add("IP").add("HostName").add("HeartbeatPort").add("BePort").add("HttpPort")
            .add("LastStartTime").add("LastHeartbeat").add("Alive").add("SystemDecommissioned")
            .add("ClusterDecommissioned").add("TabletNum").build();

    public static final int IP_INDEX = 1;
    public static final int HOSTNAME_INDEX = 2;

    private SystemInfoService clusterInfoService;

    public BackendsProcDir(SystemInfoService clusterInfoService) {
        this.clusterInfoService = clusterInfoService;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(clusterInfoService);

        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);
        List<Long> backendIds = clusterInfoService.getBackendIds(false);
        if (backendIds == null) {
            // empty
            return result;
        }

        List<List<Comparable>> backendInfos = new LinkedList<List<Comparable>>();
        for (long backendId : backendIds) {
            Backend backend = clusterInfoService.getBackend(backendId);
            if (backend == null) {
                continue;
            }

            String ip = "N/A";
            String hostName = "N/A";
            try {
                InetAddress address = InetAddress.getByName(backend.getHost());
                ip = address.getHostAddress();
                hostName = address.getHostName();
            } catch (UnknownHostException e) {
                continue;
            }

            Integer tabletNum = Catalog.getCurrentInvertedIndex().getTabletNumByBackendId(backendId);
            List<Comparable> backendInfo = Lists.newArrayList();
            backendInfo.add(backend.getOwnerClusterName());
            backendInfo.add(String.valueOf(backendId));
            backendInfo.add(ip);
            backendInfo.add(hostName);
            backendInfo.add(String.valueOf(backend.getHeartbeatPort()));
            backendInfo.add(String.valueOf(backend.getBePort()));
            backendInfo.add(String.valueOf(backend.getHttpPort()));
            backendInfo.add(TimeUtils.longToTimeString(backend.getLastStartTime()));
            backendInfo.add(TimeUtils.longToTimeString(backend.getLastUpdateMs()));
            backendInfo.add(String.valueOf(backend.isAlive()));
            if (backend.isDecommissioned() && backend.getDecommissionType() == DecomissionType.ClusterDecomission) {
                backendInfo.add(String.valueOf("false"));
                backendInfo.add(String.valueOf("true"));
            } else if (backend.isDecommissioned()
                    && backend.getDecommissionType() == DecomissionType.SystemDecomission) {
                backendInfo.add(String.valueOf("true"));
                backendInfo.add(String.valueOf("false"));
            } else {
                backendInfo.add(String.valueOf("false"));
                backendInfo.add(String.valueOf("false"));
            }
            backendInfo.add(tabletNum.toString());

            backendInfos.add(backendInfo);
        }

        // sort by id, ip hostName
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2);
        Collections.sort(backendInfos, comparator);

        for (List<Comparable> backendInfo : backendInfos) {
            List<String> oneInfo = new ArrayList<String>(backendInfo.size());
            for (Comparable element : backendInfo) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name)) {
            throw new AnalysisException("Backend id is null");
        }

        long backendId = -1L;
        try {
            backendId = Long.valueOf(name);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid backend id format: " + name);
        }

        Backend backend = clusterInfoService.getBackend(backendId);
        if (backend == null) {
            throw new AnalysisException("Backend[" + backendId + "] does not exist.");
        }

        return new BackendProcNode(backend);
    }

}
