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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BackendsProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(BackendsProcDir.class);

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("BackendId")
            .add("Host").add("HeartbeatPort").add("BePort").add("HttpPort").add("BrpcPort").add("ArrowFlightSqlPort")
            .add("LastStartTime").add("LastHeartbeat").add("Alive").add("SystemDecommissioned").add("TabletNum")
            .add("DataUsedCapacity").add("TrashUsedCapcacity").add("AvailCapacity").add("TotalCapacity").add("UsedPct")
            .add("MaxDiskUsedPct").add("RemoteUsedCapacity").add("Tag").add("ErrMsg").add("Version").add("Status")
            .add("HeartbeatFailureCounter").add("NodeRole")
            .build();

    public static final int HOSTNAME_INDEX = 3;

    private SystemInfoService systemInfoService;

    public BackendsProcDir(SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(systemInfoService);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        final List<List<String>> backendInfos = getBackendInfos();
        for (List<String> backendInfo : backendInfos) {
            List<String> oneInfo = new ArrayList<>(backendInfo.size());
            oneInfo.addAll(backendInfo);
            result.addRow(oneInfo);
        }
        return result;
    }

    /**
     * get backends info
     *
     * @return
     */
    public static List<List<String>> getBackendInfos() {
        final SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        List<List<String>> backendInfos = new LinkedList<>();
        List<Long> backendIds = systemInfoService.getAllBackendIds(false);
        if (backendIds == null) {
            return backendInfos;
        }

        long start = System.currentTimeMillis();
        Stopwatch watch = Stopwatch.createUnstarted();
        List<List<Comparable>> comparableBackendInfos = new LinkedList<>();
        for (long backendId : backendIds) {
            Backend backend = systemInfoService.getBackend(backendId);
            if (backend == null) {
                continue;
            }

            watch.start();
            Integer tabletNum = Env.getCurrentInvertedIndex().getTabletNumByBackendId(backendId);
            watch.stop();
            List<Comparable> backendInfo = Lists.newArrayList();
            backendInfo.add(String.valueOf(backendId));
            backendInfo.add(backend.getHost());
            backendInfo.add(String.valueOf(backend.getHeartbeatPort()));
            backendInfo.add(String.valueOf(backend.getBePort()));
            backendInfo.add(String.valueOf(backend.getHttpPort()));
            backendInfo.add(String.valueOf(backend.getBrpcPort()));
            backendInfo.add(String.valueOf(backend.getArrowFlightSqlPort()));
            backendInfo.add(TimeUtils.longToTimeString(backend.getLastStartTime()));
            backendInfo.add(TimeUtils.longToTimeString(backend.getLastUpdateMs()));
            backendInfo.add(String.valueOf(backend.isAlive()));
            backendInfo.add(String.valueOf(backend.isDecommissioned()));
            backendInfo.add(tabletNum.toString());

            // capacity
            // data used
            long dataUsedB = backend.getDataUsedCapacityB();
            Pair<Double, String> usedCapacity = DebugUtil.getByteUint(dataUsedB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(usedCapacity.first) + " " + usedCapacity.second);
            // trash used
            long trashUsedB = backend.getTrashUsedCapacityB();
            Pair<Double, String> trashUsedCapacity = DebugUtil.getByteUint(trashUsedB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(
                        trashUsedCapacity.first) + " " + trashUsedCapacity.second);
            // available
            long availB = backend.getAvailableCapacityB();
            Pair<Double, String> availCapacity = DebugUtil.getByteUint(availB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(availCapacity.first) + " " + availCapacity.second);
            // total
            long totalB = backend.getTotalCapacityB();
            Pair<Double, String> totalCapacity = DebugUtil.getByteUint(totalB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalCapacity.first) + " " + totalCapacity.second);

            // used percent
            double used = 0.0;
            if (totalB <= 0) {
                used = 0.0;
            } else {
                used = (double) (totalB - availB) * 100 / totalB;
            }
            backendInfo.add(String.format("%.2f", used) + " %");
            backendInfo.add(String.format("%.2f", backend.getMaxDiskUsedPct() * 100) + " %");

            // remote used capacity
            long remoteUsedB = backend.getRemoteUsedCapacityB();
            Pair<Double, String> totalRemoteUsedCapacity = DebugUtil.getByteUint(remoteUsedB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalRemoteUsedCapacity.first) + " "
                    + totalRemoteUsedCapacity.second);

            // tags
            backendInfo.add(backend.getTagMapString());
            // err msg
            backendInfo.add(backend.getHeartbeatErrMsg());
            // version
            backendInfo.add(backend.getVersion());
            // status
            backendInfo.add(new Gson().toJson(backend.getBackendStatus()));
            // heartbeat failure counter
            backendInfo.add(backend.getHeartbeatFailureCounter());

            // node role, show the value only when backend is alive.
            backendInfo.add(backend.isAlive() ? backend.getNodeRoleTag().value : "");

            comparableBackendInfos.add(backendInfo);
        }

        // backends proc node get result too slow, add log to observer.
        LOG.debug("backends proc get tablet num cost: {}, total cost: {}", watch.elapsed(TimeUnit.MILLISECONDS),
                (System.currentTimeMillis() - start));

        // sort by host name
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(1);
        comparableBackendInfos.sort(comparator);

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
            backendId = Long.parseLong(beIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid backend id format: " + beIdStr);
        }

        Backend backend = systemInfoService.getBackend(backendId);
        if (backend == null) {
            throw new AnalysisException("Backend[" + backendId + "] does not exist.");
        }

        return new BackendProcNode(backend);
    }

}
