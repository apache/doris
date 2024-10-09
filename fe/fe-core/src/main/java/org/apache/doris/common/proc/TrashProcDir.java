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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/*
 * Show trash
 * SHOW PROC '/trash'
 * SHOW PROC '/trash/backendId'
 */
public class TrashProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(TrashProcNode.class);

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("BackendId")
            .add("Backend").add("TrashUsedCapacity").build();

    private List<Backend> backends = Lists.newArrayList();

    public TrashProcDir() {
        ImmutableMap<Long, Backend> backendsInfo;
        try {
            backendsInfo = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        } catch (AnalysisException e) {
            LOG.warn("Can't get backends info", e);
            return;
        }
        for (Backend backend : backendsInfo.values()) {
            this.backends.add(backend);
        }
    }

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<String>> infos = Lists.newArrayList();

        getTrashInfo(backends, infos);

        for (List<String> info : infos) {
            result.addRow(info);
        }

        return result;
    }

    public static void getTrashInfo(List<Backend> backends, List<List<String>> infos) {
        for (Backend backend : backends) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            Long trashUsedCapacityB = null;
            boolean ok = false;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                trashUsedCapacityB = client.getTrashUsedCapacity();
                ok = true;
            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }

            List<String> backendInfo = new ArrayList<>();
            backendInfo.add(String.valueOf(backend.getId()));
            backendInfo.add(NetUtils.getHostPortInAccessibleFormat(backend.getHost(), backend.getHeartbeatPort()));
            if (trashUsedCapacityB != null) {
                Pair<Double, String> trashUsedCapacity = DebugUtil.getByteUint(trashUsedCapacityB);
                backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(trashUsedCapacity.first) + " "
                        + trashUsedCapacity.second);
            } else {
                backendInfo.add("");
            }
            infos.add(backendInfo);
        }
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String backendIdStr) throws AnalysisException {
        long backendId = -1;
        try {
            backendId = Long.parseLong(backendIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid backend id format: " + backendIdStr);
        }
        Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            throw new AnalysisException("Backend[" + backendId + "] does not exist.");
        }
        return new TrashProcNode(backend);
    }
}
