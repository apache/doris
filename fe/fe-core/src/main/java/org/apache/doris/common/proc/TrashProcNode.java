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

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TDiskTrashInfo;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class TrashProcNode implements ProcNodeInterface {
    private static final Logger LOG = LogManager.getLogger(TrashProcNode.class);

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("RootPath")
            .add("State").add("TrashUsedCapacity").build();

    private Backend backend;

    public TrashProcNode(Backend backend) {
        this.backend = backend;
    }

    @Override
    public ProcResult fetchResult() {
        Preconditions.checkNotNull(backend);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<String>> infos = Lists.newArrayList();

        getTrashDiskInfo(backend, infos);

        for (List<String> info : infos) {
            result.addRow(info);
        }

        return result;
    }

    public static void getTrashDiskInfo(Backend backend, List<List<String>> infos) {

        BackendService.Client client = null;
        TNetworkAddress address = null;
        boolean ok = false;
        List<TDiskTrashInfo> diskTrashInfos = null;
        try {
            address = new TNetworkAddress(backend.getHost(), backend.getBePort());
            client = ClientPool.backendPool.borrowObject(address);
            diskTrashInfos = client.getDiskTrashUsedCapacity();
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

        if (diskTrashInfos == null) {
            return;
        }
        for (TDiskTrashInfo diskTrashInfo : diskTrashInfos) {
            List<String> diskInfo = new ArrayList<String>();

            diskInfo.add(diskTrashInfo.getRootPath());

            diskInfo.add(diskTrashInfo.getState());

            long trashUsedCapacityB = diskTrashInfo.getTrashUsedCapacity();
            Pair<Double, String> trashUsedCapacity = DebugUtil.getByteUint(trashUsedCapacityB);
            diskInfo.add(
                    DebugUtil.DECIMAL_FORMAT_SCALE_3.format(trashUsedCapacity.first) + " " + trashUsedCapacity.second);

            infos.add(diskInfo);
        }
    }
}
