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
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/*
 * show proc "/cluster_balance/pending_tablets";
 * show proc "/cluster_balance/running_tablets";
 * show proc "/cluster_balance/history_tablets";
 */
public class TabletSchedulerDetailProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("TabletId")
            .add("Type").add("Medium").add("Status").add("State").add("SchedCode")
            .add("Priority").add("RealPriorityVal").add("SrcBe")
            .add("SrcPath").add("DestBe").add("DestPath").add("Timeout").add("Create").add("LstSched").add("LstVisit")
            .add("Finished").add("ReplicaSize").add("Rate").add("FailedSched").add("FailedRunning").add("VisibleVer")
            .add("CmtVer").add("ErrMsg")
            .build();

    private String type;
    private TabletScheduler tabletScheduler;

    public TabletSchedulerDetailProcDir(String type) {
        this.type = type;
        tabletScheduler = Env.getCurrentEnv().getTabletScheduler();
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        // get at most 1000 tablet infos
        List<List<String>> tabletInfos = Lists.newArrayList();
        if (type.equals(ClusterBalanceProcDir.PENDING_TABLETS)) {
            tabletInfos = tabletScheduler.getPendingTabletsInfo(1000);
        } else if (type.equals(ClusterBalanceProcDir.RUNNING_TABLETS)) {
            tabletInfos = tabletScheduler.getRunningTabletsInfo(1000);
        } else if (type.equals(ClusterBalanceProcDir.HISTORY_TABLETS)) {
            tabletInfos = tabletScheduler.getHistoryTabletsInfo(1000);
        } else {
            throw new AnalysisException("invalid type: " + type);
        }
        result.setRows(tabletInfos);
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tabletIdStr) throws AnalysisException {
        long tabletId = -1L;
        try {
            tabletId = Long.valueOf(tabletIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid tablet id format: " + tabletIdStr);
        }

        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
        return new ReplicasProcNode(tabletId, replicas);
    }
}
