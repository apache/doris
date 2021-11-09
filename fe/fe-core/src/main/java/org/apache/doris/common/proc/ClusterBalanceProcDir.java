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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.clone.TabletChecker;
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/*
 * show proc "/cluster_balance";
 */
public class ClusterBalanceProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Item").add("Number").build();

    public static final String CLUSTER_LOAD = "cluster_load_stat";
    public static final String WORKING_SLOTS = "working_slots";
    public static final String SCHED_STAT = "sched_stat";

    public static final String PRIORITY_REPAIR = "priority_repair";
    public static final String PENDING_TABLETS = "pending_tablets";
    public static final String RUNNING_TABLETS = "running_tablets";
    public static final String HISTORY_TABLETS = "history_tablets";

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (name.equals(CLUSTER_LOAD)) {
            return new ClusterLoadStatByTag();
        } else if (name.equals(WORKING_SLOTS)) {
            return new SchedulerWorkingSlotsProcDir();
        } else if (name.equals(SCHED_STAT)) {
            return new SchedulerStatProcNode();
        } else if (name.equals(PRIORITY_REPAIR)) {
            return new PriorityRepairProcNode();
        } else {
            return new TabletSchedulerDetailProcDir(name);
        }
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        TabletScheduler tabletScheduler = Catalog.getCurrentCatalog().getTabletScheduler();
        TabletChecker tabletChecker = Catalog.getCurrentCatalog().getTabletChecker();
        result.addRow(Lists.newArrayList(CLUSTER_LOAD, String.valueOf(tabletScheduler.getStatisticMap().size())));
        result.addRow(Lists.newArrayList(WORKING_SLOTS,
                                         String.valueOf(tabletScheduler.getBackendsWorkingSlots().size())));
        result.addRow(Lists.newArrayList(SCHED_STAT, tabletScheduler.getStat().getLastSnapshot() == null ? "0" : "1"));

        result.addRow(Lists.newArrayList(PRIORITY_REPAIR, String.valueOf(tabletChecker.getPrioPartitionNum())));
        result.addRow(Lists.newArrayList(PENDING_TABLETS, String.valueOf(tabletScheduler.getPendingNum())));
        result.addRow(Lists.newArrayList(RUNNING_TABLETS, String.valueOf(tabletScheduler.getRunningNum())));
        result.addRow(Lists.newArrayList(HISTORY_TABLETS, String.valueOf(tabletScheduler.getHistoryNum())));
        return result;
    }
}
