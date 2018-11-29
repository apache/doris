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
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/*
 * show proc "/tablet_scheduler";
 */
public class TabletSchedulerProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Item").add("Number").build();

    public static final String CLUSTER_LOAD = "cluster_load_statistics";
    public static final String WORKING_SLOTS = "working_slots";
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
            return new ClusterLoadStatisticProcDir();
        } else if (name.equals(WORKING_SLOTS)) {
            return new SchedulerWorkingSlotsProcDir();
        } else {
            return new TabletSchedulerDetailProcDir(name);
        }
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.addRow(Lists.newArrayList(CLUSTER_LOAD, "0"));
        result.addRow(Lists.newArrayList(WORKING_SLOTS, "0"));

        TabletScheduler tabletScheduler = Catalog.getCurrentCatalog().getTabletScheduler();

        result.addRow(Lists.newArrayList(PENDING_TABLETS, String.valueOf(tabletScheduler.getPendingNum())));
        result.addRow(Lists.newArrayList(RUNNING_TABLETS, String.valueOf(tabletScheduler.getRunningNum())));
        result.addRow(Lists.newArrayList(HISTORY_TABLETS, String.valueOf(tabletScheduler.getHistoryNum())));
        return result;
    }
}
