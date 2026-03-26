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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.datasource.SplitSink;
import org.apache.doris.spi.Split;

import com.google.common.collect.Lists;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Local FE-side split producer that preserves the existing async Iceberg planning behavior.
 */
public class LocalParallelPlanningSplitProducer extends AbstractIcebergPlanningSplitProducer {
    public LocalParallelPlanningSplitProducer(IcebergSplitPlanningSupport planningSupport) {
        this(planningSupport, org.apache.doris.catalog.Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor());
    }

    LocalParallelPlanningSplitProducer(IcebergSplitPlanningSupport planningSupport, Executor executor) {
        super(planningSupport, executor);
    }

    @Override
    protected void doProduce(int numBackends, SplitSink splitSink) throws Exception {
        List<FileScanTask> customFileScanTasks = getPlanningSupport().getFileScanTasksFromContext();
        if (customFileScanTasks != null) {
            addSplitTasks(customFileScanTasks, splitSink);
            return;
        }

        TableScan scan = getPlanningSupport().createTableScan();
        try (CloseableIterable<FileScanTask> fileScanTasks = getPlanningSupport().planFileScanTask(scan)) {
            if (getPlanningSupport().hasTableLevelPushDownCount()) {
                addCountPushDownSplits(fileScanTasks, numBackends, splitSink);
                return;
            }
            addSplitTasks(fileScanTasks, splitSink);
        }
    }

    private void addSplitTasks(Iterable<FileScanTask> fileScanTasks, SplitSink splitSink) throws Exception {
        for (FileScanTask task : fileScanTasks) {
            if (!splitSink.needMore()) {
                return;
            }
            splitSink.addBatch(Lists.newArrayList(getPlanningSupport().createSplit(task)));
        }
    }

    private void addCountPushDownSplits(
            Iterable<FileScanTask> fileScanTasks,
            int numBackends,
            SplitSink splitSink) throws Exception {
        List<Split> splits = new ArrayList<>();
        int needSplitCnt = getPlanningSupport().getCountPushDownSplitCount(numBackends);
        for (FileScanTask task : fileScanTasks) {
            if (!splitSink.needMore()) {
                break;
            }
            splits.add(getPlanningSupport().createSplit(task));
            if (splits.size() >= needSplitCnt) {
                break;
            }
        }
        getPlanningSupport().assignCountToSplits(splits);
        if (!splits.isEmpty()) {
            splitSink.addBatch(splits);
        }
    }
}
