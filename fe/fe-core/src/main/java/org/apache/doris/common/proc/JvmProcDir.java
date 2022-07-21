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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.monitor.jvm.JvmInfo;
import org.apache.doris.monitor.jvm.JvmService;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.monitor.jvm.JvmStats.BufferPool;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollector;
import org.apache.doris.monitor.jvm.JvmStats.MemoryPool;
import org.apache.doris.monitor.jvm.JvmStats.Threads;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

public class JvmProcDir implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("Value")
            .build();

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        JvmService jvmService = new JvmService();

        // 1. jvm info
        JvmInfo jvmInfo = jvmService.info();
        result.addRow(genRow("jvm start time", TimeUtils.longToTimeString(jvmInfo.getStartTime())));
        result.addRow(genRow("jvm version info", Joiner.on(" ").join(jvmInfo.getVersion(),
                jvmInfo.getVmName(), jvmInfo.getVmVendor(), jvmInfo.getVmVersion())));

        result.addRow(genRow("configured init heap size",
                DebugUtil.printByteWithUnit(jvmInfo.getConfiguredInitialHeapSize())));
        result.addRow(genRow("configured max heap size",
                DebugUtil.printByteWithUnit(jvmInfo.getConfiguredMaxHeapSize())));
        result.addRow(genRow("frontend pid", jvmInfo.getPid()));

        // 2. jvm stats
        JvmStats jvmStats = jvmService.stats();
        result.addRow(genRow("classes loaded", jvmStats.getClasses().getLoadedClassCount()));
        result.addRow(genRow("classes total loaded", jvmStats.getClasses().getTotalLoadedClassCount()));
        result.addRow(genRow("classes unloaded", jvmStats.getClasses().getUnloadedClassCount()));

        result.addRow(genRow("mem heap committed",
                DebugUtil.printByteWithUnit(jvmStats.getMem().getHeapCommitted().getBytes())));
        result.addRow(genRow("mem heap used",
                DebugUtil.printByteWithUnit(jvmStats.getMem().getHeapUsed().getBytes())));
        result.addRow(genRow("mem non heap committed",
                DebugUtil.printByteWithUnit(jvmStats.getMem().getNonHeapCommitted().getBytes())));
        result.addRow(genRow("mem non heap used",
                DebugUtil.printByteWithUnit(jvmStats.getMem().getNonHeapUsed().getBytes())));

        Iterator<MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            MemoryPool memPool = memIter.next();
            result.addRow(genRow("mem pool " + memPool.getName() + " used",
                    DebugUtil.printByteWithUnit(memPool.getUsed().getBytes())));
            result.addRow(genRow("mem pool " + memPool.getName() + " max",
                    DebugUtil.printByteWithUnit(memPool.getMax().getBytes())));
            result.addRow(genRow("mem pool " + memPool.getName() + " peak used",
                    DebugUtil.printByteWithUnit(memPool.getPeakUsed().getBytes())));
            result.addRow(genRow("mem pool " + memPool.getName() + " peak max",
                    DebugUtil.printByteWithUnit(memPool.getPeakMax().getBytes())));
        }

        for (BufferPool bp : jvmStats.getBufferPools()) {
            result.addRow(genRow("buffer pool " + bp.getName() + " count", bp.getCount()));
            result.addRow(genRow("buffer pool " + bp.getName() + " used",
                    DebugUtil.printByteWithUnit(bp.getUsed().getBytes())));
            result.addRow(genRow("buffer pool " + bp.getName() + " capacity",
                    DebugUtil.printByteWithUnit(bp.getTotalCapacity().getBytes())));
        }

        Iterator<GarbageCollector> gcIter = jvmStats.getGc().iterator();
        while (gcIter.hasNext()) {
            GarbageCollector gc = gcIter.next();
            result.addRow(genRow("gc " + gc.getName() + " collection count", gc.getCollectionCount()));
            result.addRow(genRow("gc " + gc.getName() + " collection time", gc.getCollectionTime().getMillis()));
        }

        Threads threads = jvmStats.getThreads();
        result.addRow(genRow("threads count", threads.getCount()));
        result.addRow(genRow("threads peak count", threads.getPeakCount()));
        result.addRow(genRow("threads new count", threads.getThreadsNewCount()));
        result.addRow(genRow("threads runnable count", threads.getThreadsRunnableCount()));
        result.addRow(genRow("threads blocked count", threads.getThreadsBlockedCount()));
        result.addRow(genRow("threads waiting count", threads.getThreadsWaitingCount()));
        result.addRow(genRow("threads timed_waiting count", threads.getThreadsTimedWaitingCount()));
        result.addRow(genRow("threads terminated count", threads.getThreadsTerminatedCount()));

        return result;
    }

    private List<String> genRow(String key, Object value) {
        List<String> row = Lists.newArrayList();
        row.add(key);
        row.add(String.valueOf(value));
        return row;
    }
}
