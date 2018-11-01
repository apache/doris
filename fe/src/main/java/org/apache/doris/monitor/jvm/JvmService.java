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

package org.apache.doris.monitor.jvm;

import org.apache.doris.monitor.jvm.JvmStats.BufferPool;
import org.apache.doris.monitor.jvm.JvmStats.Classes;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollector;
import org.apache.doris.monitor.jvm.JvmStats.GarbageCollectors;
import org.apache.doris.monitor.jvm.JvmStats.Mem;
import org.apache.doris.monitor.jvm.JvmStats.Threads;

import com.google.common.base.Joiner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Obtain JVMInfo and JvmStats
 */
public class JvmService {
    private static final Logger LOG = LogManager.getLogger(JvmService.class);

    private final JvmInfo jvmInfo;

    private JvmStats jvmStats;

    public JvmService() {
        this.jvmInfo = JvmInfo.jvmInfo();
        this.jvmStats = JvmStats.jvmStats();
    }

    public JvmInfo info() {
        return this.jvmInfo;
    }

    public synchronized JvmStats stats() {
        jvmStats = JvmStats.jvmStats();
        return jvmStats;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        // 1. jvm stats
        JvmStats jvmStats = stats();
        
        // buffer pool
        List<BufferPool> bufferPools = jvmStats.getBufferPools();
        sb.append("JVM Stats: \nBuffer pools:\n");
        for (BufferPool bufferPool : bufferPools) {
            sb.append("\t").append(bufferPool.toString()).append("\n");
        }
        
        // classes
        Classes classes = jvmStats.getClasses();
        sb.append(classes.toString()).append("\n");
        
        // gc
        GarbageCollectors gc = jvmStats.getGc();
        GarbageCollector[] gcs = gc.getCollectors();
        sb.append("Garbage Collectors: \n");
        for (GarbageCollector garbageCollector : gcs) {
            sb.append("\t").append(garbageCollector.toString());
        }
        
        // mem
        Mem mem = jvmStats.getMem();
        sb.append("\nMem: ").append(mem.toString());

        // threads
        Threads threads = jvmStats.getThreads();
        sb.append("\nThreads: ").append(threads.toString());
        
        sb.append("\nUpTime: ").append(jvmStats.getUptime().toString());
        sb.append("\nTimestamp: ").append(jvmStats.getTimestamp());

        LOG.info(sb.toString());
        
        // 2. jvm info
        JvmInfo jvmInfo = info();
        
        sb.append("\nJVM Info: \nboot class path: ").append(jvmInfo.getBootClassPath());
        sb.append("\nclass path: ").append(jvmInfo.getClassPath());
        sb.append("\nconfigured init heap size: ").append(jvmInfo.getConfiguredInitialHeapSize());
        sb.append("\nconfigured max heap size: ").append(jvmInfo.getConfiguredMaxHeapSize());
        sb.append("\npid: ").append(jvmInfo.getPid());
        sb.append("\nstart time: ").append(jvmInfo.getStartTime());
        sb.append("\nversion: ").append(jvmInfo.getVersion());
        sb.append("\nvm name").append(jvmInfo.getVmName());
        sb.append("\nvm vendor: ").append(jvmInfo.getVmVendor());
        sb.append("\nvm version").append(jvmInfo.getVmVersion());
        
        sb.append("\ngcs: ").append(Joiner.on(", ").join(jvmInfo.getGcCollectors()));
        sb.append("\ninput arguments: ").append(Joiner.on(", ").join(jvmInfo.getInputArguments()));
        sb.append("\nmem: ").append(jvmInfo.getMem().toString());
        sb.append("\nmem pools: ").append(Joiner.on(", ").join(jvmInfo.getMemoryPools()));
        sb.append("\nsystem props: ").append(jvmInfo.getSystemProperties());
        
        return sb.toString();
    }
}
