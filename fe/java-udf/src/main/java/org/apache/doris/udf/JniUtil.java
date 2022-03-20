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

package org.apache.doris.udf;

import com.google.common.base.Joiner;

import org.apache.doris.thrift.TJvmMemoryPool;
import org.apache.doris.thrift.TGetJvmMemoryMetricsResponse;
import org.apache.doris.thrift.TJvmThreadInfo;
import org.apache.doris.thrift.TGetJvmThreadsInfoRequest;
import org.apache.doris.thrift.TGetJvmThreadsInfoResponse;
import org.apache.doris.thrift.TGetJMXJsonResponse;
import org.apache.doris.monitor.jvm.JvmPauseMonitor;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Map;

/**
 * Utility class with methods intended for JNI clients
 */
public class JniUtil {
    private final static TBinaryProtocol.Factory protocolFactory_ =
            new TBinaryProtocol.Factory();

    /**
     * Initializes the JvmPauseMonitor instance.
     */
    public static void initPauseMonitor(long deadlockCheckIntervalS) {
        JvmPauseMonitor.INSTANCE.initPauseMonitor(deadlockCheckIntervalS);
    }

    /**
     * Returns a formatted string containing the simple exception name and the
     * exception message without the full stack trace. Includes the
     * the chain of causes each in a separate line.
     */
    public static String throwableToString(Throwable t) {
        StringWriter output = new StringWriter();
        output.write(String.format("%s: %s", t.getClass().getSimpleName(),
                t.getMessage()));
        // Follow the chain of exception causes and print them as well.
        Throwable cause = t;
        while ((cause = cause.getCause()) != null) {
            output.write(String.format("\nCAUSED BY: %s: %s",
                    cause.getClass().getSimpleName(), cause.getMessage()));
        }
        return output.toString();
    }

    /**
     * Returns the stack trace of the Throwable object.
     */
    public static String throwableToStackTrace(Throwable t) {
        Writer output = new StringWriter();
        t.printStackTrace(new PrintWriter(output));
        return output.toString();
    }

    /**
     * Serializes input into a byte[] using the default protocol factory.
     */
    public static <T extends TBase<?, ?>>
    byte[] serializeToThrift(T input) throws InternalException {
        TSerializer serializer = new TSerializer(protocolFactory_);
        try {
            return serializer.serialize(input);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
    }

    /**
     * Serializes input into a byte[] using a given protocol factory.
     */
    public static <T extends TBase<?, ?>, F extends TProtocolFactory>
    byte[] serializeToThrift(T input, F protocolFactory) throws InternalException {
        TSerializer serializer = new TSerializer(protocolFactory);
        try {
            return serializer.serialize(input);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
    }

    public static <T extends TBase<?, ?>>
    void deserializeThrift(T result, byte[] thriftData) throws InternalException {
        deserializeThrift(protocolFactory_, result, thriftData);
    }

    /**
     * Deserialize a serialized form of a Thrift data structure to its object form.
     */
    public static <T extends TBase<?, ?>, F extends TProtocolFactory>
    void deserializeThrift(F protocolFactory, T result, byte[] thriftData)
            throws InternalException {
        // TODO: avoid creating deserializer for each query?
        TDeserializer deserializer = new TDeserializer(protocolFactory);
        try {
            deserializer.deserialize(result, thriftData);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
    }

    /**
     * Collect the JVM's memory statistics into a thrift structure for translation into
     * Doris metrics by the backend. A synthetic 'total' memory pool is included with
     * aggregate statistics for all real pools. Metrics for the JvmPauseMonitor
     * and Garbage Collection are also included.
     */
    public static byte[] getJvmMemoryMetrics() throws InternalException {
        TGetJvmMemoryMetricsResponse jvmMetrics = new TGetJvmMemoryMetricsResponse();
        jvmMetrics.setMemoryPools(new ArrayList<TJvmMemoryPool>());
        TJvmMemoryPool totalUsage = new TJvmMemoryPool();

        totalUsage.setName("total");
        jvmMetrics.getMemoryPools().add(totalUsage);

        for (MemoryPoolMXBean memBean : ManagementFactory.getMemoryPoolMXBeans()) {
            TJvmMemoryPool usage = new TJvmMemoryPool();
            MemoryUsage beanUsage = memBean.getUsage();
            usage.setCommitted(beanUsage.getCommitted());
            usage.setInit(beanUsage.getInit());
            usage.setMax(beanUsage.getMax());
            usage.setUsed(beanUsage.getUsed());
            usage.setName(memBean.getName());

            totalUsage.committed += beanUsage.getCommitted();
            totalUsage.init += beanUsage.getInit();
            totalUsage.max += beanUsage.getMax();
            totalUsage.used += beanUsage.getUsed();

            MemoryUsage peakUsage = memBean.getPeakUsage();
            usage.setPeakCommitted(peakUsage.getCommitted());
            usage.setPeakInit(peakUsage.getInit());
            usage.setPeakMax(peakUsage.getMax());
            usage.setPeakUsed(peakUsage.getUsed());

            totalUsage.peak_committed += peakUsage.getCommitted();
            totalUsage.peak_init += peakUsage.getInit();
            totalUsage.peak_max += peakUsage.getMax();
            totalUsage.peak_used += peakUsage.getUsed();

            jvmMetrics.getMemoryPools().add(usage);
        }

        // Populate heap usage
        MemoryMXBean mBean = ManagementFactory.getMemoryMXBean();
        TJvmMemoryPool heap = new TJvmMemoryPool();
        MemoryUsage heapUsage = mBean.getHeapMemoryUsage();
        heap.setCommitted(heapUsage.getCommitted());
        heap.setInit(heapUsage.getInit());
        heap.setMax(heapUsage.getMax());
        heap.setUsed(heapUsage.getUsed());
        heap.setName("heap");
        heap.setPeakCommitted(0);
        heap.setPeakInit(0);
        heap.setPeakMax(0);
        heap.setPeakUsed(0);
        jvmMetrics.getMemoryPools().add(heap);

        // Populate non-heap usage
        TJvmMemoryPool nonHeap = new TJvmMemoryPool();
        MemoryUsage nonHeapUsage = mBean.getNonHeapMemoryUsage();
        nonHeap.setCommitted(nonHeapUsage.getCommitted());
        nonHeap.setInit(nonHeapUsage.getInit());
        nonHeap.setMax(nonHeapUsage.getMax());
        nonHeap.setUsed(nonHeapUsage.getUsed());
        nonHeap.setName("non-heap");
        nonHeap.setPeakCommitted(0);
        nonHeap.setPeakInit(0);
        nonHeap.setPeakMax(0);
        nonHeap.setPeakUsed(0);
        jvmMetrics.getMemoryPools().add(nonHeap);

        // Populate JvmPauseMonitor metrics
        jvmMetrics.setGcNumWarnThresholdExceeded(
                JvmPauseMonitor.INSTANCE.getNumGcWarnThresholdExceeded());
        jvmMetrics.setGcNumInfoThresholdExceeded(
                JvmPauseMonitor.INSTANCE.getNumGcInfoThresholdExceeded());
        jvmMetrics.setGcTotalExtraSleepTimeMillis(
                JvmPauseMonitor.INSTANCE.getTotalGcExtraSleepTime());

        // And Garbage Collector metrics
        long gcCount = 0;
        long gcTimeMillis = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcCount += bean.getCollectionCount();
            gcTimeMillis += bean.getCollectionTime();
        }
        jvmMetrics.setGcCount(gcCount);
        jvmMetrics.setGcTimeMillis(gcTimeMillis);

        return serializeToThrift(jvmMetrics, protocolFactory_);
    }

    /**
     * Get information about the live JVM threads.
     */
    public static byte[] getJvmThreadsInfo(byte[] argument) throws InternalException {
        TGetJvmThreadsInfoRequest request = new TGetJvmThreadsInfoRequest();
        JniUtil.deserializeThrift(protocolFactory_, request, argument);
        TGetJvmThreadsInfoResponse response = new TGetJvmThreadsInfoResponse();
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        response.setTotalThreadCount(threadBean.getThreadCount());
        response.setDaemonThreadCount(threadBean.getDaemonThreadCount());
        response.setPeakThreadCount(threadBean.getPeakThreadCount());
        if (request.get_complete_info) {
            for (ThreadInfo threadInfo : threadBean.dumpAllThreads(true, true)) {
                TJvmThreadInfo tThreadInfo = new TJvmThreadInfo();
                long id = threadInfo.getThreadId();
                tThreadInfo.setSummary(threadInfo.toString());
                tThreadInfo.setCpuTimeInNs(threadBean.getThreadCpuTime(id));
                tThreadInfo.setUserTimeInNs(threadBean.getThreadUserTime(id));
                tThreadInfo.setBlockedCount(threadInfo.getBlockedCount());
                tThreadInfo.setBlockedTimeInMs(threadInfo.getBlockedTime());
                tThreadInfo.setIsInNative(threadInfo.isInNative());
                response.addToThreads(tThreadInfo);
            }
        }
        return serializeToThrift(response, protocolFactory_);
    }

    public static byte[] getJMXJson() throws InternalException {
        TGetJMXJsonResponse response = new TGetJMXJsonResponse(JMXJsonUtil.getJMXJson());
        return serializeToThrift(response, protocolFactory_);
    }

    /**
     * Get Java version, input arguments and system properties.
     */
    public static String getJavaVersion() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        StringBuilder sb = new StringBuilder();
        sb.append("Java Input arguments:\n");
        sb.append(Joiner.on(" ").join(runtime.getInputArguments()));
        sb.append("\nJava System properties:\n");
        for (Map.Entry<String, String> entry : runtime.getSystemProperties().entrySet()) {
            sb.append(entry.getKey() + ":" + entry.getValue() + "\n");
        }
        return sb.toString();
    }
}
