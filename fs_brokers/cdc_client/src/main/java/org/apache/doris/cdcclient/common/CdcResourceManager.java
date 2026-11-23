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

package org.apache.doris.cdcclient.common;

import org.apache.doris.cdcclient.utils.ConfigUtil;

import java.util.EnumMap;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process-wide sink accounting and reader admission. */
public class CdcResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(CdcResourceManager.class);

    public enum ReaderGroup {
        JOB_SNAPSHOT,
        JOB_INCREMENTAL,
        STANDALONE_TVF
    }

    private final long sinkByteLimit;
    private final AtomicLong sinkBufferedBytes = new AtomicLong();
    private final long readerMemoryQuotaBytes;
    private final long readerReservationBytes;
    private final EnumMap<ReaderGroup, Long> reservedReaderBytesByGroup =
            new EnumMap<>(ReaderGroup.class);
    private long totalReservedReaderBytes;
    private int waitingIncrementalReaders;
    private final Set<ManagedReader<?>> closingReaders = ConcurrentHashMap.newKeySet();

    public CdcResourceManager() {
        this(Runtime.getRuntime().maxMemory(), System.getProperties());
    }

    CdcResourceManager(long heapBytes, Properties config) {
        String defaultRatio = Double.toString(1.0 / 3);
        double sinkRatio =
                Double.parseDouble(config.getProperty("cdc.sink.memory.ratio", defaultRatio));
        double readerRatio =
                Double.parseDouble(config.getProperty("cdc.reader.memory.ratio", defaultRatio));
        sinkByteLimit = (long) (heapBytes * sinkRatio);
        readerMemoryQuotaBytes = (long) (heapBytes * readerRatio);
        if (sinkByteLimit <= 0 || readerMemoryQuotaBytes <= 0 || !(sinkRatio + readerRatio < 1)) {
            throw new IllegalArgumentException(
                    "cdc.sink.memory.ratio ("
                            + sinkRatio
                            + ") and cdc.reader.memory.ratio ("
                            + readerRatio
                            + ") must each allow at least one byte and sum to less than 1");
        }
        readerReservationBytes = resolveReaderReservationBytes(config, heapBytes);
        for (ReaderGroup group : ReaderGroup.values()) {
            reservedReaderBytesByGroup.put(group, 0L);
        }
        LOG.info(
                "CDC memory allocation initialized: maxHeapBytes={}, sinkSoftLimitBytes={}, "
                        + "readerMemoryQuotaBytes={}, readerReservationBytes={}",
                heapBytes,
                sinkByteLimit,
                readerMemoryQuotaBytes,
                readerReservationBytes);
    }

    private static long resolveReaderReservationBytes(Properties config, long heapBytes) {
        long queueBytes = ConfigUtil.resolveMaxQueueSizeInBytes(config, heapBytes);
        if (queueBytes == 0) {
            queueBytes = ConfigUtil.defaultMaxQueueSizeInBytes(heapBytes);
        }
        // this estimate needs workload calibration; it is not a heap usage bound.
        double reservationBytes = Math.ceil(queueBytes * 1.5);
        if (reservationBytes >= Long.MAX_VALUE) {
            throw new ArithmeticException("CDC reader reservation bytes overflow");
        }
        return (long) reservationBytes;
    }

    public void addSinkBufferedBytes(long bytes) {
        sinkBufferedBytes.addAndGet(bytes);
    }

    public long getSinkBufferedBytes() {
        return sinkBufferedBytes.get();
    }

    public long getSinkByteLimit() {
        return sinkByteLimit;
    }

    public long getReaderMemoryQuotaBytes() {
        return readerMemoryQuotaBytes;
    }

    public long getReaderReservationBytes() {
        return readerReservationBytes;
    }

    public synchronized String getReaderWaitReason(ReaderGroup group) {
        return "Waiting for CDC reader memory quota: group="
                + group
                + ", requestedBytes="
                + readerReservationBytes
                + ", availableBytes="
                + getAvailableReaderBytes(group)
                + ", readerMemoryQuotaBytes="
                + readerMemoryQuotaBytes
                + ", incrementalReadersWaiting="
                + waitingIncrementalReaders;
    }

    public synchronized void setWaitingIncrementalReaders(int readers) {
        waitingIncrementalReaders = readers;
    }

    public void refreshClosingReaders() {
        closingReaders.forEach(ManagedReader::releaseIfTerminated);
    }

    public synchronized long getReaderBytes(ReaderGroup group) {
        return reservedReaderBytesByGroup.get(group);
    }

    /** Returns shared free quota, giving waiting incremental readers priority over other groups. */
    public synchronized long getAvailableReaderBytes(ReaderGroup group) {
        long availableBytes = readerMemoryQuotaBytes - totalReservedReaderBytes;
        if (group == ReaderGroup.JOB_INCREMENTAL || waitingIncrementalReaders == 0) {
            return availableBytes;
        }
        long availableReaders = availableBytes / readerReservationBytes;
        if (waitingIncrementalReaders > availableReaders) {
            return 0;
        }
        long incrementalPriorityBytes = waitingIncrementalReaders * readerReservationBytes;
        return availableBytes - incrementalPriorityBytes;
    }

    /** Returns null when another reader must release its reservation first. */
    public <R> ManagedReader<R> createIncrementalReader(
            String jobId,
            ReaderGroup group,
            Supplier<R> factory,
            Function<R, ExecutorService> closeReader) {
        refreshClosingReaders();
        if (reserveReaderSlots(group, 1) == 0) {
            return null;
        }
        ManagedReader<R> reader =
                openReader(
                        jobId,
                        group,
                        factory,
                        closeReader,
                        () -> releaseReader(group, readerReservationBytes));
        if (group == ReaderGroup.JOB_INCREMENTAL) {
            LOG.info(
                    "Registered CDC reader reservation, jobId={}, group={}, reservationBytes={}, "
                            + "groupReservedBytes={}, availableReaderBytes={}, readerMemoryQuotaBytes={}",
                    jobId,
                    group,
                    readerReservationBytes,
                    getReaderBytes(group),
                    getAvailableReaderBytes(group),
                    readerMemoryQuotaBytes);
        }
        return reader;
    }

    private synchronized int reserveReaderSlots(ReaderGroup group, int requestedReaders) {
        if (readerReservationBytes > readerMemoryQuotaBytes) {
            throw new IllegalArgumentException(
                    "CDC reader reservation exceeds memory quota: group="
                            + group
                            + ", requestedBytes="
                            + readerReservationBytes
                            + ", limitBytes="
                            + readerMemoryQuotaBytes
                            + "; check cdc.reader.memory.ratio and "
                            + ConfigUtil.MAX_QUEUE_BYTES_SYS_PROP);
        }
        long availableBytes = getAvailableReaderBytes(group);
        long availableReaders = availableBytes / readerReservationBytes;
        int admittedReaders = (int) Math.min(requestedReaders, availableReaders);
        long reservationBytes = admittedReaders * readerReservationBytes;

        reservedReaderBytesByGroup.put(
                group, reservedReaderBytesByGroup.get(group) + reservationBytes);
        totalReservedReaderBytes += reservationBytes;
        return admittedReaders;
    }

    private <R> ManagedReader<R> openReader(
            String jobId,
            ReaderGroup group,
            Supplier<R> factory,
            Function<R, ExecutorService> closeReader,
            Runnable releaseQuota) {
        boolean created = false;
        try {
            Objects.requireNonNull(closeReader, "reader closer");
            ManagedReader<R> managedReader =
                    new ManagedReader<>(
                            this,
                            jobId,
                            group,
                            Objects.requireNonNull(factory.get(), "created reader"),
                            closeReader,
                            releaseQuota);
            created = true;
            return managedReader;
        } finally {
            if (!created) {
                releaseQuota.run();
            }
        }
    }

    private synchronized void releaseReader(ReaderGroup group, long bytes) {
        reservedReaderBytesByGroup.put(group, reservedReaderBytesByGroup.get(group) - bytes);
        totalReservedReaderBytes -= bytes;
    }

    void retryClosingReaders() {
        for (ManagedReader<?> reader : closingReaders) {
            try {
                reader.close();
            } catch (RuntimeException e) {
                LOG.warn("Failed to close CDC reader", e);
            }
        }
    }

    boolean registerClosingReader(ManagedReader<?> reader) {
        return closingReaders.add(reader);
    }

    void unregisterClosingReader(ManagedReader<?> reader) {
        closingReaders.remove(reader);
    }

    public SnapshotQuota newSnapshotQuota(String jobId, ReaderGroup group) {
        return new SnapshotQuota(jobId, group);
    }

    /** A snapshot task keeps its admitted slots across split boundaries. */
    public final class SnapshotQuota implements AutoCloseable {
        private final String jobId;
        private final ReaderGroup group;
        private int availableSlots;
        private boolean closed;

        private SnapshotQuota(String jobId, ReaderGroup group) {
            this.jobId = jobId;
            this.group = group;
        }

        /** The scheduler stops calling this after the first successful reservation. */
        public synchronized int tryReserveSlots(int requestedReaders) {
            if (closed) {
                return 0;
            }
            availableSlots = reserveReaderSlots(group, requestedReaders);
            return availableSlots;
        }

        public <R> ManagedReader<R> createSnapshotReader(
                Supplier<R> factory, Function<R, ExecutorService> closer) {
            // A connector may have terminated since the previous poll.
            refreshClosingReaders();
            synchronized (this) {
                if (closed || availableSlots == 0) {
                    return null;
                }
                availableSlots--;
            }
            return openReader(jobId, group, factory, closer, this::returnSlot);
        }

        private synchronized void returnSlot() {
            if (closed) {
                releaseReader(group, readerReservationBytes);
            } else {
                availableSlots++;
            }
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                releaseReader(group, availableSlots * readerReservationBytes);
                availableSlots = 0;
            }
        }
    }
}
