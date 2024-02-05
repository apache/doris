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

package org.apache.doris.hudi;


import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.util.WeakIdentityHashMap;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import scala.collection.Iterator;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * The hudi JniScanner
 */
public class HudiJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(HudiJniScanner.class);

    private final int fetchSize;
    private final String debugString;
    private final HoodieSplit split;
    private final ScanPredicate[] predicates;
    private final ClassLoader classLoader;
    private final UserGroupInformation ugi;

    private long getRecordReaderTimeNs = 0;
    private Iterator<InternalRow> recordIterator;

    /**
     * `GenericDatumReader` of avro is a thread local map, that stores `WeakIdentityHashMap`.
     * `WeakIdentityHashMap` has cached the avro resolving decoder, and the cached resolver can only be cleaned when
     * its avro schema is recycled and become a week reference. However, the behavior of the week reference queue
     * of `WeakIdentityHashMap` is unpredictable. Secondly, the decoder is very memory intensive, the number of threads
     * to call the thread local map cannot be too many.
     * Two solutions:
     * 1. Reduce the number of threads reading avro logs and keep the readers in a fixed thread pool.
     * 2. Regularly cleaning the cached resolvers in the thread local map by reflection.
     */
    private static final AtomicLong lastUpdateTime = new AtomicLong(System.currentTimeMillis());
    private static final long RESOLVER_TIME_OUT = 60000;
    private static final ExecutorService avroReadPool;
    private static ThreadLocal<WeakIdentityHashMap<?, ?>> AVRO_RESOLVER_CACHE;
    private static final Map<Long, WeakIdentityHashMap<?, ?>> cachedResolvers = new ConcurrentHashMap<>();
    private static final ReadWriteLock cleanResolverLock = new ReentrantReadWriteLock();
    private static final ScheduledExecutorService cleanResolverService = Executors.newScheduledThreadPool(1);

    static {
        int numThreads = Math.max(Runtime.getRuntime().availableProcessors() * 2, 4);
        if (numThreads > 48) {
            numThreads = Runtime.getRuntime().availableProcessors();
        }
        avroReadPool = Executors.newFixedThreadPool(numThreads,
                new ThreadFactoryBuilder().setNameFormat("avro-log-reader-%d").build());
        LOG.info("Create " + numThreads + " daemon threads to load avro logs");

        Class<?> avroReader = GenericDatumReader.class;
        try {
            Field field = avroReader.getDeclaredField("RESOLVER_CACHE");
            field.setAccessible(true);
            AVRO_RESOLVER_CACHE = (ThreadLocal<WeakIdentityHashMap<?, ?>>) field.get(null);
            LOG.info("Get the resolved cache for avro reader");
        } catch (Exception e) {
            AVRO_RESOLVER_CACHE = null;
            LOG.warn("Failed to get the resolved cache for avro reader");
        }

        cleanResolverService.scheduleAtFixedRate(() -> {
            cleanResolverLock.writeLock().lock();
            try {
                if (System.currentTimeMillis() - lastUpdateTime.get() > RESOLVER_TIME_OUT) {
                    for (WeakIdentityHashMap<?, ?> solver : cachedResolvers.values()) {
                        solver.clear();
                    }
                    lastUpdateTime.set(System.currentTimeMillis());
                }
            } finally {
                cleanResolverLock.writeLock().unlock();
            }
        }, RESOLVER_TIME_OUT, RESOLVER_TIME_OUT, TimeUnit.MILLISECONDS);
    }

    public HudiJniScanner(int fetchSize, Map<String, String> params) {
        debugString = params.entrySet().stream().map(kv -> kv.getKey() + "=" + kv.getValue())
                .collect(Collectors.joining("\n"));
        try {
            this.classLoader = this.getClass().getClassLoader();
            String predicatesAddressString = params.remove("push_down_predicates");
            this.fetchSize = fetchSize;
            this.split = new HoodieSplit(params);
            if (predicatesAddressString == null) {
                predicates = new ScanPredicate[0];
            } else {
                long predicatesAddress = Long.parseLong(predicatesAddressString);
                if (predicatesAddress != 0) {
                    predicates = ScanPredicate.parseScanPredicates(predicatesAddress, split.requiredTypes());
                    LOG.info("HudiJniScanner gets pushed-down predicates:  " + ScanPredicate.dump(predicates));
                } else {
                    predicates = new ScanPredicate[0];
                }
            }
            ugi = Utils.getUserGroupInformation(split.hadoopConf());
        } catch (Exception e) {
            LOG.error("Failed to initialize hudi scanner, split params:\n" + debugString, e);
            throw e;
        }
    }

    @Override
    public void open() throws IOException {
        Future<?> avroFuture = avroReadPool.submit(() -> {
            Thread.currentThread().setContextClassLoader(classLoader);
            initTableInfo(split.requiredTypes(), split.requiredFields(), predicates, fetchSize);
            long startTime = System.nanoTime();
            // RecordReader will use ProcessBuilder to start a hotspot process, which may be stuck,
            // so use another process to kill this stuck process.
            // TODO(gaoxin): better way to solve the stuck process?
            AtomicBoolean isKilled = new AtomicBoolean(false);
            ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
            executorService.scheduleAtFixedRate(() -> {
                if (!isKilled.get()) {
                    synchronized (HudiJniScanner.class) {
                        List<Long> pids = Utils.getChildProcessIds(
                                Utils.getCurrentProcId());
                        for (long pid : pids) {
                            String cmd = Utils.getCommandLine(pid);
                            if (cmd != null && cmd.contains("org.openjdk.jol.vm.sa.AttachMain")) {
                                Utils.killProcess(pid);
                                isKilled.set(true);
                                LOG.info("Kill hotspot debugger process " + pid);
                            }
                        }
                    }
                }
            }, 100, 1000, TimeUnit.MILLISECONDS);

            cleanResolverLock.readLock().lock();
            try {
                lastUpdateTime.set(System.currentTimeMillis());
                if (ugi != null) {
                    recordIterator = ugi.doAs(
                            (PrivilegedExceptionAction<Iterator<InternalRow>>) () -> new MORSnapshotSplitReader(
                                    split).buildScanIterator(new Filter[0]));
                } else {
                    recordIterator = new MORSnapshotSplitReader(split)
                            .buildScanIterator(new Filter[0]);
                }
                if (AVRO_RESOLVER_CACHE != null && AVRO_RESOLVER_CACHE.get() != null) {
                    cachedResolvers.computeIfAbsent(Thread.currentThread().getId(),
                            threadId -> AVRO_RESOLVER_CACHE.get());
                    AVRO_RESOLVER_CACHE.get().clear();
                }
            } catch (Exception e) {
                LOG.error("Failed to open hudi scanner, split params:\n" + debugString, e);
                throw new RuntimeException(e.getMessage(), e);
            } finally {
                cleanResolverLock.readLock().unlock();
            }
            isKilled.set(true);
            executorService.shutdownNow();
            getRecordReaderTimeNs += System.nanoTime() - startTime;
        });
        try {
            avroFuture.get();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (recordIterator instanceof Closeable) {
            ((Closeable) recordIterator).close();
        }
        recordIterator = null;
    }

    @Override
    public int getNext() throws IOException {
        try {
            int readRowNumbers = 0;
            HudiColumnValue columnValue = new HudiColumnValue();
            int numFields = split.requiredFields().length;
            ColumnType[] columnTypes = split.requiredTypes();
            while (readRowNumbers < fetchSize && recordIterator.hasNext()) {
                columnValue.reset(recordIterator.next());
                for (int i = 0; i < numFields; i++) {
                    columnValue.reset(i, columnTypes[i]);
                    appendData(i, columnValue);
                }
                readRowNumbers++;
            }
            return readRowNumbers;
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next batch of hudi, split params:\n" + debugString, e);
            throw new IOException("Failed to get the next batch of hudi.", e);
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        return Collections.singletonMap("timer:GetRecordReaderTime", String.valueOf(getRecordReaderTimeNs));
    }
}
