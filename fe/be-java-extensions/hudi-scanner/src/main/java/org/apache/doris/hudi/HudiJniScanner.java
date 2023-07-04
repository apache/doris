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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import scala.collection.Iterator;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The hudi JniScanner
 */
public class HudiJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(HudiJniScanner.class);

    private final int fetchSize;
    private final HoodieSplit split;
    private final ScanPredicate[] predicates;
    private final ClassLoader classLoader;
    private final UserGroupInformation ugi;

    private long getRecordReaderTimeNs = 0;
    private Iterator<InternalRow> recordIterator;

    public HudiJniScanner(int fetchSize, Map<String, String> params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Hudi JNI params:\n" + params.entrySet().stream().map(kv -> kv.getKey() + "=" + kv.getValue())
                    .collect(Collectors.joining("\n")));
        }
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
    }

    @Override
    public void open() throws IOException {
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
        if (ugi != null) {
            try {
                recordIterator = ugi.doAs(
                        (PrivilegedExceptionAction<Iterator<InternalRow>>) () -> new MORSnapshotSplitReader(
                                split).buildScanIterator(split.requiredFields(), new Filter[0]));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        } else {
            recordIterator = new MORSnapshotSplitReader(split)
                    .buildScanIterator(split.requiredFields(), new Filter[0]);
        }
        isKilled.set(true);
        executorService.shutdownNow();
        getRecordReaderTimeNs += System.nanoTime() - startTime;
    }

    @Override
    public void close() throws IOException {
        if (recordIterator instanceof Closeable) {
            ((Closeable) recordIterator).close();
        }
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
                    columnValue.reset(i, columnTypes[i].getPrecision(), columnTypes[i].getScale());
                    appendData(i, columnValue);
                }
                readRowNumbers++;
            }
            return readRowNumbers;
        } catch (Exception e) {
            close();
            throw new IOException("Failed to get the next batch of hudi.", e);
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        return Collections.singletonMap("timer:GetRecordReaderTime", String.valueOf(getRecordReaderTimeNs));
    }
}
