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

package org.apache.paimon.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Doris's copy of the handler paimon gives its threads for uncaught exceptions: this one logs them,
 * where paimon's stops the process.
 *
 * <p>Paimon hands {@link #INSTANCE} to every thread its {@code ExecutorThreadFactory} makes unless the
 * caller names another handler - among them the pool behind {@code AsyncRecordReader}, which reads
 * each large data file of a merge read on a thread of its own, and the pools of
 * {@code ParallelExecution}. Paimon's own version calls {@code System.exit(-17)}, which suits the Flink
 * or Spark worker paimon is written for. Inside BE it is an {@code ::exit()} from a JVM thread: the C++
 * global destructors run under a BE that is still serving, the failure {@code jvm_launcher.cpp}
 * describes for the JVM's signal handlers, and BE aborts. The fluss client's handler of the same
 * name did exactly that when concurrent reads exhausted the JVM heap, and paimon's merge reads are
 * the readers that exhaust it most easily. A pool thread is cheap to lose: a task's own exception goes
 * to whoever waits on its future, so this handler only sees what kills a thread between tasks - an
 * {@code OutOfMemoryError} while an idle thread waits for work - and its pool starts another.
 *
 * <p>Found ahead of paimon-common's copy, and of the one paimon-hive-connector bundles, because the
 * jar it ships in names it in {@code Doris-Shadows-Classes} (see this module's pom). It declares every
 * member paimon's version does, so the paimon classes compiled against that one link to this one.
 */
public final class FatalExitExceptionHandler implements Thread.UncaughtExceptionHandler {

    public static final FatalExitExceptionHandler INSTANCE = new FatalExitExceptionHandler();

    /** The status paimon's version exits with. Nothing exits with it here; it stays for linkage. */
    public static final int EXIT_CODE = -17;

    private static final Logger LOG = LoggerFactory.getLogger(FatalExitExceptionHandler.class);

    @Override
    public void uncaughtException(Thread thread, Throwable e) {
        LOG.error("Paimon thread '{}' died of an uncaught exception. Paimon would stop the process"
                + " here; inside BE the thread is lost and the process carries on.", thread.getName(), e);
    }
}
