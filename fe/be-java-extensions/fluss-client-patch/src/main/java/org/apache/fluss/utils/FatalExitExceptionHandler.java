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

package org.apache.fluss.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Doris's copy of the handler fluss gives its client threads for uncaught exceptions: this one logs
 * them, where fluss's stops the process.
 *
 * <p>Fluss hands {@link #INSTANCE} to every thread its {@code ExecutorThreadFactory} makes - the
 * admin's metadata refresh, the remote file downloader, the security token renewal, the lookup and
 * write clients, the delayer behind its future timeouts - and to futures {@code FutureUtils} asserts
 * never fail. Fluss's own version calls {@code System.exit(-17)}, which suits a fluss server, a process
 * of its own. Inside BE it is an {@code ::exit()} from a JVM thread: the C++ global destructors run
 * under a BE that is still serving, the failure {@code jvm_launcher.cpp} describes for the JVM's
 * signal handlers. Exhausting the JVM heap with concurrent union reads did exactly that - one of these
 * threads ran out of memory, and BE aborted in its compaction thread on a destroyed mutex. Nothing a
 * fluss client thread does is worth the BE: the thread that threw is gone, the pools fluss runs on
 * start another, and the scans it was serving fail with errors of their own.
 *
 * <p>Found ahead of fluss-client's copy because the jar it ships in names it in
 * {@code Doris-Shadows-Classes} (see this module's pom). It declares every member fluss's version does,
 * so the fluss classes compiled against that one link to this one.
 */
public final class FatalExitExceptionHandler implements Thread.UncaughtExceptionHandler {

    public static final FatalExitExceptionHandler INSTANCE = new FatalExitExceptionHandler();

    /** The status fluss's version exits with. Nothing exits with it here; it stays for linkage. */
    public static final int EXIT_CODE = -17;

    private static final Logger LOG = LoggerFactory.getLogger(FatalExitExceptionHandler.class);

    @Override
    public void uncaughtException(Thread thread, Throwable e) {
        LOG.error("Fluss client thread '{}' died of an uncaught exception. Fluss would stop the process"
                + " here; inside BE the thread is lost and the process carries on.", thread.getName(), e);
    }
}
