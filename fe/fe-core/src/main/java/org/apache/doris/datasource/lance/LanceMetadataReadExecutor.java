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

package org.apache.doris.datasource.lance;

import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;

import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs Lance JNI metadata reads behind a finite FE concurrency and deadline boundary. */
final class LanceMetadataReadExecutor {
    private static final int DEFAULT_TIMEOUT_SECONDS = 60;
    private static final int MAX_CONCURRENT_READS = 4;
    private static final int MAX_QUEUED_READS = 16;
    private static final ThreadPoolExecutor EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            MAX_CONCURRENT_READS,
            MAX_QUEUED_READS,
            "lance-metadata-read",
            false,
            new ThreadPoolExecutor.AbortPolicy());

    private LanceMetadataReadExecutor() {
    }

    static <T> T execute(Callable<T> task) throws Exception {
        ConnectContext context = ConnectContext.get();
        int queryTimeoutSeconds = context == null
                ? DEFAULT_TIMEOUT_SECONDS : context.getQueryTimeoutS();
        int timeoutSeconds = queryTimeoutSeconds > 0
                ? Math.min(queryTimeoutSeconds, DEFAULT_TIMEOUT_SECONDS) : DEFAULT_TIMEOUT_SECONDS;
        return execute(task, EXECUTOR, timeoutSeconds, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    static <T> T execute(Callable<T> task, ExecutorService executor,
            long timeout, TimeUnit timeoutUnit) throws Exception {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Lance metadata read timeout must be positive");
        }
        long timeoutNanos = timeoutUnit.toNanos(timeout);
        if (timeoutNanos <= 0) {
            throw new IllegalArgumentException("Lance metadata read timeout is too small");
        }

        long deadlineNanos = System.nanoTime() + timeoutNanos;
        Future<T> future;
        try {
            future = executor.submit(() -> {
                // A request can expire while waiting in the finite queue. Do not enter JNI for a
                // result whose caller has already timed out.
                if (remainingNanos(deadlineNanos) <= 0) {
                    throw timeoutFailure(timeout, timeoutUnit);
                }
                return task.call();
            });
        } catch (RejectedExecutionException e) {
            throw new MetadataReadCapacityException(
                    "Lance metadata read capacity is exhausted");
        }

        try {
            long remainingNanos = remainingNanos(deadlineNanos);
            if (remainingNanos <= 0) {
                throw timeoutFailure(timeout, timeoutUnit);
            }
            return future.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            // Deliberately do not cancel or interrupt the Future. If JNI has started, the worker
            // remains the sole owner of its Dataset and allocator until the native call returns.
            throw timeoutFailure(timeout, timeoutUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // As with timeout, interruption of the waiting caller must not interrupt JNI.
            throw new MetadataReadInterruptedException(
                    "Interrupted while waiting for Lance metadata read");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static long remainingNanos(long deadlineNanos) {
        return deadlineNanos - System.nanoTime();
    }

    private static MetadataReadTimeoutException timeoutFailure(long timeout, TimeUnit timeoutUnit) {
        return new MetadataReadTimeoutException("Lance metadata read timed out after "
                + timeout + " " + timeoutUnit.name().toLowerCase(Locale.ROOT));
    }

    static final class MetadataReadTimeoutException extends RuntimeException {
        MetadataReadTimeoutException(String message) {
            super(message);
        }
    }

    static final class MetadataReadCapacityException extends RuntimeException {
        MetadataReadCapacityException(String message) {
            super(message);
        }
    }

    static final class MetadataReadInterruptedException extends RuntimeException {
        MetadataReadInterruptedException(String message) {
            super(message);
        }
    }
}
