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

package org.apache.doris.connector.hms;

import shade.doris.hive.org.apache.thrift.TException;

import java.util.concurrent.Callable;

/** Bridges one logical client invocation to every raw HMS attempt made by RetryingMetaStoreClient. */
final class HmsRemoteCallTracking {

    private static final ThreadLocal<Context> CURRENT = new ThreadLocal<>();

    private HmsRemoteCallTracking() {
    }

    static <T> T withTracker(HmsPartitionBatchExecutor.RemoteCallTracker tracker, int itemCount,
            Callable<T> clientInvocation) throws Exception {
        Context previous = CURRENT.get();
        CURRENT.set(new Context(tracker, itemCount));
        try {
            return clientInvocation.call();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    static <T> T trackWireAttempt(ThriftCall<T> wireAttempt) throws TException {
        Context context = CURRENT.get();
        if (context == null) {
            return wireAttempt.call();
        }
        try {
            return context.tracker.call(context.itemCount, wireAttempt::call);
        } catch (TException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @FunctionalInterface
    interface ThriftCall<T> {
        T call() throws TException;
    }

    private static final class Context {
        private final HmsPartitionBatchExecutor.RemoteCallTracker tracker;
        private final int itemCount;

        private Context(HmsPartitionBatchExecutor.RemoteCallTracker tracker, int itemCount) {
            this.tracker = tracker;
            this.itemCount = itemCount;
        }
    }
}
