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

package org.apache.doris.service;

/** Admission and draining of Stream Load RPC handlers before stopping Thrift. */
public final class TxnRpcAdmissionController {
    public static final TxnRpcAdmissionController INSTANCE = new TxnRpcAdmissionController();

    private boolean accepting = true;
    private int activeRequests;

    public synchronized Guard tryEnter() {
        if (!accepting) {
            return null;
        }
        activeRequests++;
        return new Guard();
    }

    /** Closing admission and registering a handler share the same lock. */
    public synchronized void stopAccepting() {
        accepting = false;
    }

    /** Fence a new MS RPC racing the shutdown thread's final idle observation. */
    public synchronized boolean stopAcceptingIfIdle() {
        if (activeRequests != 0) {
            return false;
        }
        accepting = false;
        return true;
    }

    public synchronized int getActiveRequests() {
        return activeRequests;
    }

    public final class Guard implements AutoCloseable {
        private boolean closed;

        private Guard() {
        }

        @Override
        public void close() {
            synchronized (TxnRpcAdmissionController.this) {
                if (!closed) {
                    closed = true;
                    activeRequests--;
                }
            }
        }
    }
}
