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

package org.apache.doris.qe;

import com.google.common.base.Preconditions;

/** Admission and accounting for client requests during FE graceful shutdown. */
public class QueryAdmissionController {
    public static final QueryAdmissionController INSTANCE = new QueryAdmissionController();

    private boolean accepting = true;
    private int activeRequests;

    // Admission and the shutdown transition share a lock. A request either contributes
    // to the drain count before shutdown, or is refused before it starts planning.
    public synchronized boolean tryAcquire() {
        if (!accepting) {
            return false;
        }
        activeRequests++;
        return true;
    }

    public synchronized void release() {
        Preconditions.checkState(activeRequests > 0, "no admitted request to release");
        activeRequests--;
    }

    // Result retrieval and session cleanup must remain possible after admission closes.
    public synchronized void acquireContinuation() {
        activeRequests++;
    }

    public synchronized void stopAccepting() {
        accepting = false;
    }

    public synchronized int getActiveRequests() {
        return activeRequests;
    }
}
