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

import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.split.SplitAssignment;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Scan tasks owned by one planning/execution attempt, including the interval before a coordinator exists.
 * Registration and termination are serialized; callbacks run outside the monitor. The owner keeps this
 * object through Arrow Flight deferral and creates a fresh object when replanning.
 */
public final class QueryScanTaskScope {
    private final Map<SplitAssignment, Runnable> scans = new IdentityHashMap<>();
    private Status cancellation;
    private boolean closed;

    public void register(SplitAssignment assignment, Runnable unregister) {
        Status reason;
        boolean finished;
        synchronized (this) {
            reason = cancellation;
            finished = closed;
            if (!finished) {
                scans.put(assignment, unregister);
            }
        }
        if (reason != null) {
            assignment.setException(new UserException(reason.getErrorMsg()));
        } else if (finished) {
            assignment.stop();
        }
        if (finished) {
            unregister.run();
        }
    }

    public void cancel(Status reason) {
        Map<SplitAssignment, Runnable> snapshot;
        synchronized (this) {
            if (closed || cancellation != null) {
                return;
            }
            cancellation = reason;
            snapshot = new IdentityHashMap<>(scans);
        }
        snapshot.keySet().forEach(scan -> scan.setException(new UserException(reason.getErrorMsg())));
    }

    public void close() {
        Map<SplitAssignment, Runnable> snapshot;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            snapshot = new IdentityHashMap<>(scans);
            scans.clear();
        }
        snapshot.forEach((scan, unregister) -> {
            scan.stop();
            unregister.run();
        });
    }
}
