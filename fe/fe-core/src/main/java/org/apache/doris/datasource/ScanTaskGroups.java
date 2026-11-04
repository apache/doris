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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.FairTaskExecutor;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

/** Statement-owned queues. A deferred Flight scan retains its lease until coordinator cleanup. */
public final class ScanTaskGroups implements Closeable {
    private final FairTaskExecutor.Group[] groups = new FairTaskExecutor.Group[2];
    private int references = 1;
    private boolean ownerClosed;

    public synchronized Lease acquire() {
        if (ownerClosed) {
            throw new CancellationException("Statement scan task groups are closed");
        }
        references++;
        return new Lease();
    }

    private synchronized FairTaskExecutor.Group group(boolean metadata) {
        int index = metadata ? 1 : 0;
        if (references == 0) {
            throw new CancellationException("Statement scan task groups are closed");
        }
        if (groups[index] == null) {
            groups[index] = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getScanTaskExecutor(metadata).newGroup(Config.external_scan_task_queue_capacity);
        }
        return groups[index];
    }

    private void release() {
        FairTaskExecutor.Group[] retired = null;
        synchronized (this) {
            if (--references == 0) {
                retired = groups.clone();
            }
        }
        if (retired != null) {
            for (FairTaskExecutor.Group group : retired) {
                if (group != null) {
                    group.close();
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (ownerClosed) {
                return;
            }
            ownerClosed = true;
        }
        release();
    }

    public final class Lease implements Closeable {
        private boolean closed;
        private final List<Runnable> closeActions = new ArrayList<>();

        public synchronized FairTaskExecutor.Group group(boolean metadata) {
            if (closed) {
                throw new CancellationException("Scan task lease is closed");
            }
            return ScanTaskGroups.this.group(metadata);
        }

        public synchronized boolean isClosed() {
            return closed;
        }

        public void onClose(Runnable action) {
            synchronized (this) {
                if (!closed) {
                    closeActions.add(action);
                    return;
                }
            }
            action.run();
        }

        @Override
        public void close() {
            List<Runnable> actions;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                actions = new ArrayList<>(closeActions);
                closeActions.clear();
            }
            try {
                actions.forEach(Runnable::run);
            } finally {
                release();
            }
        }
    }
}
