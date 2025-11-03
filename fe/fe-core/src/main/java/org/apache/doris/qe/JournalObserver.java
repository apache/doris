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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class JournalObserver implements Comparable<JournalObserver> {
    private static final Logger LOG = LogManager.getLogger(JournalObserver.class);

    private static AtomicLong idGen = new AtomicLong(0L);

    private Long id;
    private Long targetJournalVersion;
    private CountDownLatch latch;

    public JournalObserver(Long targetJournalVersion) {
        this.id = idGen.getAndIncrement();
        this.targetJournalVersion = targetJournalVersion;
        this.latch = new CountDownLatch(1);
    }

    public void update() {
        latch.countDown();
    }

    public Long getTargetJournalVersion() {
        return targetJournalVersion;
    }

    /*
     * We are waiting for this FE to replay journal to 'expectedJournalVersion' using JournalObserver.
     * Each time a journal is replayed, JournalObserver will be notified and check if the replayed
     * journal version >= 'expectedJournalVersion'. If satisfy, latch is counted down.
     * But this is not a atomic operation, the replayed journal version may already larger than expected
     * version before waiting the latch.
     * So, we wait for the latch with a small timeout in a loop until the total timeout, to avoid
     * waiting unnecessary long time.
     */
    public void waitForReplay(int timeoutMs) throws DdlException {
        long leftTimeoutMs = timeoutMs;
        final long minIntervalMs = 1000;

        try {
            boolean ok = false;
            do {
                // check if the replayed journal version is already larger than the expected version
                long replayedJournalId = Env.getCurrentEnv().getReplayedJournalId();
                if (replayedJournalId >= targetJournalVersion || timeoutMs <= 0) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("the replayed journal version {} already large than expected version: {}",
                                  replayedJournalId, targetJournalVersion);
                    }
                    return;
                }

                // waiting for notification
                ok = latch.await(minIntervalMs, TimeUnit.MILLISECONDS);
                if (ok) {
                    break;
                }

                // decrease the left timeout
                leftTimeoutMs -= minIntervalMs;
            } while (leftTimeoutMs > 0);

            if (!ok) {
                LOG.warn("timeout waiting result from master. timeout ms: {}", timeoutMs);
                throw new DdlException("Execute timeout, the command may be succeed, you'd better retry");
            }

        } catch (InterruptedException e) {
            throw new DdlException("Interrupted exception happens, "
                    + "the command may be succeed, you'd better retry");
        }
    }

    @Override
    public int compareTo(JournalObserver jo) {
        if (this.targetJournalVersion < jo.targetJournalVersion) {
            return -1;
        } else if (this.targetJournalVersion > jo.targetJournalVersion) {
            return 1;
        } else {
            if (this.id < jo.id) {
                return -1;
            } else if (this.id > jo.id) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this.hashCode() != obj.hashCode()) {
            return false;
        }
        if (!(obj instanceof JournalObserver)) {
            return false;
        }

        JournalObserver obs = ((JournalObserver) obj);
        return this.targetJournalVersion == obs.targetJournalVersion && this.id == obs.id;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public String toString() {
        return "target: " + targetJournalVersion;
    }
}
