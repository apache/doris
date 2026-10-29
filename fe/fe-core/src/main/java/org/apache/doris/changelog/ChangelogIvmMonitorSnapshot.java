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

package org.apache.doris.changelog;

/**
 * What one round of {@link ChangelogIvmMonitor} sampled. A snapshot never changes once built, and the monitor
 * replaces it as a whole, so a reader such as /metrics sees a single round without taking any lock.
 */
public class ChangelogIvmMonitorSnapshot {
    // when the round completed, and how long it took
    private final long sampleTimeMs;
    private final long sampleDurationMs;
    // the changelog kept by all the tables, counting one replica of each tablet, and counting all of them
    private final long changelogSize;
    private final long totalReplicaChangelogSize;

    public ChangelogIvmMonitorSnapshot(long sampleTimeMs, long sampleDurationMs, long changelogSize,
            long totalReplicaChangelogSize) {
        this.sampleTimeMs = sampleTimeMs;
        this.sampleDurationMs = sampleDurationMs;
        this.changelogSize = changelogSize;
        this.totalReplicaChangelogSize = totalReplicaChangelogSize;
    }

    public long getSampleTimeMs() {
        return sampleTimeMs;
    }

    public long getSampleDurationMs() {
        return sampleDurationMs;
    }

    public long getChangelogSize() {
        return changelogSize;
    }

    public long getTotalReplicaChangelogSize() {
        return totalReplicaChangelogSize;
    }
}
