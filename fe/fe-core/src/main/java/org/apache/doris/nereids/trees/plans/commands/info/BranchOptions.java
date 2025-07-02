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

package org.apache.doris.nereids.trees.plans.commands.info;

import java.util.Optional;

/**
 * Represents options that can be specified for a branch operation in the Nereids module.
 * <p>
 * This class encapsulates optional parameters that control the behavior of branch operations,
 * such as specifying a snapshot ID, retention policy, number of snapshots to keep, and retention period.
 */
public class BranchOptions {
    public static final BranchOptions EMPTY = new BranchOptions(Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final Optional<Long> snapshotId;
    // retain time in milliseconds
    private final Optional<Long> retain;
    private final Optional<Integer> numSnapshots;
    private final Optional<Long> retention;

    public BranchOptions(Optional<Long> snapshotId,
                         Optional<Long> retain,
                         Optional<Integer> numSnapshots,
                         Optional<Long> retention) {
        this.snapshotId = snapshotId;
        this.retain = retain;
        this.numSnapshots = numSnapshots;
        this.retention = retention;
    }

    public Optional<Long> getSnapshotId() {
        return snapshotId;
    }

    public Optional<Long> getRetain() {
        return retain;
    }

    public Optional<Integer> getNumSnapshots() {
        return numSnapshots;
    }

    public Optional<Long> getRetention() {
        return retention;
    }

    /**
     * Generates the SQL representation of the branch options.
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (snapshotId.isPresent()) {
            sb.append(" AS OF VERSION ").append(snapshotId.get());
        }
        if (retain.isPresent()) {
            // "RETAIN", and convert retain time to MINUTES
            sb.append(" RETAIN ").append(retain.get() / 1000 / 60).append(" MINUTES");
        }
        if (numSnapshots.isPresent() || retention.isPresent()) {
            sb.append(" WITH SNAPSHOT RETENTION");
            if (numSnapshots.isPresent()) {
                sb.append(" ").append(numSnapshots.get()).append(" SNAPSHOTS");
            }
            if (retention.isPresent()) {
                sb.append(" ").append(retention.get() / 1000 / 60).append(" MINUTES");
            }
        }
        return sb.toString();
    }
}
