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

package org.apache.doris.connector.api.ddl;

/**
 * Neutral carrier for a {@code CREATE [OR REPLACE] BRANCH} request, decoupling the connector SPI from the
 * fe-core/nereids {@code CreateOrReplaceBranchInfo}/{@code BranchOptions} types.
 *
 * <p>The retention fields are nullable ({@code null} = not specified in the SQL, so the connector leaves the
 * corresponding setting untouched), mirroring the {@code Optional<>} fields of the source {@code BranchOptions}.
 * They are named after the snapshot-management knobs they drive, so a connector applies them 1:1:
 * {@code maxSnapshotAgeMs} (SQL {@code RETAIN}), {@code minSnapshotsToKeep} (SQL {@code WITH SNAPSHOT RETENTION
 * n SNAPSHOTS}), {@code maxRefAgeMs} (SQL {@code WITH SNAPSHOT RETENTION ... MINUTES}). A {@code null}
 * {@code snapshotId} means "use the table's current snapshot", resolved by the connector against the live table.</p>
 */
public final class BranchChange {

    private final String name;
    private final boolean create;
    private final boolean replace;
    private final boolean ifNotExists;
    private final Long snapshotId;
    private final Long maxSnapshotAgeMs;
    private final Integer minSnapshotsToKeep;
    private final Long maxRefAgeMs;

    public BranchChange(String name, boolean create, boolean replace, boolean ifNotExists,
            Long snapshotId, Long maxSnapshotAgeMs, Integer minSnapshotsToKeep, Long maxRefAgeMs) {
        this.name = name;
        this.create = create;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.snapshotId = snapshotId;
        this.maxSnapshotAgeMs = maxSnapshotAgeMs;
        this.minSnapshotsToKeep = minSnapshotsToKeep;
        this.maxRefAgeMs = maxRefAgeMs;
    }

    public String getName() {
        return name;
    }

    public boolean isCreate() {
        return create;
    }

    public boolean isReplace() {
        return replace;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    /** The target snapshot id, or {@code null} to use the table's current snapshot. */
    public Long getSnapshotId() {
        return snapshotId;
    }

    /** SQL {@code RETAIN} in ms, or {@code null} when unset. */
    public Long getMaxSnapshotAgeMs() {
        return maxSnapshotAgeMs;
    }

    /** SQL {@code WITH SNAPSHOT RETENTION n SNAPSHOTS}, or {@code null} when unset. */
    public Integer getMinSnapshotsToKeep() {
        return minSnapshotsToKeep;
    }

    /** SQL {@code WITH SNAPSHOT RETENTION ... MINUTES} in ms, or {@code null} when unset. */
    public Long getMaxRefAgeMs() {
        return maxRefAgeMs;
    }
}
