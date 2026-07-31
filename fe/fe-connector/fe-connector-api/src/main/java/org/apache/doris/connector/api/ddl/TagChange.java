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
 * Neutral carrier for a {@code CREATE [OR REPLACE] TAG} request, decoupling the connector SPI from the
 * fe-core/nereids {@code CreateOrReplaceTagInfo}/{@code TagOptions} types.
 *
 * <p>A {@code null} {@code snapshotId} means "use the table's current snapshot" (resolved by the connector
 * against the live table); a tag, unlike a branch, requires a snapshot, so the connector fails loud when the
 * current snapshot is also absent. {@code maxRefAgeMs} (SQL {@code RETAIN}) is {@code null} when unset.</p>
 */
public final class TagChange {

    private final String name;
    private final boolean create;
    private final boolean replace;
    private final boolean ifNotExists;
    private final Long snapshotId;
    private final Long maxRefAgeMs;

    public TagChange(String name, boolean create, boolean replace, boolean ifNotExists,
            Long snapshotId, Long maxRefAgeMs) {
        this.name = name;
        this.create = create;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.snapshotId = snapshotId;
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
    public Long getMaxRefAgeMs() {
        return maxRefAgeMs;
    }
}
