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
 * Represents the options available for managing tags in Iceberg through Doris.
 * This class encapsulates optional parameters that can be specified when creating or manipulating tags.
 *
 * <p>{@code TagOptions} is typically used in conjunction with commands that interact with Iceberg tables,
 * such as creating a tag or specifying retention policies.</p>
 */
public class TagOptions {
    public static final TagOptions EMPTY = new TagOptions(Optional.empty(), Optional.empty());

    private final Optional<Long> snapshotId;

    private final Optional<Long> retain;

    public TagOptions(Optional<Long> snapshotId,
                      Optional<Long> retain) {
        this.snapshotId = snapshotId;
        this.retain = retain;
    }

    public Optional<Long> getSnapshotId() {
        return snapshotId;
    }

    public Optional<Long> getRetain() {
        return retain;
    }

    /**
     * Generates the SQL representation of the tag options.
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
        return sb.toString();
    }
}
