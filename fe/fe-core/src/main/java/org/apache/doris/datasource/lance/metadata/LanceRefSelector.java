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

package org.apache.doris.datasource.lance.metadata;

import org.apache.doris.analysis.TableSnapshot;

import java.util.Objects;
import java.util.Optional;

/**
 * What a query selects on a Lance table: the latest version of the main chain, an explicit
 * {@code FOR VERSION AS OF} / {@code FOR TIME AS OF} snapshot, a tag, or a branch (optionally
 * with a snapshot inside that branch). A tag and a snapshot exclude each other.
 */
public final class LanceRefSelector {
    private static final LanceRefSelector LATEST = new LanceRefSelector(Optional.empty(), Optional.empty(),
            Optional.empty());

    private final Optional<TableSnapshot> snapshot;
    private final Optional<String> tag;
    private final Optional<String> branch;

    private LanceRefSelector(Optional<TableSnapshot> snapshot, Optional<String> tag, Optional<String> branch) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.tag = Objects.requireNonNull(tag, "tag");
        this.branch = Objects.requireNonNull(branch, "branch");
    }

    /** The latest version of the main chain. */
    public static LanceRefSelector latest() {
        return LATEST;
    }

    /** A {@code FOR VERSION AS OF} / {@code FOR TIME AS OF} snapshot on the main chain. */
    public static LanceRefSelector snapshot(Optional<TableSnapshot> snapshot) {
        return snapshot.isPresent() ? new LanceRefSelector(snapshot, Optional.empty(), Optional.empty()) : LATEST;
    }

    /** The version a tag points at. */
    public static LanceRefSelector tag(String tag) {
        return new LanceRefSelector(Optional.empty(), Optional.of(tag), Optional.empty());
    }

    /** A branch, at its latest version or at the snapshot selected inside it. */
    public static LanceRefSelector branch(String branch, Optional<TableSnapshot> snapshot) {
        return new LanceRefSelector(snapshot, Optional.empty(), Optional.of(branch));
    }

    public Optional<TableSnapshot> getSnapshot() {
        return snapshot;
    }

    public Optional<String> getTag() {
        return tag;
    }

    public Optional<String> getBranch() {
        return branch;
    }
}
