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

package org.apache.doris.connector.api.mvcc;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One final (post-merge) Doris partition in a {@link ConnectorMvccPartitionView}.
 *
 * <p>For a {@link ConnectorMvccPartitionView.Style#RANGE} view, {@link #getLowerBound()} /
 * {@link #getUpperBound()} are the connector's pre-rendered partition-key value tuples for the
 * closed-open {@code [lower, upper)} range (one string per partition column), and the generic model
 * assembles them into a {@code RangePartitionItem} using the table's partition column types — so no
 * data-source-specific range math leaks into fe-core. {@link #getFreshnessValue()} is the per-partition
 * marker (a snapshot id or epoch-millis timestamp, per the view's
 * {@link ConnectorMvccPartitionView#getFreshness()}) the generic model wraps into the matching
 * {@code MTMVSnapshotIf}.</p>
 */
public final class ConnectorMvccPartition {

    private final String name;
    private final List<String> lowerBound;
    private final List<String> upperBound;
    private final long freshnessValue;

    public ConnectorMvccPartition(String name, List<String> lowerBound, List<String> upperBound,
            long freshnessValue) {
        this.name = Objects.requireNonNull(name, "name");
        this.lowerBound = lowerBound == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(lowerBound);
        this.upperBound = upperBound == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(upperBound);
        this.freshnessValue = freshnessValue;
    }

    /** The final Doris partition name (the enclosing partition's name after any overlap merge). */
    public String getName() {
        return name;
    }

    /** Pre-rendered closed lower-bound value tuple (one entry per partition column). */
    public List<String> getLowerBound() {
        return lowerBound;
    }

    /** Pre-rendered open upper-bound value tuple (one entry per partition column). */
    public List<String> getUpperBound() {
        return upperBound;
    }

    /** The per-partition freshness marker (snapshot id or epoch millis, per the view's freshness kind). */
    public long getFreshnessValue() {
        return freshnessValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorMvccPartition)) {
            return false;
        }
        ConnectorMvccPartition that = (ConnectorMvccPartition) o;
        return freshnessValue == that.freshnessValue
                && name.equals(that.name)
                && lowerBound.equals(that.lowerBound)
                && upperBound.equals(that.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, lowerBound, upperBound, freshnessValue);
    }

    @Override
    public String toString() {
        return "ConnectorMvccPartition{name='" + name
                + "', lower=" + lowerBound
                + ", upper=" + upperBound
                + ", freshness=" + freshnessValue + "}";
    }
}
