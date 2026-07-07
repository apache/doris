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

import java.util.Objects;

/**
 * A connector-supplied, whole-table MTMV freshness marker for a connector whose table-level change
 * signal is a last-modified TIMESTAMP rather than a snapshot id.
 *
 * <p>Returned by {@link org.apache.doris.connector.api.ConnectorMetadata#getTableFreshness}. The
 * generic table model ({@code PluginDrivenMvccExternalTable}) wraps it into an
 * {@code MTMVMaxTimestampSnapshot(name, timestampMillis)} — byte-parity with legacy hive, whose table
 * snapshot is {@code MTMVMaxTimestampSnapshot}. A connector that returns {@link java.util.Optional#empty()}
 * (the default, e.g. paimon/iceberg) keeps the snapshot-id table snapshot unchanged.</p>
 *
 * <p><b>Semantics</b> (mirror legacy {@code HiveDlaTable.getTableSnapshot}):</p>
 * <ul>
 *   <li>partitioned table &rArr; {@code name} = the partition name carrying the newest modify time,
 *       {@code timestampMillis} = that max modify time (0 when there are no partitions, with
 *       {@code name} = the table name);</li>
 *   <li>unpartitioned table &rArr; {@code name} = the table name, {@code timestampMillis} = the table's
 *       last-DDL time (0 when absent).</li>
 * </ul>
 *
 * <p>The connector computes the millis (the {@code MTMVMaxTimestampSnapshot} carries BOTH the name and
 * the timestamp so that dropping the partition that owns the max time is detected as a change); fe-core
 * never parses the underlying source properties.</p>
 */
public final class ConnectorTableFreshness {

    private final String name;
    private final long timestampMillis;

    public ConnectorTableFreshness(String name, long timestampMillis) {
        this.name = Objects.requireNonNull(name, "name");
        this.timestampMillis = timestampMillis;
    }

    /** The partition name owning the newest modify time (partitioned) or the table name (unpartitioned). */
    public String getName() {
        return name;
    }

    /** The newest modify time in epoch millis (max partition modify time, or the table last-DDL time). */
    public long getTimestampMillis() {
        return timestampMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorTableFreshness)) {
            return false;
        }
        ConnectorTableFreshness that = (ConnectorTableFreshness) o;
        return timestampMillis == that.timestampMillis && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timestampMillis);
    }

    @Override
    public String toString() {
        return "ConnectorTableFreshness{name='" + name + "', timestampMillis=" + timestampMillis + "}";
    }
}
