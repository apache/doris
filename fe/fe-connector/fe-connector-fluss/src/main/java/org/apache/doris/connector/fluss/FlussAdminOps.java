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

package org.apache.doris.connector.fluss;

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableStats;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Every remote call this connector makes to a fluss cluster, as one synchronous interface.
 *
 * <p>It exists to make the connector testable without a mocking framework. Metadata mapping and, above
 * all, split generation are pure functions of what these methods return — offsets, snapshot ids,
 * partition lists — so a recording implementation lets a unit test drive the interesting boundaries
 * (an empty bucket, a lake offset that has caught up with the log, a partition that exists only in the
 * lake) directly, instead of trying to coax a live cluster into each of those states. The
 * connection-backed implementation is then the only place that has to worry about futures, timeouts
 * and error wrapping, and integration tests against a real cluster cover it.
 *
 * <p>Signatures mirror {@code org.apache.fluss.client.admin.Admin} one for one, minus the asynchrony:
 * every method blocks with a timeout and returns the value, so nothing downstream handles a
 * {@link java.util.concurrent.CompletableFuture}. Fluss's own exceptions (subclasses of
 * {@code org.apache.fluss.exception.ApiException}) propagate unwrapped — callers discriminate on them,
 * notably {@code LakeTableSnapshotNotExistException} when deciding whether a union read is possible.
 *
 * <p>Implementations are NOT thread-safe and are scoped to one statement.
 */
public interface FlussAdminOps {

    List<String> listDatabases();

    boolean databaseExists(String databaseName);

    List<String> listTables(String databaseName);

    boolean tableExists(TablePath tablePath);

    TableInfo getTableInfo(TablePath tablePath);

    List<PartitionInfo> listPartitionInfos(TablePath tablePath);

    /** Server-side partial partition pruning: only partitions matching {@code partialPartitionSpec}. */
    List<PartitionInfo> listPartitionInfos(TablePath tablePath, PartitionSpec partialPartitionSpec);

    TableStats getTableStats(TablePath tablePath);

    /** Latest kv snapshot per bucket of a primary-key table, with the log offset each snapshot ends at. */
    KvSnapshots getLatestKvSnapshots(TablePath tablePath);

    /** As above, for one partition of a partitioned primary-key table. */
    KvSnapshots getLatestKvSnapshots(TablePath tablePath, String partitionName);

    /**
     * The lake snapshot a reader may actually read, together with the per-bucket log offset the lake
     * ends at.
     *
     * <p>Deliberately not the <em>latest</em> lake snapshot: data that tiering has just committed can
     * still be unreadable in the lake (a paimon deletion-vector table keeps it in L0 for a while), and
     * a reader that took the latest snapshot id would skip the log covering it and lose rows. Both
     * halves — snapshot id and offsets — come from this one call so they cannot disagree.
     *
     * @throws org.apache.fluss.exception.LakeTableSnapshotNotExistException if nothing is readable in
     *         the lake yet, which is the normal state of a table whose tiering has not committed once
     */
    LakeSnapshot getReadableLakeSnapshot(TablePath tablePath);

    /** Offsets for {@code buckets} of a non-partitioned table, keyed by bucket id. */
    Map<Integer, Long> listOffsets(TablePath tablePath, Collection<Integer> buckets, OffsetSpec offsetSpec);

    /** Offsets for {@code buckets} of one partition, keyed by bucket id. */
    Map<Integer, Long> listOffsets(TablePath tablePath, String partitionName,
            Collection<Integer> buckets, OffsetSpec offsetSpec);
}
