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

package org.apache.doris.catalog;

import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A lightweight wrapper base for read changelog of table
 *
 * <p>A changelog scan reads the change rows of an offset range, not the table, and neither the
 * range nor the changelog index reports how many change rows it holds: a read that starts at the
 * beginning of the stream returns about the table's rows while a delta returns a few, and FE
 * cannot tell the two apart.
 *
 * <p>A stream read knows better: it records, next to the offset it reads from, the row count of
 * the partition's changelog index, and hands the difference to this wrapper as the per-partition
 * estimate (see {@link org.apache.doris.catalog.stream.PartitionStreamState}). Without such an
 * estimate the wrapper answers with the row count of the changelog index, i.e. the size of the
 * whole changelog: that over-estimates a delta, but a value in the right order of magnitude keeps
 * the optimizer from treating the read as an empty relation, which is the direction that costs
 * memory.
 */
public class OlapChangelogTableWrapper extends OlapTableWrapper {

    private final MaterializedIndexMeta changelogIndexMeta;

    public OlapChangelogTableWrapper(OlapTable originTable) {
        this(originTable, Maps.newHashMap());
    }

    public OlapChangelogTableWrapper(OlapTable originTable, Map<Long, Pair<Long, Long>> partitionOffsetMap) {
        this(originTable, partitionOffsetMap, Collections.emptyMap());
    }

    public OlapChangelogTableWrapper(OlapTable originTable, Map<Long, Pair<Long, Long>> partitionOffsetMap,
            Map<Long, Long> partitionVisibleVersionMap) {
        this(originTable, partitionOffsetMap, partitionVisibleVersionMap, Collections.emptyMap());
    }

    public OlapChangelogTableWrapper(OlapTable originTable, Map<Long, Pair<Long, Long>> partitionOffsetMap,
            Map<Long, Long> partitionVisibleVersionMap, Map<Long, Long> partitionRowCounts) {
        super(originTable, originTable.getName(), originTable.getChangelogIndexMeta().getSchema(), KeysType.DUP_KEYS,
                partitionOffsetMap, partitionVisibleVersionMap, partitionRowCounts);
        this.changelogIndexMeta = originTable.getChangelogIndexMeta();
        Preconditions.checkNotNull(changelogIndexMeta, "changelog meta is null, table=%s", originTable.getName());
        this.setBaseIndexId(changelogIndexMeta.getIndexId());
    }

    @Override
    public long getBaseIndexId() {
        return changelogIndexMeta.getIndexId();
    }

    public static boolean isChangelogSyntheticColumn(Column column) {
        return column.getName().equals(Column.CHANGELOG_TSO_COL)
                || column.getName().equals(Column.CHANGELOG_LSN_COL);
    }

    @Override
    public MaterializedIndex getPartitionIndex(Partition partition, long indexId) {
        MaterializedIndex index = partition.getIndex(indexId);
        if (index != null) {
            return index;
        }
        // The changelog index meta does not exist as a partition index.
        // For scan range generation, reuse the base index's tablets.
        if (indexId == changelogIndexMeta.getIndexId()) {
            return partition.getIndex(originTable.getBaseIndexId());
        }
        return null;
    }

    @Override
    public KeysType getKeysType() {
        return KeysType.DUP_KEYS;
    }

    @Override
    public List<Long> selectNonEmptyPartitionIds(Collection<Long> partitionIds,
            Optional<StreamReadMode> streamReadMode) {
        if (Config.isCloudMode() && !hasFixedVisibleVersions()) {
            // A changelog scan can start immediately after its target transaction becomes visible. Refresh
            // cached-empty or unknown partitions so an older cache entry cannot prune newly visible changelog data.
            List<CloudPartition> partitions = partitionIds.stream()
                    .map(this::getPartition)
                    .filter(Objects::nonNull)
                    .map(partition -> (CloudPartition) partition)
                    .collect(Collectors.toList());
            return CloudPartition.selectNonEmptyPartitionIdsFromMs(partitions);
        }
        return super.selectNonEmptyPartitionIds(partitionIds, streamReadMode);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        OlapChangelogTableWrapper other = (OlapChangelogTableWrapper) obj;
        return changelogIndexMeta.equals(other.changelogIndexMeta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), changelogIndexMeta.getChangelogIndexId());
    }
}
