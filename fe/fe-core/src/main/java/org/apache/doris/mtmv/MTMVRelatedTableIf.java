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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * The table that implements this interface can serve as a partition table followed by MTMV
 */
public interface MTMVRelatedTableIf extends TableIf {

    /**
     * Get all partitions of the table
     *
     * @return partitionId->PartitionItem
     */
    Map<Long, PartitionItem> getAndCopyPartitionItems();

    /**
     * Obtain a list of partitions filtered by time
     *
     * @param pos The position of the partition column to be checked in all partition columns
     * @param config
     * @return
     * @throws AnalysisException
     */
    default Map<Long, PartitionItem> getPartitionItemsByTimeFilter(int pos, MTMVPartitionSyncConfig config)
            throws AnalysisException {
        Map<Long, PartitionItem> partitionItems = getAndCopyPartitionItems();
        if (config.getSyncLimit() <= 0) {
            return partitionItems;
        }
        long nowTruncSubSec = MTMVUtil.getNowTruncSubSec(config.getTimeUnit(), config.getSyncLimit());
        Optional<String> dateFormat = config.getDateFormat();
        Map<Long, PartitionItem> res = Maps.newHashMap();
        for (Entry<Long, PartitionItem> entry : partitionItems.entrySet()) {
            if (entry.getValue().isGreaterThanSpecifiedTime(pos, dateFormat, nowTruncSubSec)) {
                res.put(entry.getKey(), entry.getValue());
            }
        }
        return res;
    }

    /**
     * getPartitionType LIST/RANGE/UNPARTITIONED
     *
     * @return
     */
    PartitionType getPartitionType();

    /**
     * getPartitionColumnNames
     *
     * @return
     * @throws DdlException
     */
    Set<String> getPartitionColumnNames() throws DdlException;

    /**
     * getPartitionColumns
     *
     * @return
     */
    List<Column> getPartitionColumns();

    /**
     * getPartitionSnapshot
     *
     * @param partitionId
     * @return partition snapshot at current time
     * @throws AnalysisException
     */
    MTMVSnapshotIf getPartitionSnapshot(long partitionId) throws AnalysisException;

    /**
     * getTableSnapshot
     *
     * @return table snapshot at current time
     * @throws AnalysisException
     */
    MTMVSnapshotIf getTableSnapshot() throws AnalysisException;

    /**
     * getPartitionName
     *
     * @param partitionId
     * @return partitionName
     * @throws AnalysisException
     */
    String getPartitionName(long partitionId) throws AnalysisException;

    /**
     * Does the current type of table allow timed triggering
     *
     * @return If return false,The method of comparing whether to synchronize will directly return true,
     *         otherwise the snapshot information will be compared
     */
    boolean needAutoRefresh();

    /**
     * if allow partition column `isAllowNull`
     *
     * @return
     */
    boolean isPartitionColumnAllowNull();
}
