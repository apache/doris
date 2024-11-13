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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The table that implements this interface can serve as a partition table followed by MTMV
 */
public interface MTMVRelatedTableIf extends TableIf {

    /**
     * Get all partitions of the table
     * Note: This method is called every time there is a refresh and transparent rewrite,
     * so if this method is slow, it will significantly reduce query performance
     *
     * @return partitionName->PartitionItem
     */
    Map<String, PartitionItem> getAndCopyPartitionItems() throws AnalysisException;

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
     * It is best to use the version. If there is no version, use the last update time
     * If snapshots have already been obtained in bulk in the context,
     * the results should be obtained directly from the context
     *
     * @param partitionName
     * @param context
     * @return partition snapshot at current time
     * @throws AnalysisException
     */
    MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context) throws AnalysisException;

    /**
     * getTableSnapshot
     * It is best to use the version. If there is no version, use the last update time
     * If snapshots have already been obtained in bulk in the context,
     * the results should be obtained directly from the context
     *
     * @param context
     * @return table snapshot at current time
     * @throws AnalysisException
     */
    MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context) throws AnalysisException;

    /**
     * Does the current type of table allow timed triggering
     *
     * @return If return false,The method of comparing whether to synchronize will directly return true,
     *         otherwise the snapshot information will be compared
     */
    default boolean needAutoRefresh() {
        return true;
    }

    /**
     * if allow partition column `isAllowNull`
     *
     * @return
     */
    boolean isPartitionColumnAllowNull();
}
