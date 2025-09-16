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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVSnapshotIf;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This abstract class represents a Hive Metastore (HMS) Dla Table and provides a blueprint for
 * various operations related to metastore tables in Doris.
 *
 * Purpose:
 * - To encapsulate common functionalities that HMS Dla tables should have for implementing other interfaces
 *
 * Why needed:
 * - To provide a unified way to manage and interact with different kinds of Dla Table
 * - To facilitate the implementation of multi-table materialized views (MTMV) by providing necessary
 *   methods for snapshot and partition management.
 * - To abstract out the specific details of HMS table operations, making the code more modular and maintainable.
 */
public abstract class HMSDlaTable implements MTMVBaseTableIf {
    protected HMSExternalTable hmsTable;

    public HMSDlaTable(HMSExternalTable table) {
        this.hmsTable = table;
    }

    abstract Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot)
            throws AnalysisException;

    abstract PartitionType getPartitionType(Optional<MvccSnapshot> snapshot);

    abstract Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot);

    abstract List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot);

    abstract MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot) throws AnalysisException;

    abstract MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException;

    abstract MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException;

    abstract boolean isPartitionColumnAllowNull();

    @Override
    public void beforeMTMVRefresh(MTMV mtmv) throws DdlException {
    }

    /**
     * If the table is supported as related table.
     * For example, an Iceberg table may become unsupported after partition revolution.
     * @return
     */
    protected boolean isValidRelatedTable() {
        return true;
    }
}
