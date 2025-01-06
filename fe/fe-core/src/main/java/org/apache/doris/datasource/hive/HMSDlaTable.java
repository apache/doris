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
import org.apache.doris.catalog.Env;
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

public abstract class HMSDlaTable implements MTMVBaseTableIf {
    HMSExternalTable hmsTable;

    public HMSDlaTable(HMSExternalTable table) {
        this.hmsTable = table;
    }


    abstract Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot)
            throws AnalysisException;

    abstract PartitionType getPartitionType(Optional<MvccSnapshot> snapshot);

    abstract Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) throws DdlException;

    abstract List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot);

    abstract MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot) throws AnalysisException;

    abstract MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException;

    public boolean needAutoRefresh() {
        return true;
    }

    abstract boolean isPartitionColumnAllowNull();

    @Override
    public void beforeMTMVRefresh(MTMV mtmv) throws DdlException {
        Env.getCurrentEnv().getRefreshManager()
                .refreshTable(hmsTable.getCatalog().getName(), hmsTable.getDbName(), hmsTable.getName(), true);
    }
}
