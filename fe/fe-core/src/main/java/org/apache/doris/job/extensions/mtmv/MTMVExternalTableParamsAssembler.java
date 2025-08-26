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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class MTMVExternalTableParamsAssembler {
    private boolean incremental;

    public boolean isIncremental() {
        return incremental;
    }

    static MTMVExternalTableParamsAssembler getMTMVExternalTableParamsAssembler(MTMV mtmv)
            throws AnalysisException {
        TableIf table = MTMVUtil.getTable(MaterializedViewUtils.getIncrementalMVBaseTable(mtmv));
        if (table instanceof PaimonExternalTable) {
            return new MTMVPaimonParamsAssembler();
        } else {
            throw new NereidsException(new RuntimeException(
                    "Unsupported table type: " + table.getClass().getName()));
        }
    }

    void markReadBySnapshot(Map<String, String> params, long snapshotId) {
        this.incremental = false;
    }

    void markReadBySnapshotIncremental(Map<String, String> params, long startSnapshotId, long endSnapshotId) {
        this.incremental = true;
    }

    abstract List<String> calculateNeedRefreshPartitions(
            MTMVExternalTableParamsAssembler.RefreshSnapshotInfo refreshSnapshotInfo,
            Map<String, Set<String>> partitionMappings, MvccTable table);

    abstract Optional<RefreshSnapshotInfo> calculateNextSnapshot(
            MvccTable table, long startSnapshotId, long endSnapshotId);

    public enum RefreshType {
        INCREMENTAL,
        OVERWRITE
    }

    public static class RefreshSnapshotInfo {
        private long snapshotId;
        private RefreshType refreshType;

        public RefreshSnapshotInfo(long snapshotId, RefreshType refreshType) {
            this.snapshotId = snapshotId;
            this.refreshType = refreshType;
        }

        public long getSnapshotId() {
            return snapshotId;
        }

        public RefreshType getRefreshType() {
            return refreshType;
        }
    }
}
