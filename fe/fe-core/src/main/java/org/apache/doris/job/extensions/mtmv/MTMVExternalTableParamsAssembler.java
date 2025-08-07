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
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;

import java.util.Map;

public interface MTMVExternalTableParamsAssembler {
    public static MTMVExternalTableParamsAssembler getMTMVExternalTableParamsAssembler(MTMV mtmv)
            throws AnalysisException {
        TableIf table = MTMVUtil.getTable(MaterializedViewUtils.getIncrementalMVBaseTable(mtmv));
        if (table instanceof PaimonExternalTable) {
            return new MTMVPaimonParamsAssembler();
        } else {
            throw new NereidsException(new RuntimeException(
                    "Unsupported table type: " + table.getClass().getName()));
        }
    }

    void markReadBySnapshot(Map<String, String> params, long snapshotId);

    void markReadBySnapshotIncremental(Map<String, String> params, long startSnapshotId, long endSnapshotId);
}
