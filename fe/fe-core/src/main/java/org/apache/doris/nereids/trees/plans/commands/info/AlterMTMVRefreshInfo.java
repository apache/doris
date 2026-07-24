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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.UserException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * rename
 */
public class AlterMTMVRefreshInfo extends AlterMTMVInfo {
    private final MTMVRefreshInfo refreshInfo;

    /**
     * constructor for alter MTMV
     */
    public AlterMTMVRefreshInfo(TableNameInfo mvName, MTMVRefreshInfo refreshInfo) {
        super(mvName);
        this.refreshInfo = Objects.requireNonNull(refreshInfo, "require refreshInfo object");
    }

    public void analyze(ConnectContext ctx) throws AnalysisException {
        super.analyze(ctx);
        refreshInfo.validate();
        validateRefreshMethodCompat();
    }

    private void validateRefreshMethodCompat() {
        try {
            MTMV mtmv = (MTMV) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException(getMvName().getDb())
                    .getTableOrMetaException(getMvName().getTbl(), TableIf.TableType.MATERIALIZED_VIEW);
            RefreshMethod newMethod = refreshInfo.getRefreshMethod();
            if (newMethod == null) {
                return;
            }
            if (newMethod == RefreshMethod.INCREMENTAL) {
                // ALTER only changes metadata. It cannot add the hidden IVM
                // row-id column, UNIQUE KEY layout, or persisted IVM streams.
                throw new AnalysisException(
                        "Cannot ALTER refresh method to INCREMENTAL. "
                        + "Please recreate the materialized view.");
            }
            if (newMethod == RefreshMethod.PARTITIONS
                    && mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
                // A persisted PARTITIONS policy would run on every schedule or
                // commit, so reject it when the MV has no partition definition.
                // One-shot manual PARTITIONS FALLBACK is handled by MTMVTask.
                throw new AnalysisException(
                        "Cannot ALTER refresh method to PARTITIONS on a non-partitioned materialized view. "
                        + "Please recreate the materialized view with PARTITION BY.");
            }
            if (newMethod == mtmv.getRefreshInfo().getRefreshMethod()) {
                return;
            }
            if (mtmv.isIvm()) {
                // Changing an IVM MV to another default method would leave IVM
                // physical metadata in place but make refreshInfo describe a
                // different shape. Keep the invariant by requiring recreation.
                throw new AnalysisException(
                        "Cannot ALTER the refresh method of an INCREMENTAL materialized view. "
                        + "Please recreate the materialized view with the desired refresh method.");
            }
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    public void run() throws UserException {
        Env.getCurrentEnv().alterMTMVRefreshInfo(this);
    }

    public MTMVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }
}
