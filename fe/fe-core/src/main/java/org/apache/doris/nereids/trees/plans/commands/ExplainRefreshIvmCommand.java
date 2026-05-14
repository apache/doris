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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.mtmv.ivm.IvmRefreshExplainResult;
import org.apache.doris.mtmv.ivm.IvmRefreshManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;
import java.util.Objects;

/**
 * Explain IVM refresh dry-run plans.
 */
public class ExplainRefreshIvmCommand extends Command implements NoForward {
    private static final ShowResultSetMetaData OVERVIEW_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Item", ScalarType.createVarchar(30)))
            .addColumn(new Column("Delta", ScalarType.createVarchar(-1)))
            .addColumn(new Column("Value", ScalarType.createVarchar(-1)))
            .build();
    private static final ShowResultSetMetaData DELTA_PLAN_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("IVM Delta Plan", ScalarType.createVarchar(-1)))
            .build();

    private final RefreshMTMVInfo refreshMTMVInfo;
    private final ExplainLevel level;
    private final boolean showPlanProcess;
    private final Integer deltaId;

    /**
     * Creates an EXPLAIN REFRESH command for IVM refresh planning.
     */
    public ExplainRefreshIvmCommand(RefreshMTMVInfo refreshMTMVInfo, ExplainLevel level,
            boolean showPlanProcess, Integer deltaId) {
        super(PlanType.EXPLAIN_REFRESH_IVM_COMMAND);
        this.refreshMTMVInfo = Objects.requireNonNull(refreshMTMVInfo, "refreshMTMVInfo can not be null");
        this.level = Objects.requireNonNull(level, "level can not be null");
        this.showPlanProcess = showPlanProcess;
        this.deltaId = deltaId;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (showPlanProcess) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "EXPLAIN REFRESH does not support PLAN PROCESS");
        }
        if (refreshMTMVInfo.getRefreshMode() != RefreshMode.INCREMENTAL) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "EXPLAIN REFRESH only supports IVM materialized views");
        }
        if (deltaId == null && level != ExplainLevel.NORMAL) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "EXPLAIN " + level + " REFRESH requires FOR DELTA k");
        }
        if (deltaId != null && level != ExplainLevel.REWRITTEN_PLAN) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "EXPLAIN " + level + " REFRESH FOR DELTA is not supported yet; "
                            + "use EXPLAIN LOGICAL PLAN REFRESH ... FOR DELTA k");
        }

        refreshMTMVInfo.analyze(ctx);
        MTMV mtmv = getMtmv();
        IvmRefreshExplainResult result = createIvmRefreshManager().explainRefresh(mtmv);
        if (deltaId == null) {
            executor.sendResultSet(new ShowResultSet(OVERVIEW_META_DATA, result.formatOverviewRows()));
        } else {
            executor.sendResultSet(new ShowResultSet(DELTA_PLAN_META_DATA, result.formatDeltaPlanRows(deltaId)));
        }
    }

    private MTMV getMtmv() throws org.apache.doris.common.AnalysisException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(refreshMTMVInfo.getMvName().getDb());
        return (MTMV) db.getTableOrMetaException(refreshMTMVInfo.getMvName().getTbl(),
                TableType.MATERIALIZED_VIEW);
    }

    IvmRefreshManager createIvmRefreshManager() {
        return new IvmRefreshManager();
    }

    public RefreshMTMVInfo getRefreshMTMVInfo() {
        return refreshMTMVInfo;
    }

    public ExplainLevel getLevel() {
        return level;
    }

    public boolean showPlanProcess() {
        return showPlanProcess;
    }

    public Integer getDeltaId() {
        return deltaId;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExplainRefreshIvmCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.EXPLAIN;
    }
}
