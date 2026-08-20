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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Locale;

/**
 * Command for triggering compaction on one tablet.
 */
public class AdminCompactTabletCommand extends Command implements ForwardWithSync {
    private enum CompactionType {
        CUMULATIVE("cumulative"),
        BASE("base"),
        FULL("full");

        private final String value;

        CompactionType(String value) {
            this.value = value;
        }

        private static CompactionType fromString(String compactionType) throws AnalysisException {
            try {
                return valueOf(compactionType.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new AnalysisException("Where clause should looks like: type = 'BASE/CUMULATIVE/FULL'");
            }
        }
    }

    private final long tabletId;
    private final String compactionType;
    private CompactionType typeFilter;

    public AdminCompactTabletCommand(long tabletId, String compactionType) {
        super(PlanType.ADMIN_COMPACT_TABLET_COMMAND);
        this.tabletId = tabletId;
        this.compactionType = compactionType;
    }

    public long getTabletId() {
        return tabletId;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().compactTablet(tabletId, typeFilter.value);
    }

    private void validate(ConnectContext ctx) throws UserException {
        validateTablet(ctx);
        typeFilter = CompactionType.fromString(compactionType);
    }

    private void validateTablet(ConnectContext ctx) throws UserException {
        TabletMeta tabletMeta = Env.getCurrentInvertedIndex().getTabletMeta(tabletId);
        if (tabletMeta == null) {
            throw new AnalysisException("Unknown tablet: " + tabletId);
        }

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbNullable(tabletMeta.getDbId());
        if (db == null) {
            throw new AnalysisException("Unknown database for tablet: " + tabletId);
        }
        Table table = db.getTableNullable(tabletMeta.getTableId());
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("Unknown OLAP table for tablet: " + tabletId);
        }

        boolean hasGlobalAdmin = Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ctx, PrivPredicate.ADMIN);
        boolean hasTableAlter = Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx,
                InternalCatalog.INTERNAL_CATALOG_NAME, db.getFullName(), table.getName(), PrivPredicate.ALTER);
        if (!hasGlobalAdmin && !hasTableAlter) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCompactTabletCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ADMIN;
    }
}
