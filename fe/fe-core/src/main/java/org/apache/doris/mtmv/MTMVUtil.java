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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class MTMVUtil {

    /**
     * get Table by BaseTableInfo
     *
     * @param baseTableInfo
     * @return
     * @throws AnalysisException
     */
    public static TableIf getTable(BaseTableInfo baseTableInfo) throws AnalysisException {
        TableIf table = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(baseTableInfo.getCtlId())
                .getDbOrAnalysisException(baseTableInfo.getDbId())
                .getTableOrAnalysisException(baseTableInfo.getTableId());
        return table;
    }

    public static MTMVRelatedTableIf getRelatedTable(BaseTableInfo baseTableInfo) {
        TableIf relatedTable = null;
        try {
            relatedTable = MTMVUtil.getTable(baseTableInfo);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(e.getMessage(), e);
        }
        if (!(relatedTable instanceof MTMVRelatedTableIf)) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "base table for partitioning only can be OlapTable or HMSTable");
        }
        return (MTMVRelatedTableIf) relatedTable;
    }

    public static MTMV getMTMV(long dbId, long mtmvId) throws DdlException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
        return (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
    }

    /**
     * if base tables of mtmv contains external table
     *
     * @param mtmv
     * @return
     */
    public static boolean mtmvContainsExternalTable(MTMV mtmv) {
        Set<BaseTableInfo> baseTables = mtmv.getRelation().getBaseTables();
        for (BaseTableInfo baseTableInfo : baseTables) {
            if (baseTableInfo.getCtlId() != InternalCatalog.INTERNAL_CATALOG_ID) {
                return true;
            }
        }
        return false;
    }

    /**
     * Convert LiteralExpr to second
     *
     * @param expr
     * @param dateFormatOptional
     * @return
     * @throws AnalysisException
     */
    public static long getExprTimeSec(org.apache.doris.analysis.LiteralExpr expr, Optional<String> dateFormatOptional)
            throws AnalysisException {
        if (expr instanceof org.apache.doris.analysis.MaxLiteral) {
            return Long.MAX_VALUE;
        }
        if (expr instanceof org.apache.doris.analysis.NullLiteral) {
            return Long.MIN_VALUE;
        }
        if (expr instanceof org.apache.doris.analysis.DateLiteral) {
            return ((org.apache.doris.analysis.DateLiteral) expr).unixTimestamp(TimeUtils.getTimeZone()) / 1000;
        }
        if (!dateFormatOptional.isPresent()) {
            throw new AnalysisException("expr is not DateLiteral and DateFormat is not present.");
        }
        String dateFormat = dateFormatOptional.get();
        Expression strToDate = DateTimeExtractAndTransform
                .strToDate(new VarcharLiteral(expr.getStringValue()), new VarcharLiteral(dateFormat));
        if (strToDate instanceof DateTimeV2Literal) {
            return ((IntegerLiteral) DateTimeExtractAndTransform
                    .unixTimestamp((DateTimeV2Literal) strToDate)).getValue();
        } else if (strToDate instanceof DateV2Literal) {
            return ((IntegerLiteral) DateTimeExtractAndTransform
                    .unixTimestamp((DateV2Literal) strToDate)).getValue();
        } else {
            throw new AnalysisException(
                    String.format("strToDate failed, stringValue: %s, dateFormat: %s",
                            expr.getStringValue(), dateFormat));
        }
    }

    public static boolean allowModifyMTMVData(ConnectContext ctx) {
        if (ctx == null) {
            return false;
        }
        return ctx.getSessionVariable().isAllowModifyMaterializedViewData();
    }

    public static void checkModifyMTMVData(Database db, List<Long> tableIdList, ConnectContext ctx)
            throws AnalysisException {
        if (CollectionUtils.isEmpty(tableIdList)) {
            return;
        }
        for (long tableId : tableIdList) {
            Optional<Table> table = db.getTable(tableId);
            if (table.isPresent() && table.get() instanceof MTMV && !MTMVUtil.allowModifyMTMVData(ctx)) {
                throw new AnalysisException("Not allowed to perform current operation on async materialized view");
            }
        }
    }
}
