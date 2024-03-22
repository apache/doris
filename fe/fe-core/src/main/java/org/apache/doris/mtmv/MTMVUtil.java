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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeAcquire;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
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
     * Obtain the minimum second from `syncLimit` `timeUnit` ago
     *
     * @param timeUnit
     * @param syncLimit
     * @return
     * @throws AnalysisException
     */
    public static long getNowTruncSubSec(MTMVPartitionSyncTimeUnit timeUnit, int syncLimit)
            throws AnalysisException {
        if (syncLimit < 1) {
            throw new AnalysisException("Unexpected syncLimit, syncLimit: " + syncLimit);
        }
        // get current time
        Expression now = DateTimeAcquire.now();
        if (!(now instanceof DateTimeLiteral)) {
            throw new AnalysisException("now() should return DateTimeLiteral, now: " + now);
        }
        DateTimeLiteral nowLiteral = (DateTimeLiteral) now;
        // date trunc
        now = DateTimeExtractAndTransform
                .dateTrunc(nowLiteral, new VarcharLiteral(timeUnit.name()));
        if (!(now instanceof DateTimeLiteral)) {
            throw new AnalysisException("dateTrunc() should return DateTimeLiteral, now: " + now);
        }
        nowLiteral = (DateTimeLiteral) now;
        // date sub
        if (syncLimit > 1) {
            nowLiteral = dateSub(nowLiteral, timeUnit, syncLimit - 1);
        }
        return ((IntegerLiteral) DateTimeExtractAndTransform.unixTimestamp(nowLiteral)).getValue();
    }

    private static DateTimeLiteral dateSub(
            org.apache.doris.nereids.trees.expressions.literal.DateLiteral date, MTMVPartitionSyncTimeUnit timeUnit,
            int num)
            throws AnalysisException {
        IntegerLiteral integerLiteral = new IntegerLiteral(num);
        Expression result;
        switch (timeUnit) {
            case DAY:
                result = DateTimeArithmetic.dateSub(date, integerLiteral);
                break;
            case YEAR:
                result = DateTimeArithmetic.yearsSub(date, integerLiteral);
                break;
            case MONTH:
                result = DateTimeArithmetic.monthsSub(date, integerLiteral);
                break;
            default:
                throw new AnalysisException("MTMV partition limit not support timeUnit: " + timeUnit.name());
        }
        if (!(result instanceof DateTimeLiteral)) {
            throw new AnalysisException("sub() should return  DateTimeLiteral, result: " + result);
        }
        return (DateTimeLiteral) result;
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

    /**
     * Generate MTMVPartitionSyncConfig based on mvProperties
     *
     * @param mvProperties
     * @return
     */
    public static MTMVPartitionSyncConfig generateMTMVPartitionSyncConfigByProperties(
            Map<String, String> mvProperties) {
        int syncLimit = StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT)) ? -1
                : Integer.parseInt(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT));
        MTMVPartitionSyncTimeUnit timeUnit =
                StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT))
                        ? MTMVPartitionSyncTimeUnit.DAY : MTMVPartitionSyncTimeUnit
                        .valueOf(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT).toUpperCase());
        Optional<String> dateFormat =
                StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT))
                        ? Optional.empty()
                        : Optional.of(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT));
        return new MTMVPartitionSyncConfig(syncLimit, timeUnit, dateFormat);
    }
}
