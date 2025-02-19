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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AnalyzeTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * AnalyzeCommand
 */
public class AnalyzeTableCommand extends AnalyzeCommand {
    private static final Logger LOG = LogManager.getLogger(AnalyzeTableCommand.class);

    AnalyzeTableOp analyzeTableOp;

    /**
     * AnalyzeCommand
     */
    public AnalyzeTableCommand(AnalyzeTableOp analyzeTableOp) {
        super(PlanType.ANALYZE_TABLE, analyzeTableOp.getAnalyzeProperties());
        this.analyzeTableOp = analyzeTableOp;
    }

    private void validate(ConnectContext ctx) throws Exception {
        TableNameInfo tableNameInfo = analyzeTableOp.getTableNameInfo();
        tableNameInfo.analyze(ctx);
        String catalogName = tableNameInfo.getCtl();
        String dbName = tableNameInfo.getDb();
        String tblName = tableNameInfo.getTbl();
        CatalogIf catalog = ctx.getEnv().getCatalogMgr().getCatalogOrAnalysisException(catalogName);
        analyzeTableOp.setCatalogId(catalog.getId());
        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        analyzeTableOp.setDbId(db.getId());
        analyzeTableOp.setTable(db.getTableOrAnalysisException(tblName));
        check();
    }

    public AnalyzeTableOp getAnalyzeTableOp() {
        return analyzeTableOp;
    }

    /**
     * check
     */
    public void check() throws AnalysisException {
        if (analyzeTableOp.getTable() instanceof View) {
            throw new AnalysisException("Analyze view is not allowed");
        }
        checkAnalyzePrivilege(analyzeTableOp.getTableNameInfo());
        if (analyzeTableOp.getColumnNames() == null) {
            analyzeTableOp.setColumnNames(analyzeTableOp.getTable().getSchemaAllIndexes(false).stream()
                    // Filter unsupported type columns.
                    .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                    .map(Column::getName)
                    .collect(Collectors.toList()));
        } else {
            analyzeTableOp.getTable().readLock();
            try {
                List<String> baseSchema = analyzeTableOp.getTable().getSchemaAllIndexes(false)
                        .stream().map(Column::getName).collect(Collectors.toList());
                Optional<String> optional = analyzeTableOp.getColumnNames().stream()
                        .filter(entity -> !baseSchema.contains(entity)).findFirst();
                if (optional.isPresent()) {
                    String columnName = optional.get();
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR,
                            columnName, analyzeTableOp.getTableNameInfo().getTbl());
                }
                checkColumn();
            } finally {
                analyzeTableOp.getTable().readUnlock();
            }
        }
        analyzeProperties.check();

        if (analyzeProperties.isSync()
                && (analyzeProperties.isAutomatic() || analyzeProperties.getPeriodTimeInMs() != 0)) {
            throw new AnalysisException("Automatic/Period statistics collection "
                    + "and synchronous statistics collection cannot be set at same time");
        }
        if (analyzeProperties.isAutomatic() && analyzeProperties.getPeriodTimeInMs() != 0) {
            throw new AnalysisException("Automatic collection "
                    + "and period statistics collection cannot be set at same time");
        }
        if (analyzeProperties.isSample() && analyzeProperties.forceFull()) {
            throw new AnalysisException("Impossible to analyze with sample and full simultaneously");
        }
    }

    private void checkAnalyzePrivilege(TableNameInfo tableNameInfo) throws AnalysisException {
        ConnectContext ctx = ConnectContext.get();
        // means it a system analyze
        if (ctx == null) {
            return;
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, tableNameInfo.getCtl(), tableNameInfo.getDb(),
                    tableNameInfo.getTbl(), PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableNameInfo.getDb() + ": " + tableNameInfo.getTbl());
        }
    }

    private void checkColumn() throws AnalysisException {
        boolean containsUnsupportedTytpe = false;
        for (String colName : analyzeTableOp.getColumnNames()) {
            Column column = analyzeTableOp.getTable() instanceof OlapTable
                    ? ((OlapTable) analyzeTableOp.getTable()).getVisibleColumn(colName)
                    : analyzeTableOp.getTable().getColumn(colName);
            if (column == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                        colName, FeNameFormat.getColumnNameRegex());
            }
            if (StatisticsUtil.isUnsupportedType(column.getType())) {
                containsUnsupportedTytpe = true;
            }
        }
        if (containsUnsupportedTytpe) {
            if (ConnectContext.get() == null
                    || !ConnectContext.get().getSessionVariable().enableAnalyzeComplexTypeColumn) {
                analyzeTableOp.setColumnNames(analyzeTableOp.getColumnNames().stream()
                        .filter(c -> !StatisticsUtil.isUnsupportedType(analyzeTableOp.getTable() instanceof OlapTable
                                ? ((OlapTable) analyzeTableOp.getTable()).getVisibleColumn(c).getType()
                                : analyzeTableOp.getTable().getColumn(c).getType()))
                        .collect(Collectors.toList()));
            } else {
                throw new AnalysisException(
                        "Contains unsupported column type"
                                + "if you want to ignore them and analyze rest"
                                + "columns, please set session variable "
                                + "`ignore_column_with_complex_type` to true");
            }
        }
    }

    /**
     * checkAndSetSample
     */
    public void checkAndSetSample() throws AnalysisException {
        if (analyzeTableOp.getAnalyzeProperties().forceFull()) {
            // if the user trys hard to do full, we stop him hard.
            throw new AnalysisException(
                    "analyze with full is forbidden for performance issue in cloud mode, use `with sample` then");
        }
        if (!analyzeTableOp.getAnalyzeProperties().isSample()) {
            // otherwise, we gently translate it to use sample
            LOG.warn("analyze with full is forbidden for performance issue in cloud mode, force to use sample");
            analyzeTableOp.getAnalyzeProperties().setSampleRows(StatisticsUtil.getHugeTableSampleRows());
        }
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().analyze(this, executor.isProxy());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE TABLE ");

        if (analyzeTableOp.getTableNameInfo() != null) {
            sb.append(" ");
            sb.append(analyzeTableOp.getTableNameInfo().toSql());
        }

        if (analyzeTableOp.getTableNameInfo() != null) {
            sb.append("(");
            sb.append(StringUtils.join(analyzeTableOp.getColumnNames(), ","));
            sb.append(")");
        }

        if (analyzeTableOp.getAnalyzeProperties().getAnalysisType().equals(AnalysisInfo.AnalysisType.HISTOGRAM)) {
            sb.append(" ");
            sb.append("UPDATE HISTOGRAM");
        }

        if (analyzeProperties != null) {
            sb.append(" ");
            sb.append(analyzeProperties.toSQL());
        }

        return sb.toString();
    }
}
