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
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * alter mtmv add column info.
 * Grammar: ALTER MATERIALIZED VIEW mv ADD COLUMN colName AS expression.
 * The new column type is fully inferred from the expression, users do NOT specify a type.
 */
public class AlterMTMVAddColumnInfo extends AlterMTMVInfo {
    private final String columnName;
    private final String exprSql;
    private ColumnDefinition analyzedColumn;
    private String rewrittenQuerySql;

    public AlterMTMVAddColumnInfo(TableNameInfo mvName, String columnName, String exprSql) {
        super(mvName);
        this.columnName = Objects.requireNonNull(columnName, "require columnName");
        this.exprSql = Objects.requireNonNull(exprSql, "require exprSql object");
    }

    @Override
    public void analyze(ConnectContext ctx) throws AnalysisException {
        super.analyze(ctx);
        if (StringUtils.isBlank(columnName)) {
            throw new AnalysisException("ADD COLUMN must specify a column name");
        }
        if (StringUtils.isBlank(exprSql)) {
            throw new AnalysisException("ADD COLUMN must specify expression by `AS expression`");
        }
        try {
            MTMV mtmv = (MTMV) Env.getCurrentInternalCatalog().getDbOrAnalysisException(mvName.getDb())
                    .getTableOrDdlException(mvName.getTbl(), TableType.MATERIALIZED_VIEW);
            if (mtmv.getColumn(columnName) != null) {
                throw new AnalysisException("Column already exists: " + columnName);
            }
            inferColumnFromExpression(mtmv, ctx);
        } catch (DdlException | org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    private void inferColumnFromExpression(MTMV mtmv, ConnectContext ctx) {
        rewrittenQuerySql = MTMV.rewriteQuerySqlForAddColumn(mtmv.getQuerySql(), exprSql, columnName);
        List<ColumnDefinition> allColumns = MTMVPlanUtil.generateColumnsBySql(
                rewrittenQuerySql,
                ctx,
                mtmv.getMvPartitionInfo() == null ? null : mtmv.getMvPartitionInfo().getPartitionCol(),
                mtmv.getDistributionColumnNames(),
                null,
                mtmv.getTableProperty() == null ? null : mtmv.getTableProperty().getProperties());
        if (CollectionUtils.isEmpty(allColumns) || allColumns.size() != mtmv.getBaseSchema().size() + 1) {
            throw new AnalysisException("failed to infer added column by expression");
        }
        analyzedColumn = allColumns.get(allColumns.size() - 1);
    }

    @Override
    public void run() throws UserException {
        Env.getCurrentEnv().alterMTMVAddColumnInfo(this);
    }

    public ColumnDefinition getColumn() {
        return analyzedColumn;
    }

    public String getExprSql() {
        return exprSql;
    }

    public String getRewrittenQuerySql() {
        return rewrittenQuerySql;
    }
}
