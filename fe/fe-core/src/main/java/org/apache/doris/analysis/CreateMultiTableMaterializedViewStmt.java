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

package org.apache.doris.analysis;

import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.MVRefreshInfo.BuildMode;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class CreateMultiTableMaterializedViewStmt extends CreateTableStmt {
    private final String mvName;
    private final BuildMode buildMode;
    private final MVRefreshInfo refreshInfo;
    private final QueryStmt queryStmt;
    private final Map<String, TableIf> tables = Maps.newHashMap();

    public CreateMultiTableMaterializedViewStmt(String mvName, BuildMode buildMode,
            MVRefreshInfo refreshInfo, KeysDesc keyDesc, PartitionDesc partitionDesc, DistributionDesc distributionDesc,
            Map<String, String> properties, QueryStmt queryStmt) {
        this.mvName = mvName;
        this.buildMode = buildMode;
        this.refreshInfo = refreshInfo;
        this.queryStmt = queryStmt;

        this.keysDesc = keyDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        engineName = DEFAULT_ENGINE_NAME;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (true) {
            throw new UserException("Multi table materialized view was not graduated.");
        }
        refreshInfo.analyze(analyzer);
        queryStmt.setNeedToSql(true);
        queryStmt.setToSQLWithHint(true);
        queryStmt.analyze(analyzer);
        if (queryStmt instanceof SelectStmt) {
            analyzeSelectClause((SelectStmt) queryStmt);
        }
        String defaultDb = analyzer.getDefaultDb();
        if (Strings.isNullOrEmpty(defaultDb)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }

        tableName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, defaultDb, mvName);
        if (partitionDesc != null) {
            ((ColumnPartitionDesc) partitionDesc).analyze(analyzer, this);
        }
        super.analyze(analyzer);
    }

    private void analyzeSelectClause(SelectStmt selectStmt) throws AnalysisException {
        for (TableRef tableRef : selectStmt.getTableRefs()) {
            TableIf table = null;
            if (tableRef instanceof BaseTableRef) {
                String dbName = tableRef.getName().getDb();
                String ctlName = tableRef.getName().getCtl();
                CatalogIf catalogIf = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctlName);
                if (catalogIf == null) {
                    throw new AnalysisException("Failed to get the catalog: " + ctlName);
                }
                Optional<DatabaseIf> dbIf = catalogIf.getDb(dbName);
                if (!dbIf.isPresent()) {
                    throw new AnalysisException("Failed to get the database: " + dbName);
                }
                Optional<TableIf> tableIf = dbIf.get().getTable(tableRef.getName().getTbl());
                if (!tableIf.isPresent()) {
                    throw new AnalysisException("Failed to get the table: " + tableRef.getName().getTbl());
                }
                table = tableIf.orElse(null);
            } else if (tableRef instanceof InlineViewRef) {
                InlineViewRef inlineViewRef = (InlineViewRef) tableRef;
                table = inlineViewRef.getDesc().getTable();
            }
            if (table == null) {
                throw new AnalysisException("Failed to get the table by " + tableRef);
            }
            tables.put(table.getName(), table);
            tables.put(tableRef.getAlias(), table);
        }
        checkSelectListItems(selectStmt.getSelectList().getItems());
        columnDefs = generateColumnDefinitions(selectStmt);
    }

    private void checkSelectListItems(List<SelectListItem> items) throws AnalysisException {
        for (SelectListItem item : items) {
            if (item.isStar()) {
                continue;
            }
            Expr itemExpr = item.getExpr();
            String alias = item.getAlias();
            if (itemExpr instanceof SlotRef) {
                continue;
            } else if (itemExpr instanceof FunctionCallExpr && ((FunctionCallExpr) itemExpr).isAggregateFunction()) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) itemExpr;
                String functionName = functionCallExpr.getFnName().getFunction();
                MVColumnPattern mvColumnPattern = CreateMaterializedViewStmt.FN_NAME_TO_PATTERN
                        .get(functionName.toLowerCase());
                if (mvColumnPattern == null) {
                    throw new AnalysisException(
                            "Materialized view does not support this function:" + functionCallExpr.toSqlImpl());
                }
                if (!mvColumnPattern.match(functionCallExpr)) {
                    throw new AnalysisException(
                            "The function " + functionName + " must match pattern:" + mvColumnPattern);
                }
                if (StringUtils.isEmpty(alias)) {
                    throw new AnalysisException("Function expr: " + functionName + " must have a alias name for MTMV.");
                }
            } else {
                throw new AnalysisException(
                        "Materialized view does not support this expr:" + itemExpr.toSqlImpl());
            }
        }
    }

    private List<ColumnDef> generateColumnDefinitions(SelectStmt selectStmt) throws AnalysisException {
        List<Column> schema = generateSchema(selectStmt);
        return schema.stream()
                .map(column -> new ColumnDef(
                        column.getName(),
                        new TypeDef(column.getType()),
                        column.isKey(),
                        null,
                        column.isAllowNull(),
                        column.isAutoInc(),
                        new DefaultValue(column.getDefaultValue() != null, column.getDefaultValue()),
                        column.getComment())
                ).collect(Collectors.toList());
    }

    private List<Column> generateSchema(SelectStmt selectStmt) throws AnalysisException {
        ArrayList<Expr> resultExprs = selectStmt.getResultExprs();
        ArrayList<String> colLabels = selectStmt.getColLabels();
        Map<String, Column> uniqueMVColumnItems = Maps.newLinkedHashMap();
        for (int i = 0; i < resultExprs.size(); i++) {
            Column column = generateMTMVColumn(resultExprs.get(i), colLabels.get(i));
            if (uniqueMVColumnItems.put(column.getName(), column) != null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, column.getName());
            }
        }

        return Lists.newArrayList(uniqueMVColumnItems.values());
    }

    private Column generateMTMVColumn(Expr expr, String colLabel) {
        return new Column(colLabel.toLowerCase(), expr.getType(), true);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW ").append(mvName).append(" BUILD ").append(buildMode.toString());
        if (refreshInfo != null) {
            sb.append(" ").append(refreshInfo);
        }
        if (partitionDesc != null) {
            sb.append(" ").append(partitionDesc);
        }
        if (distributionDesc != null) {
            sb.append(" ").append(distributionDesc);
        }
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<>(properties, " = ", true, true, true));
            sb.append(")");
        }
        sb.append(" AS ").append(queryStmt.toSql());
        return sb.toString();
    }

    public String getMVName() {
        return mvName;
    }

    public Map<String, TableIf> getTables() {
        return tables;
    }

    public MVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    public QueryStmt getQueryStmt() {
        return queryStmt;
    }

    public BuildMode getBuildMode() {
        return buildMode;
    }
}
