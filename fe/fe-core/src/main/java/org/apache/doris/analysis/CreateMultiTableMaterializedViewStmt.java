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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateMultiTableMaterializedViewStmt extends CreateTableStmt {
    private final String mvName;
    private final MVRefreshInfo.BuildMode buildMode;
    private final MVRefreshInfo refreshInfo;
    private final QueryStmt queryStmt;
    private Database database;
    private final Map<String, OlapTable> olapTables = Maps.newHashMap();

    public CreateMultiTableMaterializedViewStmt(String mvName, MVRefreshInfo.BuildMode buildMode,
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
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        refreshInfo.analyze(analyzer);
        queryStmt.analyze(analyzer);
        if (queryStmt instanceof SelectStmt) {
            analyzeSelectClause((SelectStmt) queryStmt);
        }
        tableName = new TableName(null, database.getFullName(), mvName);
        if (partitionDesc != null) {
            ((ColumnPartitionDesc) partitionDesc).analyze(analyzer, this);
        }
        super.analyze(analyzer);
    }

    private void analyzeSelectClause(SelectStmt selectStmt) throws AnalysisException, DdlException {
        for (TableRef tableRef : selectStmt.getTableRefs()) {
            String dbName = tableRef.getName().getDb();
            if (database == null) {
                database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
            } else if (!dbName.equals(database.getFullName())) {
                throw new AnalysisException("The databases of multiple tables must be the same.");
            }
            OlapTable table = (OlapTable) database.getTableOrAnalysisException(tableRef.getName().getTbl());
            olapTables.put(table.getName(), table);
        }
        columnDefs = generateColumnDefinitions(selectStmt.getSelectList());
    }

    private List<ColumnDef> generateColumnDefinitions(SelectList selectList) throws AnalysisException, DdlException {
        List<MVColumnItem> mvColumnItems = generateMVColumnItems(olapTables, selectList);
        List<Column> schema = generateSchema(mvColumnItems);
        return schema.stream()
                .map(column -> new ColumnDef(
                        column.getName(),
                        new TypeDef(column.getType()),
                        column.isKey(),
                        null,
                        column.isAllowNull(),
                        new DefaultValue(column.getDefaultValue() != null, column.getDefaultValue()),
                        column.getComment())
                ).collect(Collectors.toList());
    }

    private List<Column> generateSchema(List<MVColumnItem> mvColumnItems) throws DdlException {
        List<Column> columns = Lists.newArrayList();
        for (MVColumnItem mvColumnItem : mvColumnItems) {
            OlapTable olapTable = olapTables.get(mvColumnItem.getBaseTableName());
            columns.add(mvColumnItem.toMVColumn(olapTable));
        }
        return columns;
    }

    private List<MVColumnItem> generateMVColumnItems(Map<String, OlapTable> olapTables, SelectList selectList)
            throws AnalysisException {
        Map<String, MVColumnItem> uniqueMVColumnItems = Maps.newLinkedHashMap();
        for (SelectListItem item : selectList.getItems()) {
            MVColumnItem mvColumnItem = generateMVColumnItem(item);
            if (uniqueMVColumnItems.put(mvColumnItem.getName(), mvColumnItem) != null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, mvColumnItem.getName());
            }
        }
        return Lists.newArrayList(uniqueMVColumnItems.values().iterator());
    }

    private MVColumnItem generateMVColumnItem(SelectListItem item) {
        Expr itemExpr = item.getExpr();
        MVColumnItem mvColumnItem = null;
        if (itemExpr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) itemExpr;
            String alias = item.getAlias();
            String name = (alias != null) ? alias.toLowerCase() : slotRef.getColumnName().toLowerCase();
            mvColumnItem = new MVColumnItem(
                    name,
                    slotRef.getType(),
                    slotRef.getColumn().getAggregationType(),
                    slotRef.getColumn().isAggregationTypeImplicit(),
                    null,
                    slotRef.getColumnName(),
                    slotRef.getTableName().getTbl()
            );
        }
        return mvColumnItem;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW ").append(mvName).append(" BUILD ").append(buildMode.toString());
        if (refreshInfo != null) {
            sb.append(" ").append(refreshInfo.toString());
        }
        if (partitionDesc != null) {
            sb.append(" ").append(partitionDesc.toString());
        }
        if (distributionDesc != null) {
            sb.append(" ").append(distributionDesc.toString());
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

    public Database getDatabase() {
        return database;
    }

    public Map<String, OlapTable> getOlapTables() {
        return olapTables;
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
