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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Show variables statement.
public class ShowVariablesStmt extends ShowStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(ShowVariablesStmt.class);
    private static final String NAME_COL = "Variable_name";
    private static final String VALUE_COL = "Value";
    private static final String DEFAULT_VALUE_COL = "Default_Value";
    private static final String CHANGED_COL = "Changed";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(NAME_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(VALUE_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(DEFAULT_VALUE_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(CHANGED_COL, ScalarType.createVarchar(20)))
                    .build();

    private SetType type;
    private String pattern;
    private Expr where;
    private SelectStmt selectStmt;

    public ShowVariablesStmt(SetType type, String pattern) {
        this.type = type;
        this.pattern = pattern;
    }

    public ShowVariablesStmt(SetType type, String pattern, Expr where) {
        this.type = type;
        this.pattern = pattern;
        this.where = where;
    }

    public SetType getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) {
        if (type == null) {
            type = SetType.DEFAULT;
        }
    }

    @Override
    public SelectStmt toSelectStmt(Analyzer analyzer) {
        if (where == null) {
            return null;
        }
        if (selectStmt != null) {
            return selectStmt;
        }
        analyze(analyzer);
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        TableName tableName = null;
        if (type == SetType.GLOBAL) {
            tableName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, InfoSchemaDb.DATABASE_NAME,
                    "GLOBAL_VARIABLES");
        } else {
            tableName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, InfoSchemaDb.DATABASE_NAME,
                    "SESSION_VARIABLES");
        }
        // name
        SelectListItem item = new SelectListItem(new SlotRef(tableName, "VARIABLE_NAME"), NAME_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, NAME_COL), item.getExpr().clone(null));
        // value
        item = new SelectListItem(new SlotRef(tableName, "VARIABLE_VALUE"), VALUE_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, VALUE_COL), item.getExpr().clone(null));
        // change
        where = where.substitute(aliasMap);
        selectStmt = new SelectStmt(selectList,
                new FromClause(Lists.newArrayList(new TableRef(tableName, null))),
                where, null, null, null, LimitElement.NO_LIMIT);
        if (LOG.isDebugEnabled()) {
            LOG.debug("select stmt is {}", selectStmt.toSql());
        }

        // DB: type
        // table: thread id
        analyzer.setSchemaInfo(null, null, null);
        return selectStmt;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW ");
        sb.append(type.toString()).append(" VARIABLES");
        if (pattern != null) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
