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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

// SHOW COLUMNS
public class ShowColumnStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Field", ScalarType.createVarchar(20)))
            .addColumn(new Column("Type", ScalarType.createVarchar(20)))
            .addColumn(new Column("Null", ScalarType.createVarchar(20)))
            .addColumn(new Column("Key", ScalarType.createVarchar(20)))
            .addColumn(new Column("Default", ScalarType.createVarchar(20)))
            .addColumn(new Column("Extra", ScalarType.createVarchar(20))).build();

    private static final ShowResultSetMetaData META_DATA_VERBOSE =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Privileges", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(20)))
                    .build();

    private ShowResultSetMetaData metaData;
    private TableName tableName;
    private String db;
    private String pattern;
    private boolean isVerbose;
    private SelectStmt selectStmt;
    private Expr where;

    public ShowColumnStmt(TableName tableName, String db, String pattern, boolean isVerbose) {
        this.tableName = tableName;
        this.db = db;
        this.pattern = pattern;
        this.isVerbose = isVerbose;
    }

    public ShowColumnStmt(TableName tableName, String db, String pattern, boolean isVerbose, Expr where) {
        this.tableName = tableName;
        this.db = db;
        this.pattern = pattern;
        this.isVerbose = isVerbose;
        this.where = where;
    }

    public String getCtl() {
        return tableName.getCtl();
    }

    public String getDb() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!Strings.isNullOrEmpty(db)) {
            tableName.setDb(db);
        }
        tableName.analyze(analyzer);
        if (isVerbose) {
            metaData = META_DATA_VERBOSE;
        } else {
            metaData = META_DATA;
        }
    }

    @Override
    public SelectStmt toSelectStmt(Analyzer analyzer) throws AnalysisException {
        if (where == null) {
            return null;
        }
        if (selectStmt != null) {
            return selectStmt;
        }
        analyze(analyzer);
        TableName columnsTableName = new TableName(tableName.getCtl(), InfoSchemaDb.DATABASE_NAME, "columns");
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap();
        // Field
        SelectListItem item = new SelectListItem(new SlotRef(columnsTableName, "COLUMN_NAME"), "Field");
        selectList.addItem(item);
        aliasMap.putNoAnalyze(new SlotRef(null, "Field"), item.getExpr().clone(null));
        // Type
        item = new SelectListItem(new SlotRef(columnsTableName, "DATA_TYPE"), "Type");
        selectList.addItem(item);
        aliasMap.putNoAnalyze(new SlotRef(null, "Type"), item.getExpr().clone(null));
        // Collation
        if (isVerbose) {
            item = new SelectListItem(new SlotRef(columnsTableName, "COLLATION_NAME"), "Collation");
            selectList.addItem(item);
            aliasMap.putNoAnalyze(new SlotRef(null, "Collation"), item.getExpr().clone(null));
        }
        // Null
        item = new SelectListItem(new SlotRef(columnsTableName, "IS_NULLABLE"), "Null");
        selectList.addItem(item);
        aliasMap.putNoAnalyze(new SlotRef(null, "Null"), item.getExpr().clone(null));
        // Key
        item = new SelectListItem(new SlotRef(columnsTableName, "COLUMN_KEY"), "Key");
        selectList.addItem(item);
        aliasMap.putNoAnalyze(new SlotRef(null, "Key"), item.getExpr().clone(null));
        // Default
        item = new SelectListItem(new SlotRef(columnsTableName, "COLUMN_DEFAULT"), "Default");
        selectList.addItem(item);
        aliasMap.putNoAnalyze(new SlotRef(null, "Default"), item.getExpr().clone(null));
        // Extra
        item = new SelectListItem(new SlotRef(columnsTableName, "EXTRA"), "Extra");
        selectList.addItem(item);
        aliasMap.putNoAnalyze(new SlotRef(null, "Extra"), item.getExpr().clone(null));
        if (isVerbose) {
            // Privileges
            item = new SelectListItem(new SlotRef(columnsTableName, "PRIVILEGES"), "Privileges");
            selectList.addItem(item);
            aliasMap.putNoAnalyze(new SlotRef(null, "Privileges"), item.getExpr().clone(null));
            // Comment
            item = new SelectListItem(new SlotRef(columnsTableName, "COLUMN_COMMENT"), "Comment");
            selectList.addItem(item);
            aliasMap.putNoAnalyze(new SlotRef(null, "Comment"), item.getExpr().clone(null));
        }

        where = where.substitute(aliasMap);
        selectStmt = new SelectStmt(selectList,
                new FromClause(Lists.newArrayList(new TableRef(columnsTableName, null))),
                where, null, null, null, LimitElement.NO_LIMIT);
        analyzer.setSchemaInfo(tableName.getDb(), tableName.getTbl(), tableName.getCtl());

        return selectStmt;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return metaData;
    }
}
