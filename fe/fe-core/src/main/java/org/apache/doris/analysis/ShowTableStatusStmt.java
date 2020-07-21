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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

// SHOW TABLE STATUS
public class ShowTableStatusStmt extends ShowStmt {
    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "tables");
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Engine", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Version", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Row_format", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Rows", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Avg_row_length", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Data_length", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Max_data_length", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Index_length", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Data_free", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Auto_increment", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Create_time", ScalarType.createType(PrimitiveType.DATETIME)))
                    .addColumn(new Column("Update_time", ScalarType.createType(PrimitiveType.DATETIME)))
                    .addColumn(new Column("Check_time", ScalarType.createType(PrimitiveType.DATETIME)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Checksum", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Create_options", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(64)))
                    .build();

    private String db;
    private String wild;
    private Expr where;
    private SelectStmt selectStmt;

    public ShowTableStatusStmt(String db, String wild, Expr where) {
        this.db = db;
        this.wild = wild;
        this.where = where;
    }

    public String getDb() {
        return db;
    }

    public String getPattern() {
        return wild;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(db)) {
            db = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            db = ClusterNamespace.getFullName(analyzer.getClusterName(), db);
        }
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), db, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getQualifiedUser(), db);
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

        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        // Name
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_NAME"), "Name");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Name"), item.getExpr().clone(null));
        // Engine
        item = new SelectListItem(new SlotRef(TABLE_NAME, "ENGINE"), "Engine");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Engine"), item.getExpr().clone(null));
        // Version
        item = new SelectListItem(new SlotRef(TABLE_NAME, "VERSION"), "Version");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Version"), item.getExpr().clone(null));
        // Version
        item = new SelectListItem(new SlotRef(TABLE_NAME, "ROW_FORMAT"), "Row_format");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Row_format"), item.getExpr().clone(null));
        // Rows
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_ROWS"), "Rows");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Rows"), item.getExpr().clone(null));
        // Avg_row_length
        item = new SelectListItem(new SlotRef(TABLE_NAME, "AVG_ROW_LENGTH"), "Avg_row_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Avg_row_length"), item.getExpr().clone(null));
        // Data_length
        item = new SelectListItem(new SlotRef(TABLE_NAME, "DATA_LENGTH"), "Data_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Data_length"), item.getExpr().clone(null));
        // Max_data_length
        item = new SelectListItem(new SlotRef(TABLE_NAME, "MAX_DATA_LENGTH"), "Max_data_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Max_data_length"), item.getExpr().clone(null));
        // Index_length
        item = new SelectListItem(new SlotRef(TABLE_NAME, "INDEX_LENGTH"), "Index_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Index_length"), item.getExpr().clone(null));
        // Data_free
        item = new SelectListItem(new SlotRef(TABLE_NAME, "DATA_FREE"), "Data_free");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Data_free"), item.getExpr().clone(null));
        // Data_free
        item = new SelectListItem(new SlotRef(TABLE_NAME, "AUTO_INCREMENT"), "Auto_increment");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Auto_increment"), item.getExpr().clone(null));
        // Create_time
        item = new SelectListItem(new SlotRef(TABLE_NAME, "CREATE_TIME"), "Create_time");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Create_time"), item.getExpr().clone(null));
        // Update_time
        item = new SelectListItem(new SlotRef(TABLE_NAME, "UPDATE_TIME"), "Update_time");
        selectList.addItem(item);
        // Check_time
        item = new SelectListItem(new SlotRef(TABLE_NAME, "CHECK_TIME"), "Check_time");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Check_time"), item.getExpr().clone(null));
        // Collation
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_COLLATION"), "Collation");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Collation"), item.getExpr().clone(null));
        // Checksum
        item = new SelectListItem(new SlotRef(TABLE_NAME, "CHECKSUM"), "Checksum");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Checksum"), item.getExpr().clone(null));
        // Create_options
        item = new SelectListItem(new SlotRef(TABLE_NAME, "CREATE_OPTIONS"), "Create_options");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Create_options"), item.getExpr().clone(null));
        // Comment
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_COMMENT"), "Comment");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Comment"), item.getExpr().clone(null));

        where = where.substitute(aliasMap);
        selectStmt = new SelectStmt(selectList,
                new FromClause(Lists.newArrayList(new TableRef(TABLE_NAME, null))),
                where, null, null, null, LimitElement.NO_LIMIT);
        analyzer.setSchemaInfo(db, null, null);

        return selectStmt;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
