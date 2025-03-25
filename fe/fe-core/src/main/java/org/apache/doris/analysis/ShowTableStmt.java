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
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// SHOW TABLES
public class ShowTableStmt extends ShowStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(ShowTableStmt.class);
    private static final String NAME_COL_PREFIX = "Tables_in_";
    private static final String TYPE_COL = "Table_type";
    private static final String STORAGE_FORMAT_COL = "Storage_format";
    private static final String INVERTED_INDEX_STORAGE_FORMAT_COL = "Inverted_index_storage_format";
    private String db;
    private String catalog;
    private final boolean isVerbose;
    private TableType type;
    private final String pattern;
    private Expr where;
    private SelectStmt selectStmt;

    public ShowTableStmt(String db, String catalog, boolean isVerbose, String pattern) {
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = null;
        this.catalog = catalog;
    }

    public ShowTableStmt(String db, String catalog, boolean isVerbose, String pattern, Expr where) {
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = where;
        this.catalog = catalog;
    }

    public ShowTableStmt(String db, String catalog, boolean isVerbose, TableType type, String pattern,
                         Expr where) {
        this(db, catalog, isVerbose, pattern, where);
        this.type = type;
    }

    public String getDb() {
        return db;
    }

    public String getCatalog() {
        return catalog;
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public TableType getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(db)) {
            db = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = analyzer.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_CATALOG);
            }
        }

        // we do not check db privs here. because user may not have any db privs,
        // but if it has privs of tbls inside this db,it should be allowed to see this db.
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
        TableName tablesTableName = new TableName(catalog, InfoSchemaDb.DATABASE_NAME, "tables");

        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        SelectListItem item = new SelectListItem(new SlotRef(tablesTableName, "TABLE_NAME"),
                NAME_COL_PREFIX + ClusterNamespace.getNameFromFullName(db));
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, NAME_COL_PREFIX + ClusterNamespace.getNameFromFullName(db)),
                item.getExpr().clone(null));
        if (isVerbose) {
            item = new SelectListItem(new SlotRef(tablesTableName, "TABLE_TYPE"), TYPE_COL);
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, TYPE_COL), item.getExpr().clone(null));
        }
        if (type != null) {
            BinaryPredicate viewFilter = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                    new SlotRef(tablesTableName, "ENGINE"), new StringLiteral(type.toEngineName()));
            where = CompoundPredicate.createConjunction(viewFilter, where);
        }
        where = where.substitute(aliasMap);
        selectStmt = new SelectStmt(selectList,
                new FromClause(Lists.newArrayList(new TableRef(tablesTableName, null))),
                where, null, null, null, LimitElement.NO_LIMIT);

        analyzer.setSchemaInfo(ClusterNamespace.getNameFromFullName(db), null, catalog);

        return selectStmt;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW");
        if (isVerbose) {
            sb.append(" FULL");
        }
        if (type != null) {
            switch (type) {
                // todo(only show views from now)
                case VIEW:
                    sb.append(" VIEWS");
                    break;
                default:
                    sb.append(" TABLES");
            }
        } else {
            sb.append(" TABLES");
        }
        if (!Strings.isNullOrEmpty(db)) {
            if (!Strings.isNullOrEmpty(catalog)) {
                sb.append(" FROM ").append(catalog);
                sb.append(".").append(ClusterNamespace.getNameFromFullName(db));
            } else {
                sb.append(" FROM ").append(db);
            }
        }
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
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(
                new Column(NAME_COL_PREFIX + ClusterNamespace.getNameFromFullName(db), ScalarType.createVarchar(20)));
        if (isVerbose) {
            builder.addColumn(new Column(TYPE_COL, ScalarType.createVarchar(20)));
            builder.addColumn(new Column(STORAGE_FORMAT_COL, ScalarType.createVarchar(20)));
            builder.addColumn(new Column(INVERTED_INDEX_STORAGE_FORMAT_COL, ScalarType.createVarchar(20)));
        }
        return builder.build();
    }
}
