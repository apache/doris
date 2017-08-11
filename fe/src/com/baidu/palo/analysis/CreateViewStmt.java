// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.catalog.PrimitiveType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.List;
import java.util.Set;

public class CreateViewStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateViewStmt.class);

    private final boolean ifNotExists;
    private final TableName tableName;
    private final List<String> columnNames;
    private final QueryStmt viewDefStmt;

    // Set during analyze
    private final List<Column> finalCols;

    private String originalViewDef;
    private String inlineViewDef;
    private QueryStmt cloneStmt;

    public CreateViewStmt(boolean ifNotExists, TableName tableName, List<String> columnNames, QueryStmt queryStmt) {
        this.ifNotExists = ifNotExists;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.viewDefStmt = queryStmt;
        finalCols = Lists.newArrayList();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public List<Column> getColumns() {
        return finalCols;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    /**
     * Sets the originalViewDef and the expanded inlineViewDef based on viewDefStmt.
     * If columnNames were given, checks that they do not contain duplicate column names
     * and throws an exception if they do.
     */
    private void createColumnAndViewDefs(Analyzer analyzer) throws AnalysisException, InternalException {
        if (columnNames != null) {
            if (columnNames.size() != viewDefStmt.getColLabels().size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            // TODO(zc): type
            for (int i = 0; i < columnNames.size(); ++i) {
                PrimitiveType type = viewDefStmt.getBaseTblResultExprs().get(i).getType().getPrimitiveType();
                finalCols.add(new Column(
                        columnNames.get(i),
                        ColumnType.createType(type)));
            }
        } else {
            // TODO(zc): type
            for (int i = 0; i < viewDefStmt.getBaseTblResultExprs().size(); ++i) {
                PrimitiveType type = viewDefStmt.getBaseTblResultExprs().get(i).getType().getPrimitiveType();
                finalCols.add(new Column(
                        viewDefStmt.getColLabels().get(i),
                        ColumnType.createType(type)));
            }
        }
        // Set for duplicate columns
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }

        // format view def string
        originalViewDef = viewDefStmt.toSql();

        if (columnNames == null) {
            inlineViewDef = originalViewDef;
            return;
        }

        Analyzer tmpAnalyzer = new Analyzer(analyzer);
        cloneStmt.substituteSelectList(tmpAnalyzer, columnNames);
        inlineViewDef = cloneStmt.toSql();

        // StringBuilder sb = new StringBuilder();
        // sb.append("SELECT ");
        // for (int i = 0; i < columnNames.size(); ++i) {
        //     if (i != 0) {
        //         sb.append(", ");
        //     }
        //     String colRef = viewDefStmt.getColLabels().get(i);
        //     if (!colRef.startsWith("`")) {
        //         colRef = "`" + colRef + "`";
        //     }
        //     String colAlias = finalCols.get(i).getName();

        //     sb.append(String.format("`%s`.%s AS `%s`", tableName.getTbl(), colRef, colAlias));
        // }
        // sb.append(String.format(" FROM (%s) %s", originalViewDef, tableName.getTbl()));
        // inlineViewDef = sb.toString();
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        if (columnNames != null) {
            cloneStmt = viewDefStmt.clone();
        }
        tableName.analyze(analyzer);
        viewDefStmt.setNeedToSql(true);
        // Analyze view define statement
        Analyzer viewAnalyzer = new Analyzer(analyzer);
        viewDefStmt.analyze(viewAnalyzer);

        // check privilege
        analyzer.checkPrivilege(tableName.getDb(), AccessPrivilege.READ_WRITE);
        createColumnAndViewDefs(analyzer);
    }
}
