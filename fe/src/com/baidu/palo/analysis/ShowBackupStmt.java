// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

import com.baidu.palo.analysis.BinaryPredicate.Operator;
import com.baidu.palo.backup.BackupHandler;
import com.baidu.palo.backup.BackupJob;
import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.PatternMatcher;
import com.baidu.palo.common.proc.BackupProcNode;
import com.baidu.palo.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ShowBackupStmt extends ShowStmt {

    private String dbName;
    private Expr where;
    private String label;

    public ShowBackupStmt(String dbName, Expr where) {
        this.dbName = dbName;
        this.where = where;
    }

    public String getDbName() {
        return dbName;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // check access
        if (!analyzer.getCatalog().getUserMgr().checkAccess(analyzer.getUser(), dbName, AccessPrivilege.READ_ONLY)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getUser(), dbName);
        }

        analyzeWhere();
    }

    private void analyzeWhere() throws AnalysisException {
        boolean valid = true;
        while (where != null) {
            if (where instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) where;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break;
                }
            } else if (where instanceof LikePredicate) {
                LikePredicate likePredicate = (LikePredicate) where;
                if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
                    valid = false;
                    break;
                }
            } else {
                valid = false;
                break;
            }

            // left child
            if (!(where.getChild(0) instanceof SlotRef)) {
                valid = false;
                break;
            }
            String leftKey = ((SlotRef) where.getChild(0)).getColumnName();
            if (!leftKey.equalsIgnoreCase("label")) {
                valid = false;
                break;
            }

            // right child
            if (!(where.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break;
            }

            label = ((StringLiteral) where.getChild(1)).getStringValue();
            if (Strings.isNullOrEmpty(label)) {
                valid = false;
                break;
            }

            break;
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_backup_label\","
                    + " or LABEL LIKE \"matcher\"");
        }
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        List<List<String>> result = new LinkedList<List<String>>();
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        BackupHandler backupHandler = Catalog.getInstance().getBackupHandler();
        PatternMatcher matcher = null;
        if (!Strings.isNullOrEmpty(label)) {
            matcher = PatternMatcher.createMysqlPattern(label);
        }
        List<List<Comparable>> backupJobInfos = backupHandler.getJobInfosByDb(db.getId(), BackupJob.class, matcher);
        for (List<Comparable> infoStr : backupJobInfos) {
            List<String> oneInfo = new ArrayList<String>(BackupProcNode.TITLE_NAMES.size());
            for (Comparable element : infoStr) {
                oneInfo.add(element.toString());
            }
            result.add(oneInfo);
        }
        return result;
    }


    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : BackupProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, ColumnType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("SHOW BACKUP");
        if (dbName != null) {
            builder.append(" FROM `").append(dbName).append("` ");
        }

        builder.append(where.toSql());
        return builder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
