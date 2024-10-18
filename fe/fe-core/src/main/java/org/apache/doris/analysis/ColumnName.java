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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TableName.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;
import java.util.stream.Stream;

public class ColumnName {
    @SerializedName(value = "ctl")
    private String ctl;

    @SerializedName(value = "db")
    private String db;

    @SerializedName(value = "tbl")
    private String tbl;

    @SerializedName(value = "col")
    private String col;

    public ColumnName() {

    }

    public ColumnName(String ctl, String db, String tbl, String col) {
        if (Env.isStoredTableNamesLowerCase() && !Strings.isNullOrEmpty(tbl)) {
            tbl = tbl.toLowerCase();
        }
        this.ctl = ctl;
        this.db = db;
        this.tbl = tbl;
        this.col = col;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(ctl)) {
            ctl = analyzer.getDefaultCatalog();
            if (Strings.isNullOrEmpty(ctl)) {
                ctl = InternalCatalog.INTERNAL_CATALOG_NAME;
            }
        }
        if (Strings.isNullOrEmpty(db)) {
            db = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        if (Strings.isNullOrEmpty(tbl)) {
            throw new AnalysisException("Table name is null");
        }

        if (Strings.isNullOrEmpty(col)) {
            throw new AnalysisException("Column name is null");
        }
    }

    public boolean isFullyQualified() {
        return Stream.of(ctl, db, tbl, col).noneMatch(Strings::isNullOrEmpty);
    }


    public String getCtl() {
        return ctl;
    }

    public void setCtl(String ctl) {
        this.ctl = ctl;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTbl() {
        return tbl;
    }

    public void setTbl(String tbl) {
        this.tbl = tbl;
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ColumnName) {
            return toString().equals(other.toString());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctl, tbl, db, col);
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        if (ctl != null && !ctl.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            stringBuilder.append("`").append(ctl).append("`.");
        }
        if (db != null) {
            stringBuilder.append("`").append(db).append("`.");
        }
        stringBuilder.append("`").append(tbl).append("`.");
        stringBuilder.append("`").append(col).append("`");
        return stringBuilder.toString();
    }
}
