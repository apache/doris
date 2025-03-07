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
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

public class TableName implements Writable {
    @SerializedName(value = "ctl")
    private String ctl;
    @SerializedName(value = "tbl")
    private String tbl;
    @SerializedName(value = "db")
    private String db;

    public TableName() {

    }

    public TableName(String alias) {
        String[] parts = alias.split("\\.");
        Preconditions.checkArgument(parts.length > 0, "table name can't be empty");
        tbl = parts[parts.length - 1];
        if (Env.isStoredTableNamesLowerCase() && !Strings.isNullOrEmpty(tbl)) {
            tbl = tbl.toLowerCase();
        }
        if (parts.length >= 2) {
            db = parts[parts.length - 2];
        }
        if (parts.length >= 3) {
            ctl = parts[parts.length - 3];
        }
    }

    public TableName(String ctl, String db, String tbl) {
        if (Env.isStoredTableNamesLowerCase() && !Strings.isNullOrEmpty(tbl)) {
            tbl = tbl.toLowerCase();
        }
        this.ctl = ctl;
        this.db = db;
        this.tbl = tbl;
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

    public boolean isEmpty() {
        return tbl.isEmpty();
    }

    /**
     * Returns true if this name has a non-empty catalog and a non-empty database field
     * and a non-empty table name.
     */
    public boolean isFullyQualified() {
        return Stream.of(ctl, db, tbl).noneMatch(Strings::isNullOrEmpty);
    }

    /**
     * Analyzer.registerTableRef task alias of index 1 as the legal implicit alias.
     */
    public String[] tableAliases() {
        if (ctl == null || ctl.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            // db.tbl
            // tbl
            return new String[] {toString(), tbl};
        } else {
            // ctl.db.tbl
            // db.tbl
            // tbl
            return new String[] {toString(),
                    String.format("%s.%s", db, tbl), tbl};
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (ctl != null && !ctl.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            stringBuilder.append(ctl).append(".");
        }
        if (db != null) {
            stringBuilder.append(db).append(".");
        }
        stringBuilder.append(tbl);
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof TableName) {
            return toString().equals(other.toString());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctl, tbl, db);
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        if (ctl != null && !ctl.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            stringBuilder.append("`").append(ctl).append("`.");
        }
        if (db != null) {
            stringBuilder.append("`").append(db).append("`.");
        }
        stringBuilder.append("`").append(tbl).append("`");
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public void readFields(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_111) {
            TableName fromJson = GsonUtils.GSON.fromJson(Text.readString(in), TableName.class);
            ctl = fromJson.ctl;
            db = fromJson.db;
            tbl = fromJson.tbl;
        } else {
            ctl = InternalCatalog.INTERNAL_CATALOG_NAME;
            db = Text.readString(in);
            tbl = Text.readString(in);
        }
    }

    public TableName cloneWithoutAnalyze() {
        return new TableName(this.ctl, this.db, this.tbl);
    }
}
