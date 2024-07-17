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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Objects;

/**
 * table name info
 */
public class ColumnNameInfo extends TableNameInfo {
    @SerializedName(value = "col")
    private String col;

    public ColumnNameInfo() {

    }

    /**
     * ColumnNameInfo
     */
    public ColumnNameInfo(List<String> parts) throws AnalysisException {
        Objects.requireNonNull(parts, "require parts object");
        int size = parts.size();
        if (size < 3) {
            throw new AnalysisException("column name path is wrong. for example: `db.tbl.col` or `ctl.db.tbl.col`");
        }
        if (size >= 4) {
            ctl = parts.get(size - 4);
        }
        db = parts.get(size - 3);
        tbl = parts.get(size - 2);
        if (Env.isStoredTableNamesLowerCase() && !Strings.isNullOrEmpty(tbl)) {
            tbl = tbl.toLowerCase();
        }
        col = parts.get(size - 1);
    }

    /**
     * analyze tableNameInfo
     * @param ctx ctx
     */
    public void analyze(ConnectContext ctx) throws AnalysisException {
        super.analyze(ctx);
        if (Strings.isNullOrEmpty(col)) {
            throw new AnalysisException("Column name is null");
        }
    }

    public String getCol() {
        return col;
    }

    /**
     * equals
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnNameInfo that = (ColumnNameInfo) o;
        return tbl.equals(that.tbl) && db.equals(that.db) && ctl.equals(that.ctl) && col.equals(that.col);
    }

    /**
     * hashCode
     */
    @Override
    public int hashCode() {
        return Objects.hash(tbl, db, ctl, col);
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder(super.toSql());
        sb.append(".`").append(col).append("`");
        return sb.toString();
    }
}
