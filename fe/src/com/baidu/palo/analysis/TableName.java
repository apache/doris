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

import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;

import com.google.common.base.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableName implements Writable {
    private String tbl;
    private String db;

    public TableName() {

    }

    public TableName(String db, String tbl) {
        this.db = db;
        this.tbl = tbl;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(db)) {
            db = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            if (Strings.isNullOrEmpty(analyzer.getClusterName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NAME_NULL);
            }
            db = ClusterNamespace.getDbFullName(analyzer.getClusterName(), db);
        }

        if (Strings.isNullOrEmpty(tbl)) {
            throw new AnalysisException("Table name is null");
        }
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

    public boolean isEmpty() {
        return tbl.isEmpty();
    }

    /**
     * Returns true if this name has a non-empty database field and a non-empty
     * table name.
     */
    public boolean isFullyQualified() {
        return db != null && !db.isEmpty() && !tbl.isEmpty();
    }

    public String getNoClusterString() {
        if (db == null) {
            return tbl;
        } else {
            String dbName = ClusterNamespace.getDbNameFromFullName(db);
            if (dbName == null) {
                return db + "." + tbl;
            } else {
                return dbName + "." + tbl;
            }
        }
    }

    @Override
    public String toString() {
        if (db == null) {
            return tbl;
        } else {
            return db + "." + tbl;
        }
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

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        if (db != null) {
            stringBuilder.append("`").append(db).append("`.");
        }
        stringBuilder.append("`").append(tbl).append("`");
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, db);
        Text.writeString(out, tbl);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        db = Text.readString(in);
        tbl = Text.readString(in);
    }
}
