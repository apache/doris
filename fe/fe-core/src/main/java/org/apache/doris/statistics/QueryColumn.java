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

package org.apache.doris.statistics;

import org.apache.doris.thrift.TQueryColumn;

import java.util.Objects;

public class QueryColumn {

    public final long catalogId;
    public final long dbId;
    public final long tblId;
    public final String colName;

    public QueryColumn(long catalogId, long dbId, long tblId, String colName) {
        this.catalogId = catalogId;
        this.dbId = dbId;
        this.tblId = tblId;
        this.colName = colName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogId, dbId, tblId, colName);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof QueryColumn)) {
            return false;
        }
        QueryColumn otherCriticalColumn = (QueryColumn) other;
        return this.catalogId == otherCriticalColumn.catalogId
            && this.dbId == otherCriticalColumn.dbId
            && this.tblId == otherCriticalColumn.tblId
            && this.colName.equals(otherCriticalColumn.colName);
    }

    public TQueryColumn toThrift() {
        TQueryColumn tQueryColumn = new TQueryColumn();
        tQueryColumn.catalogId = String.valueOf(catalogId);
        tQueryColumn.dbId = String.valueOf(dbId);
        tQueryColumn.tblId = String.valueOf(tblId);
        tQueryColumn.colName = colName;
        return tQueryColumn;
    }
}
