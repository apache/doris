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

import com.google.gson.annotations.SerializedName;

import java.util.Objects;
import java.util.StringJoiner;

public class StatisticsCacheKey {

    /**
     * May be index id either, since they are natively same in the code.
     * catalogId and dbId are not included in the hashCode. Because tableId is globally unique.
     */
    @SerializedName("catalogId")
    public final long catalogId;
    @SerializedName("dbId")
    public final long dbId;
    @SerializedName("tableId")
    public final long tableId;
    @SerializedName("idxId")
    public final long idxId;
    @SerializedName("colName")
    public final String colName;

    private static final String DELIMITER = "-";

    public StatisticsCacheKey(long catalogId, long dbId, long tableId, long idxId, String colName) {
        this.catalogId = catalogId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.idxId = idxId;
        this.colName = colName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogId, dbId, tableId, idxId, colName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StatisticsCacheKey k = (StatisticsCacheKey) obj;
        return this.catalogId == k.catalogId && this.dbId == k.dbId && this.tableId == k.tableId
                && this.idxId == k.idxId && this.colName.equals(k.colName);
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(DELIMITER);
        sj.add("ColumnStats");
        sj.add(String.valueOf(catalogId));
        sj.add(String.valueOf(dbId));
        sj.add(String.valueOf(tableId));
        sj.add(String.valueOf(idxId));
        sj.add(colName);
        return sj.toString();
    }
}
