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

import org.apache.doris.statistics.util.StatisticsUtil;

import java.util.StringJoiner;

public class StatsId {

    public final String id;
    public final long catalogId;
    public final long dbId;
    public final long tblId;
    public final long idxId;

    public final String colId;

    // nullable
    public final String partId;

    public StatsId(ResultRow row) {
        this.id = row.get(0);
        this.catalogId = Long.parseLong(row.get(1));
        this.dbId = Long.parseLong(row.get(2));
        this.tblId = Long.parseLong(row.get(3));
        this.idxId = Long.parseLong(row.get(4));
        this.colId = row.get(5);
        this.partId = row.get(6);
    }

    public String toSQL() {
        StringJoiner sj = new StringJoiner(",");
        sj.add(StatisticsUtil.quote(id));
        sj.add(String.valueOf(catalogId));
        sj.add(String.valueOf(dbId));
        sj.add(String.valueOf(tblId));
        sj.add(String.valueOf(idxId));
        sj.add(StatisticsUtil.quote(colId));
        sj.add(partId);
        return sj.toString();
    }
}
