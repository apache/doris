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

import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatisticsCacheKey {

    /**
     * May be index id either, since they are natively same in the code.
     */
    public final long tableId;
    public final long idxId;
    public final String colName;
    public final boolean preHeating;

    private static final String DELIMITER = "-";

    public StatisticsCacheKey(long tableId, String colName) {
        this(tableId, -1, colName);
    }

    public StatisticsCacheKey(long tableId, long idxId, String colName) {
        this.tableId = tableId;
        this.idxId = idxId;
        this.colName = colName;
        this.preHeating = false;
    }

    public StatisticsCacheKey(long tableId, long idxId, String colName, boolean preHeating) {
        this.tableId = tableId;
        this.idxId = idxId;
        this.colName = colName;
        this.preHeating = preHeating;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, idxId, colName);
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
        return this.tableId == k.tableId && this.idxId == k.idxId && this.colName.equals(k.colName);
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(DELIMITER);
        sj.add(String.valueOf(tableId));
        sj.add(String.valueOf(idxId));
        sj.add(colName);
        return sj.toString();
    }

    public static StatisticsCacheKey fromString(String s) {
        Pattern pattern = Pattern.compile("^(-?\\d+)-(-?\\d+)-(.+)$");
        Matcher matcher = pattern.matcher(s);
        if (!matcher.matches()) {
            throw new RuntimeException("Id malformed: " + s);
        }
        long tblId = Long.parseLong(matcher.group(1));
        long idxId = Long.parseLong(matcher.group(2));
        String colName = matcher.group(3);
        return new StatisticsCacheKey(tblId, idxId, colName, true);

    }
}
