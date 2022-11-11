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

public class StatisticsCacheKey {

    /**
     * May be index id either, since they are natively same in the code.
     */
    public final long tableId;
    public final String colName;

    public StatisticsCacheKey(long tableId, String colName) {
        this.tableId = tableId;
        this.colName = colName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, colName);
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
        return this.tableId == k.tableId && this.colName.equals(k.colName);
    }
}
