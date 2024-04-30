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

import org.apache.doris.common.Pair;

import java.util.Set;
import java.util.StringJoiner;

public class AutoAnalysisPendingJob {

    public final String catalogName;
    public final String dbName;
    public final String tableName;
    public final Set<Pair<String, String>> columns;
    public final JobPriority priority;

    public AutoAnalysisPendingJob(String catalogName, String dbName, String tableName,
            Set<Pair<String, String>> columns, JobPriority priority) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = columns;
        this.priority = priority;
    }

    public String getColumnNames() {
        if (columns == null) {
            return "";
        }
        StringJoiner stringJoiner = new StringJoiner(",");
        for (Pair<String, String> col : columns) {
            stringJoiner.add(col.toString());
        }
        return stringJoiner.toString();
    }
}
