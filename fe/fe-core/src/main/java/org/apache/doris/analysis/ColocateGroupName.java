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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;

public class ColocateGroupName {
    private String db;
    private String group;

    public ColocateGroupName(String db, String group) {
        this.db = db;
        this.group = group;
    }

    public String getDb() {
        return db;
    }

    public String getGroup() {
        return group;
    }

    public void analyze() throws AnalysisException {

    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(db)) {
            sb.append("`").append(db).append("`.");
        }
        sb.append("`").append(group).append("`");
        return sb.toString();
    }
}
