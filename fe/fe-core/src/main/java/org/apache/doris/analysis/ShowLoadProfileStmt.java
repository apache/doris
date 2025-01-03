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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.qe.ShowResultSetMetaData;

// For stmt like:
// show load profile "/";   # list all saving load job ids
public class ShowLoadProfileStmt extends ShowStmt implements NotFallbackInParser {
    // This should be same as ProfileManager.PROFILE_HEADERS
    public static final ShowResultSetMetaData META_DATA_QUERY_IDS;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String key : SummaryProfile.SUMMARY_KEYS) {
            builder.addColumn(new Column(key, ScalarType.createStringType()));
        }
        META_DATA_QUERY_IDS = builder.build();
    }

    public ShowLoadProfileStmt(String useless) {
    }

    @Override
    public String toSql() {
        return "SHOW LOAD PROFILE";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA_QUERY_IDS;
    }
}
