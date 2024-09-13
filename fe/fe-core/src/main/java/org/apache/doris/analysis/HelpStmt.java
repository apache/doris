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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

public class HelpStmt extends ShowStmt implements NotFallbackInParser {
    private static final ShowResultSetMetaData TOPIC_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("description", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("example", ScalarType.createVarchar(1000)))
                    .build();
    private static final ShowResultSetMetaData CATEGORY_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("source_category_name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("is_it_category", ScalarType.createVarchar(1)))
                    .build();
    private static final ShowResultSetMetaData KEYWORD_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("is_it_category", ScalarType.createVarchar(1)))
                    .build();
    private String mask;

    public HelpStmt(String mask) {
        this.mask = mask;
    }

    public String getMask() {
        return mask;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(mask)) {
            throw new AnalysisException("Help empty info.");
        }
    }

    @Override
    public String toSql() {
        return "HELP " + mask;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return TOPIC_META_DATA;
    }

    public ShowResultSetMetaData getCategoryMetaData() {
        return CATEGORY_META_DATA;
    }

    public ShowResultSetMetaData getKeywordMetaData() {
        return KEYWORD_META_DATA;
    }
}
