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
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

/**
 * Statement for show all catalog or desc the specific catalog.
 */
public class ShowCatalogStmt extends ShowStmt implements NotFallbackInParser {
    private static final ShowResultSetMetaData META_DATA_ALL =
            ShowResultSetMetaData.builder().addColumn(new Column("CatalogId", ScalarType.BIGINT))
                    .addColumn(new Column("CatalogName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Type", ScalarType.createStringType()))
                    .addColumn(new Column("IsCurrent", ScalarType.createStringType()))
                    .addColumn(new Column("CreateTime", ScalarType.createStringType()))
                    .addColumn(new Column("LastUpdateTime", ScalarType.createStringType()))
                    .addColumn(new Column("Comment", ScalarType.createStringType()))
                    .build();

    private static final ShowResultSetMetaData META_DATA_SPECIFIC =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Key", ScalarType.createStringType()))
                    .addColumn(new Column("Value", ScalarType.createStringType()))
                    .build();

    private final String catalogName;
    private String pattern;

    public ShowCatalogStmt() {
        this.catalogName = null;
        this.pattern = null;
    }

    public ShowCatalogStmt(String catalogName, String pattern) {
        this.catalogName = catalogName;
        this.pattern = pattern;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW");

        if (catalogName != null) {
            sb.append(" CATALOG ");
            sb.append(catalogName);
        } else {
            sb.append(" CATALOGS");

            if (pattern != null) {
                sb.append(" LIKE ");
                sb.append("'");
                sb.append(pattern);
                sb.append("'");
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (catalogName == null) {
            return META_DATA_ALL;
        } else {
            return META_DATA_SPECIFIC;
        }
    }
}
