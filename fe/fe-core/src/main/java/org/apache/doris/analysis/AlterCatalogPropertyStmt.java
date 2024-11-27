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

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;

import java.util.Map;

/**
 * Statement for alter the catalog property.
 */
public class AlterCatalogPropertyStmt extends AlterCatalogStmt implements NotFallbackInParser {
    private final Map<String, String> newProperties;

    public AlterCatalogPropertyStmt(String catalogName, Map<String, String> newProperties) {
        super(catalogName);
        this.newProperties = newProperties;
    }

    public Map<String, String> getNewProperties() {
        return newProperties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        PropertyAnalyzer.checkCatalogProperties(newProperties, true);
    }

    @Override
    public String toSql() {
        return "ALTER CATALOG " + catalogName + " SET PROPERTIES ("
                + new PrintableMap<>(newProperties, "=", true, false, true) + ")";
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
