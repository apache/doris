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

import java.util.Map;

public class AlterDatabasePropertyStmt extends DdlStmt {
    private String dbName;
    private Map<String, String> properties;

    public AlterDatabasePropertyStmt(String dbName, Map<String, String> properties) {
        this.dbName = dbName;
        this.properties = properties;
    }

    public String getDbName() {
        return dbName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        // TODO: add some property check
    }

    @Override
    public String toSql() {
        return "ALTER DATABASE " + dbName + " SET PROPERTIES ("
                + new PrintableMap<String, String>(properties, "=", true, false, ",") + ")";
    }
}
