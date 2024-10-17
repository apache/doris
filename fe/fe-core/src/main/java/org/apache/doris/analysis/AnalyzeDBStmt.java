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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;

public class AnalyzeDBStmt extends AnalyzeStmt implements NotFallbackInParser {

    private final String ctlName;
    private final String dbName;

    private CatalogIf ctlIf;

    private DatabaseIf<TableIf> db;

    public AnalyzeDBStmt(String ctlName, String dbName, AnalyzeProperties analyzeProperties) {
        super(analyzeProperties);
        this.ctlName = ctlName;
        this.dbName = dbName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (ctlName == null) {
            ctlIf = Env.getCurrentEnv().getCurrentCatalog();
        } else {
            ctlIf = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(ctlName);
        }
        db = ctlIf.getDbOrAnalysisException(dbName);
    }

    public CatalogIf getCtlIf() {
        return ctlIf;
    }

    public DatabaseIf<TableIf> getDb() {
        return db;
    }
}
