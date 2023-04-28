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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/StatementBase.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class MysqlLoadStmt extends InsertStmt {

    private DataDescription dataDescription;

    @Override
    public List<? extends DataDesc> getDataDescList() {
        return Collections.singletonList(dataDescription);
    }

    @Override
    public ResourceDesc getResourceDesc() {
        // mysql load does not have resource desc
        return null;
    }

    @Override
    public LoadType getLoadType() {
        return LoadType.MYSQL_LOAD;
    }

    @Override
    public void analyzeProperties() throws DdlException {

    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        String fullDbName = dataDescription.analyzeFullDbName(label.getDbName(), analyzer);
        dataDescription.analyze(fullDbName);
        Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(fullDbName);
        OlapTable table = db.getOlapTableOrAnalysisException(dataDescription.getTableName());
        dataDescription.checkKeyTypeForLoad(table);
        if (!dataDescription.isClientLocal()) {
            for (String path : dataDescription.getFilePaths()) {
                if (Config.mysql_load_server_secure_path.isEmpty()) {
                    throw new AnalysisException("Load local data from fe local is not enabled. If you want to use it,"
                            + " plz set the `mysql_load_server_secure_path` for FE to be a right path.");
                } else {
                    File file = new File(path);
                    try {
                        if (!(file.getCanonicalPath().startsWith(Config.mysql_load_server_secure_path))) {
                            throw new AnalysisException("Local file should be under the secure path of FE.");
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    if (!file.exists()) {
                        throw new AnalysisException("File: " + path + " is not exists.");
                    }
                }
            }
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
