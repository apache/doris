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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// MODIFY ENGINE TO odbc PROPERTIES("driver" = "Oracle 19 ODBC driver")
public class ModifyEngineClause extends AlterTableClause {
    private static final Logger LOG = LogManager.getLogger(ModifyEngineClause.class);
    private String engine;
    private Map<String, String> properties;

    public ModifyEngineClause(String engine, Map<String, String> properties) {
        super(AlterOpType.MODIFY_ENGINE);
        this.engine = engine;
        this.properties = properties;
    }

    public String getEngine() {
        return engine;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(engine)) {
            throw new AnalysisException("Engine name is missing");
        }

        if (!engine.equalsIgnoreCase(Table.TableType.ODBC.name())) {
            throw new AnalysisException("Only support alter table engine from MySQL to ODBC");
        }

        if (properties == null || !properties.containsKey(OdbcTable.ODBC_DRIVER)) {
            throw new AnalysisException("Need specify 'driver' property");
        }
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY ENGINE TO ").append(engine);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
