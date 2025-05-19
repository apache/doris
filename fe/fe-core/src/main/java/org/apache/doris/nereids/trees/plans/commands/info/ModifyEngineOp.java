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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.ModifyEngineClause;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Map;

/**
 * ModifyEngineOp
 */
public class ModifyEngineOp extends AlterTableOp {
    private String engine;
    private Map<String, String> properties;

    public ModifyEngineOp(String engine, Map<String, String> properties) {
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
    public void validate(ConnectContext ctx) throws UserException {
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
    public AlterTableClause translateToLegacyAlterClause() {
        return new ModifyEngineClause(engine, properties);
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
