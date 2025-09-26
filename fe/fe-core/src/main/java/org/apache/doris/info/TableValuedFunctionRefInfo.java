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

package org.apache.doris.info;

import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import java.util.Map;

/**
 * TableValuedFunctionRefInfo
 */
public class TableValuedFunctionRefInfo extends TableRefInfo {

    private Table table;
    private TableValuedFunctionIf tableFunction;
    private String funcName;
    private Map<String, String> params;

    /**
     * constructor
     */
    public TableValuedFunctionRefInfo(String funcName, String alias, Map<String, String> params)
            throws AnalysisException {
        super(new TableNameInfo(null, null, TableValuedFunctionIf.TVF_TABLE_PREFIX + funcName), alias);
        this.funcName = funcName;
        this.params = params;
        this.tableFunction = TableValuedFunctionIf.getTableFunction(funcName, params);
        this.table = tableFunction.getTable();
        if (hasAlias()) {
            return;
        }
        tableAlias = TableValuedFunctionIf.TVF_TABLE_PREFIX + funcName;
    }

    public TableValuedFunctionRefInfo(TableValuedFunctionRefInfo other) {
        super(other);
        this.funcName = other.funcName;
        this.params = other.params;
        this.tableFunction = other.tableFunction;
        this.table = other.table;
    }

    public Table getTable() {
        return table;
    }

    public TableValuedFunctionIf getTableFunction() {
        return tableFunction;
    }

    public String getFuncName() {
        return funcName;
    }

    public Map<String, String> getParams() {
        return params;
    }

    @Override
    public TableRefInfo clone() {
        return new TableValuedFunctionRefInfo(this);
    }
}
