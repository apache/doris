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

import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.tablefunction.TableValuedFunctionInf;

import java.util.List;

public class TableValuedFunctionRef extends TableRef {

    private Table table;
    private TableValuedFunctionInf tableFunction;

    public TableValuedFunctionRef(String funcName, String alias, List<String> params) throws UserException {
        super(new TableName(null, "#table_function#" + funcName), alias);
        this.tableFunction = TableValuedFunctionInf.getTableFunction(funcName, params);
        if (hasExplicitAlias())
            return;
        aliases_ = new String[] { "#table_function#" + funcName };
    }

    public TableValuedFunctionRef(TableValuedFunctionRef other) {
        super(other);
        this.tableFunction = other.tableFunction;
    }

    @Override
    public TableRef clone() {
        return new TableValuedFunctionRef(this);
    }

    @Override
    public TupleDescriptor createTupleDescriptor(Analyzer analyzer) {
        TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor();
        result.setTable(table);
        return result;
    }

    /**
     * Register this table ref and then analyze the Join clause.
     */
    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (isAnalyzed)
            return;
        // Table function could generate a table which will has columns
        // Maybe will call be during this process
        this.table = tableFunction.getTable();
        desc = analyzer.registerTableRef(this);
        isAnalyzed = true; // true that we have assigned desc
        analyzeJoin(analyzer);
    }

    public TableValuedFunctionInf getTableFunction() {
        return tableFunction;
    }

}
