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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import java.util.Map;

public class TableValuedFunctionRef extends TableRef {

    private Table table;
    private TableValuedFunctionIf tableFunction;
    private String funcName;
    private Map<String, String> params;

    public TableValuedFunctionRef(String funcName, String alias, Map<String, String> params) throws AnalysisException {
        super(new TableName(null, null, "_table_valued_function_" + funcName), alias);
        this.funcName = funcName;
        this.params = params;
        this.tableFunction = TableValuedFunctionIf.getTableFunction(funcName, params);
        this.table = tableFunction.getTable();
        if (hasExplicitAlias()) {
            return;
        }
        aliases = new String[] { "_table_valued_function_" + funcName };
    }

    public TableValuedFunctionRef(TableValuedFunctionRef other) {
        super(other);
        this.funcName = other.funcName;
        this.params = other.params;
        this.tableFunction = other.tableFunction;
        this.table = other.table;
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
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        desc = analyzer.registerTableRef(this);
        isAnalyzed = true; // true that we have assigned desc
        analyzeJoin(analyzer);
    }

    public ScanNode getScanNode(PlanNodeId id) {
        return tableFunction.getScanNode(id, desc);
    }

    public TableValuedFunctionIf getTableFunction() {
        return tableFunction;
    }

}
