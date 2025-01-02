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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.tablefunction.BackendsTableValuedFunction;
import org.apache.doris.tablefunction.LocalTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import java.util.Map;
import java.util.stream.Collectors;

public class TableValuedFunctionRef extends TableRef {

    private Table table;
    private TableValuedFunctionIf tableFunction;
    private String funcName;
    private Map<String, String> params;

    public TableValuedFunctionRef(String funcName, String alias, Map<String, String> params) throws AnalysisException {
        super(new TableName(null, null, TableValuedFunctionIf.TVF_TABLE_PREFIX + funcName), alias);
        this.funcName = funcName;
        this.params = params;
        this.tableFunction = TableValuedFunctionIf.getTableFunction(funcName, params);
        this.table = tableFunction.getTable();
        if (hasExplicitAlias()) {
            return;
        }
        aliases = new String[] { TableValuedFunctionIf.TVF_TABLE_PREFIX + funcName };
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

    @Override
    protected String tableNameToSql() {
        String aliasSql = null;
        String alias = getExplicitAlias();
        if (alias != null) {
            aliasSql = ToSqlUtils.getIdentSql(alias);
        }

        // set tableName
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(funcName);
        stringBuilder.append('(');
        String paramsString  = params.entrySet().stream().map(kv -> "\"" + kv.getKey() + "\""
                        + " = " + "\"" + kv.getValue() + "\"")
                        .collect(Collectors.joining(","));
        stringBuilder.append(paramsString);
        stringBuilder.append(')');

        // set alias
        stringBuilder.append((aliasSql != null) ? " " + aliasSql : "");
        return stringBuilder.toString();
    }

    /**
     * Register this table ref and then analyze the Join clause.
     */
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }

        // check privilige for backends/local tvf
        if (funcName.equalsIgnoreCase(BackendsTableValuedFunction.NAME)
                || funcName.equalsIgnoreCase(LocalTableValuedFunction.NAME)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.OPERATOR)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
            }
        }

        desc = analyzer.registerTableRef(this);
        isAnalyzed = true; // true that we have assigned desc
        analyzeJoin(analyzer);
    }

    public ScanNode getScanNode(PlanNodeId id, SessionVariable sv) {
        return tableFunction.getScanNode(id, desc, sv);
    }

    public TableValuedFunctionIf getTableFunction() {
        return tableFunction;
    }

}
