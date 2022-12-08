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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;

import java.util.List;
import java.util.Map;

public abstract class TableValuedFunctionIf {
    private FunctionGenTable table = null;

    public FunctionGenTable getTable() throws AnalysisException {
        if (table == null) {
            table = new FunctionGenTable(-1, getTableName(), TableIf.TableType.TABLE_VALUED_FUNCTION,
                    getTableColumns(), this);
        }
        return table;
    }

    // All table functions should be registered here
    public static TableValuedFunctionIf getTableFunction(String funcName, Map<String, String> params)
                                                        throws UserException {
        switch (funcName.toLowerCase()) {
            case NumbersTableValuedFunction.NAME:
                return new NumbersTableValuedFunction(params);
            case S3TableValuedFunction.NAME:
                return new S3TableValuedFunction(params);
            case HdfsTableValuedFunction.NAME:
                return new HdfsTableValuedFunction(params);
            default:
                throw new UserException("Could not find table function " + funcName);
        }
    }

    public abstract String getTableName();

    public abstract List<Column> getTableColumns() throws AnalysisException;

    public abstract ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc);
}
