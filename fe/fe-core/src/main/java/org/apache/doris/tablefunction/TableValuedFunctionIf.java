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
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import java.util.List;
import java.util.Map;

public abstract class TableValuedFunctionIf {
    private FunctionGenTable table = null;
    public static final String TVF_TABLE_PREFIX = "_table_valued_function_";

    public FunctionGenTable getTable() throws AnalysisException {
        if (table == null) {
            table = new FunctionGenTable(-1, getTableName(), TableIf.TableType.TABLE_VALUED_FUNCTION,
                    getTableColumns(), this);
        }
        return table;
    }

    // All table functions should be registered here
    public static TableValuedFunctionIf getTableFunction(String funcName, Map<String, String> params)
                                                        throws AnalysisException {
        switch (funcName.toLowerCase()) {
            case NumbersTableValuedFunction.NAME:
                return new NumbersTableValuedFunction(params);
            case S3TableValuedFunction.NAME:
                return new S3TableValuedFunction(params);
            case HdfsTableValuedFunction.NAME:
                return new HdfsTableValuedFunction(params);
            case HttpStreamTableValuedFunction.NAME:
                return new HttpStreamTableValuedFunction(params);
            case LocalTableValuedFunction.NAME:
                return new LocalTableValuedFunction(params);
            case IcebergTableValuedFunction.NAME:
                return new IcebergTableValuedFunction(params);
            case BackendsTableValuedFunction.NAME:
                return new BackendsTableValuedFunction(params);
            case FrontendsTableValuedFunction.NAME:
                return new FrontendsTableValuedFunction(params);
            case FrontendsDisksTableValuedFunction.NAME:
                return new FrontendsDisksTableValuedFunction(params);
            case CatalogsTableValuedFunction.NAME:
                return new CatalogsTableValuedFunction(params);
            case MvInfosTableValuedFunction.NAME:
                return new MvInfosTableValuedFunction(params);
            case PartitionsTableValuedFunction.NAME:
                return new PartitionsTableValuedFunction(params);
            case JobsTableValuedFunction.NAME:
                return new JobsTableValuedFunction(params);
            case TasksTableValuedFunction.NAME:
                return new TasksTableValuedFunction(params);
            case GroupCommitTableValuedFunction.NAME:
                return new GroupCommitTableValuedFunction(params);
            case QueryTableValueFunction.NAME:
                return QueryTableValueFunction.createQueryTableValueFunction(params);
            case PartitionValuesTableValuedFunction.NAME:
                return new PartitionValuesTableValuedFunction(params);
            default:
                throw new AnalysisException("Could not find table function " + funcName);
        }
    }

    public abstract String getTableName();

    public abstract List<Column> getTableColumns() throws AnalysisException;

    public abstract ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv);

    public void checkAuth(ConnectContext ctx) {

    }
}
