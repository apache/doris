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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.UserException;

import java.util.List;

public abstract class TableValuedFunctionInf {

    public abstract String getFuncName();
    
    public Table getTable() {
        Table table = new Table(-1, getTableName(), TableType.TABLE_VALUED_FUNCTION, getTableColumns());
        return table;
    }

    // All table functions should be registered here
    public static TableValuedFunctionInf getTableFunction(String funcName, List<String> params) throws UserException {
        if (funcName.equalsIgnoreCase(NumbersTableValuedFunction.NAME)) {
            return new NumbersTableValuedFunction(params);
        }
        throw new UserException("Could not find table function " + funcName);
    }

    public abstract String getTableName();

    public abstract List<Column> getTableColumns();
    
    public abstract List<TableValuedFunctionTask> getTasks();
}