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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;

/**
 * Immutable context object carried through {@link ExprToExternalSqlVisitor}.
 * Encapsulates the four parameters that were previously passed as arguments to
 * {@code Expr.toSql(boolean, boolean, TableType, TableIf)}.
 */
public class ToSqlParams {

    /**
     * Pre-built instance equivalent to {@code toSqlWithoutTbl()}:
     * disableTableName=true, needExternalSql=false, tableType=null, table=null.
     */
    public static final ToSqlParams WITHOUT_TABLE = new ToSqlParams(true, false, null, null);
    public static final ToSqlParams WITH_TABLE = new ToSqlParams(false, false, null, null);

    public final boolean disableTableName;
    public final boolean needExternalSql;
    public final TableType tableType;
    public final TableIf table;

    public ToSqlParams(boolean disableTableName, boolean needExternalSql, TableType tableType, TableIf table) {
        this.disableTableName = disableTableName;
        this.needExternalSql = needExternalSql;
        this.tableType = tableType;
        this.table = table;
    }
}
