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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

public class ColocateTableUtils {

    static Table getColocateTable(Database db, String tableName) {
        Table parentTable;
        db.readLock();
        try {
            parentTable = db.getTable(tableName);
        } finally {
            db.readUnlock();
        }
        return parentTable;
    }

    public static Table getTable(Database db, long tblId) {
        Table tbl;
        db.readLock();
        try {
            tbl = db.getTable(tblId);
        } finally {
            db.readUnlock();
        }
        return tbl;
    }

    static void checkTableExist(Table colocateTable, String colocateTableName) throws DdlException {
        if (colocateTable == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_NOT_EXIST, colocateTableName);
        }
    }

    static void checkTableType(Table colocateTable) throws DdlException {
        if (colocateTable.type != (Table.TableType.OLAP)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_BE_OLAP_TABLE, colocateTable.getName());
        }
    }

    public static void checkTableIsColocated(Table parentTable, String colocateTableName) throws DdlException {
        if (Catalog.getCurrentCatalog().getColocateTableIndex().isColocateTable(parentTable.getId())) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_NOT_COLOCATE_TABLE, colocateTableName);
        }
    }
}
