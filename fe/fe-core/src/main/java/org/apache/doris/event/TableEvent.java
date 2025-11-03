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

package org.apache.doris.event;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;

public abstract class TableEvent extends Event {
    protected final long ctlId;
    protected final String ctlName;
    protected final long dbId;
    protected final String dbName;
    protected final long tableId;
    protected final String tableName;

    public TableEvent(EventType eventType, long ctlId, long dbId, long tableId) throws AnalysisException {
        super(eventType);
        this.ctlId = ctlId;
        this.dbId = dbId;
        this.tableId = tableId;
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(ctlId);
        DatabaseIf db = catalog.getDbOrAnalysisException(dbId);
        TableIf table = db.getTableOrAnalysisException(tableId);
        this.ctlName = catalog.getName();
        this.dbName = db.getFullName();
        this.tableName = table.getName();
    }

    public long getCtlId() {
        return ctlId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getCtlName() {
        return ctlName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "TableEvent{"
                + "ctlId=" + ctlId
                + ", ctlName='" + ctlName + '\''
                + ", dbId=" + dbId
                + ", dbName='" + dbName + '\''
                + ", tableId=" + tableId
                + ", tableName='" + tableName + '\''
                + "} " + super.toString();
    }
}
