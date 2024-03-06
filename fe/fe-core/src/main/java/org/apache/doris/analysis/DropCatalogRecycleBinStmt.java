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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

/**
 * Statement for drop a db/table/partition in catalog recycle bin.
 */
public class DropCatalogRecycleBinStmt extends DdlStmt {
    private String idType;
    private long dbId = -1;
    private long tableId = -1;
    private long partitionId = -1;

    public DropCatalogRecycleBinStmt(String idType, long id) {
        this.idType = idType;
        if (idType.equals("'DbId'")) {
            this.dbId = id;
        } else if (idType.equals("'TableId'")) {
            this.tableId = id;
        } else if (idType.equals("'PartitionId'")) {
            this.partitionId = id;
        }
    }

    public String getIdType() {
        return idType;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (!idType.equals("'DbId'") && !idType.equals("'TableId'") && !idType.equals("'PartitionId'")) {
            String message = "DROP CATALOG RECYCLE BIN: " + idType + " should be 'DbId', 'TableId' or 'PartitionId'.";
            throw new AnalysisException(message);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP CATALOG RECYCLE BIN WHERE ");
        sb.append(this.idType);
        sb.append(" = ");
        if (idType.equals("'DbId'")) {
            sb.append(this.dbId);
        } else if (idType.equals("'TableId'")) {
            sb.append(this.tableId);
        } else if (idType.equals("'PartitionId'")) {
            sb.append(this.partitionId);
        }
        return sb.toString();
    }
}
