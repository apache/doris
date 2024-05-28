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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

/**
 * drop catalog recycle db/table/partition info
 */
public class DropCatalogRecycleBinInfo {

    private String idType;
    private long dbId = -1;
    private long tableId = -1;
    private long partitionId = -1;

    /**
     *  constructor for DropCatalogRecycleBinInfo
     */
    public DropCatalogRecycleBinInfo(String idType, long id) {
        this.idType = idType;
        if (idType.equalsIgnoreCase("'DbId'") || idType.equalsIgnoreCase("\"DbId\"")) {
            this.dbId = id;
        } else if (idType.equalsIgnoreCase("'TableId'") || idType.equalsIgnoreCase("\"TableId\"")) {
            this.tableId = id;
        } else if (idType.equalsIgnoreCase("'PartitionId'") || idType.equalsIgnoreCase("\"PartitionId\"")) {
            this.partitionId = id;
        }
    }

    /**
     * analyze drop catalog recycle bin info
     *
     * @param ctx ConnectContext
     */
    public void analyze(ConnectContext ctx) {
        if (!idType.equalsIgnoreCase("'DbId'")
                && !idType.equalsIgnoreCase("\"DbId\"")
                && !idType.equalsIgnoreCase("'TableId'")
                && !idType.equalsIgnoreCase("\"TableId\"")
                && !idType.equalsIgnoreCase("'PartitionId'")
                && !idType.equalsIgnoreCase("\"PartitionId\"")) {
            String message = "DROP CATALOG RECYCLE BIN: " + idType + " should be 'DbId', 'TableId' or 'PartitionId'.";
            throw new AnalysisException(message);
        }
    }

    /**
     * getIdType
     */
    public String getIdType() {
        return idType;
    }

    /**
     * getDbId
     */
    public long getDbId() {
        return dbId;
    }

    /**
     * getTableId
     */
    public long getTableId() {
        return tableId;
    }

    /**
     * getPartitionId
     */
    public long getPartitionId() {
        return partitionId;
    }
}
