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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * drop catalog recycle bin command
 */
public class DropCatalogRecycleBinCommand extends Command implements ForwardWithSync {

    /**
     * id type
     */
    public enum IdType {
        DATABASE_ID,
        TABLE_ID,
        PARTITION_ID;

        /**
         * return IdType according to given String
         */
        public static IdType fromString(String idTypeStr) {
            IdType idType;
            if (idTypeStr.equalsIgnoreCase("DbId")) {
                idType = DATABASE_ID;
            } else if (idTypeStr.equalsIgnoreCase("TableId")) {
                idType = TABLE_ID;
            } else if (idTypeStr.equalsIgnoreCase("PartitionId")) {
                idType = PARTITION_ID;
            } else {
                String message = "DROP CATALOG RECYCLE BIN: " + idTypeStr
                        + " should be 'DbId', 'TableId' or 'PartitionId'.";
                throw new AnalysisException(message);
            }
            return idType;
        }
    }

    private final IdType idType;
    private long id = -1;

    /**
     * constructor
     */
    public DropCatalogRecycleBinCommand(IdType idType, long id) {
        super(PlanType.DROP_CATALOG_RECYCLE_BIN_COMMAND);
        this.idType = idType;
        this.id = id;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        Env.getCurrentEnv().dropCatalogRecycleBin(idType, id);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropCatalogRecycleBinCommand(this, context);
    }
}
