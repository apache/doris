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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * recover partition command
 */
public class RecoverPartitionCommand extends RecoverCommand {
    private final TableNameInfo dbTblName;
    private final String partitionName;
    private final long partitionId;
    private final String newPartitionName;

    /**
     * constructor
     */
    public RecoverPartitionCommand(TableNameInfo dbTblName, String partitionName,
                                    long partitionId, String newPartitionName) {
        super(PlanType.RECOVER_PARTITION_COMMAND);
        this.dbTblName = dbTblName;
        this.partitionName = partitionName;
        this.partitionId = partitionId;
        this.newPartitionName = newPartitionName;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws UserException {
        dbTblName.analyze(ctx);

        // disallow external catalog
        Util.prohibitExternalCatalog(dbTblName.getCtl(), this.getClass().getSimpleName());
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbTblName.getCtl(), dbTblName.getDb(),
                        dbTblName.getTbl(), PrivPredicate.ALTER_CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "RECOVERY",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbTblName.getDb() + ": " + dbTblName.getTbl());
        }

        Env.getCurrentEnv().recoverPartition(dbTblName.getDb(), dbTblName.getTbl(),
                                                partitionName, newPartitionName, partitionId);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitRecoverPartitionCommand(this, context);
    }
}
