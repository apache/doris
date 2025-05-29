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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Map;

/**
 * admin set partition version command
 */
public class AdminSetPartitionVersionCommand extends Command implements ForwardWithSync {
    long partitionId = -1;
    long visibleVersion = -1;
    private final TableNameInfo tableName;
    private final Map<String, String> properties;

    /**
     * constructor
     */
    public AdminSetPartitionVersionCommand(TableNameInfo tableName, Map<String, String> properties) {
        super(PlanType.ADMIN_SET_PARTITION_VERSION);
        this.tableName = tableName;
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {

        tableName.analyze(ctx);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        checkProperties();

        String database = tableName.getDb();
        String table = tableName.getTbl();

        int setSuccess = Env.getCurrentEnv().setPartitionVersionInternal(database, table, partitionId, visibleVersion,
                false);
        if (setSuccess == -1) {
            throw new DdlException("Failed to set partition visible version to " + visibleVersion + ". " + "Partition "
                + partitionId + " not exists. Database " + database + ", Table " + table + ".");
        }

    }

    private void checkProperties() throws AnalysisException {
        partitionId = PropertyAnalyzer.analyzePartitionId(properties);
        if (partitionId == -1) {
            throw new AnalysisException("Should specify 'partition_id' property.");
        }
        visibleVersion = PropertyAnalyzer.analyzeVisibleVersion(properties);
        if (visibleVersion == -1) {
            throw new AnalysisException("Should specify 'visible_version' property.");
        }
        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetPartitionVersionCommand(this, context);
    }
}
