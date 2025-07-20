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
 * admin set partition version ("key" = "value");
 * key and value may be : "partition_id" = "10075", "visible_version" = "100");
 */
public class AdminSetPartitionVersionCommand extends Command implements ForwardWithSync {
    private long partitionId = -1;
    private long visibleVersion = -1;
    private final TableNameInfo tableName;
    private final Map<String, String> properties;

    /**
     * constructor of AdminSetPartitionVersionCommand
     */
    public AdminSetPartitionVersionCommand(TableNameInfo tableName, Map<String, String> properties) {
        super(PlanType.ADMIN_SET_PARTITION_VERSION_COMMAND);
        this.tableName = tableName;
        this.properties = properties;
    }

    public String getDatabase() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public Long getPartitionId() {
        return partitionId;
    }

    public Long getVisibleVersion() {
        return visibleVersion;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        tableName.analyze(ctx);
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        checkProperties();
        Env.getCurrentEnv().setPartitionVersion(this);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetPartitionVersionCommand(this, context);
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
}
