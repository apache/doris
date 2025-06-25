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

package org.apache.doris.nereids.trees.plans.commands.use;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.NoForward;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

/**
 * Representation of a use cluster statement.
 */
public class UseCloudClusterCommand extends Command implements NoForward {
    private String cluster;
    private String database;
    private String catalogName;

    public UseCloudClusterCommand(String cluster) {
        super(PlanType.USE_CLOUD_CLUSTER_COMMAND);
        this.cluster = cluster;
    }

    public UseCloudClusterCommand(String cluster, String db) {
        super(PlanType.USE_CLOUD_CLUSTER_COMMAND);
        this.cluster = cluster;
        this.database = db;
    }

    public UseCloudClusterCommand(String cluster, String db, String catalogName) {
        super(PlanType.USE_CLOUD_CLUSTER_COMMAND);
        this.cluster = cluster;
        this.database = db;
        this.catalogName = catalogName;
    }

    public String getCluster() {
        return cluster;
    }

    public String getDatabase() {
        return database;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        handleUseCloudClusterCommand(ctx);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
        // check resource usage privilege
        if (Strings.isNullOrEmpty(cluster)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_CLUSTER_ERROR);
        }
        if (!Env.getCurrentEnv().getAccessManager().checkCloudPriv(ConnectContext.get().getCurrentUserIdentity(),
                cluster, PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
            throw new AnalysisException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                + "'@'" + ConnectContext.get().getRemoteIP()
                + "' for compute group '" + cluster + "'");
        }

        if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames().contains(cluster)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLOUD_CLUSTER_ERROR, cluster);
        }

        if (Strings.isNullOrEmpty(database)) {
            return;
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(),
                StringUtils.isEmpty(catalogName) ? InternalCatalog.INTERNAL_CATALOG_NAME : catalogName,
                database,
                PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    ctx.getQualifiedUser(), database);
        }
    }

    private void handleUseCloudClusterCommand(ConnectContext context) throws AnalysisException {
        if (!Config.isCloudMode()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_CLOUD_MODE);
            return;
        }

        try {
            ((CloudEnv) context.getEnv()).changeCloudCluster(cluster, context);
        } catch (DdlException e) {
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        }

        if (Strings.isNullOrEmpty(database)) {
            return;
        }

        try {
            if (catalogName != null) {
                context.getEnv().changeCatalog(context, catalogName);
            }
            context.getEnv().changeDb(context, database);
        } catch (DdlException e) {
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        }

        context.getState().setOk();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUseCloudClusterCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.USE;
    }
}
