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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Represents the command for SHOW CLUSTERS.
 */
public class ShowClustersCommand extends ShowCommand {
    public static final ImmutableList<String> CLUSTER_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("cluster").add("is_current").add("users").add("backend_num").build();

    public static final ImmutableList<String> COMPUTE_GROUP_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("IsCurrent").add("Users").add("BackendNum").build();

    private static final Logger LOG = LogManager.getLogger(ShowClustersCommand.class);
    private final boolean isComputeGroup;

    public ShowClustersCommand(boolean isComputeGroup) {
        super(PlanType.SHOW_CLUSTERS_COMMAND);
        this.isComputeGroup = isComputeGroup;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        if (Config.isNotCloudMode()) {
            // just user admin
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get().getCurrentUserIdentity(),
                        PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV, Privilege.NODE_PRIV), Operator.OR))) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        final List<List<String>> rows = Lists.newArrayList();
        if (!Config.isCloudMode()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_CLOUD_MODE);
            return new ShowResultSet(getMetaData(), rows);
        }

        List<String> clusterNames = null;
        clusterNames = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames();

        final Set<String> clusterNameSet = Sets.newTreeSet();
        clusterNameSet.addAll(clusterNames);

        for (String clusterName : clusterNameSet) {
            ArrayList<String> row = Lists.newArrayList(clusterName);
            // current_used, users
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkCloudPriv(ConnectContext.get().getCurrentUserIdentity(), clusterName,
                            PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
                continue;
            }
            String clusterNameFromCtx = "";
            try {
                clusterNameFromCtx = ctx.getCloudCluster();
            } catch (ComputeGroupException e) {
                LOG.warn("failed to get cluster name", e);
            }
            row.add(clusterName.equals(clusterNameFromCtx) ? "TRUE" : "FALSE");
            List<String> users = Env.getCurrentEnv().getAuth().getCloudClusterUsers(clusterName);
            // non-root do not display root information
            if (!Auth.ROOT_USER.equals(ctx.getQualifiedUser())) {
                users.remove(Auth.ROOT_USER);
            }
            // common user, not admin
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx.getCurrentUserIdentity(),
                    PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV), Operator.OR))) {
                users.removeIf(user -> !user.equals(ClusterNamespace.getNameFromFullName(ctx.getQualifiedUser())));
            }

            String result = Joiner.on(", ").join(users);
            row.add(result);
            int backendNum = ((CloudSystemInfoService) Env.getCurrentEnv().getCurrentSystemInfo())
                    .getBackendsByClusterName(clusterName).size();
            row.add(String.valueOf(backendNum));
            rows.add(row);
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowClustersCommand(this, context);
    }

    /**
     * getMetaData()
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ImmutableList<String> titleNames = null;
        if (isComputeGroup) {
            titleNames = COMPUTE_GROUP_TITLE_NAMES;
        } else {
            titleNames = CLUSTER_TITLE_NAMES;
        }

        for (String title : titleNames) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }
}

