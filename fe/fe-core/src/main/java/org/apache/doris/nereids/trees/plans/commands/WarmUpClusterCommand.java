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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Triple;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.WarmUpItem;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * WarmUpClusterCommand
 */
public class WarmUpClusterCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(WarmUpClusterCommand.class);
    private final List<WarmUpItem> warmUpItems;
    private final String srcCluster;
    private final String dstCluster;
    private final boolean isForce;
    private boolean isWarmUpWithTable;
    private List<Triple<String, String, String>> tables = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();

    /**
     * WarmUpClusterCommand
     */
    public WarmUpClusterCommand(List<WarmUpItem> warmUpItems,
                                String srcCluster,
                                String dstCluster,
                                boolean isForce,
                                boolean isWarmUpWithTable) {
        super(PlanType.WARM_UP_CLUSTER_COMMAND);
        this.warmUpItems = warmUpItems;
        this.srcCluster = srcCluster;
        this.dstCluster = dstCluster;
        this.isForce = isForce;
        this.isWarmUpWithTable = isWarmUpWithTable;
    }

    public WarmUpClusterCommand(List<WarmUpItem> warmUpItems,
                                String srcCluster,
                                String dstCluster,
                                boolean isForce,
                                boolean isWarmUpWithTable,
                                Map<String, String> properties) {
        this(warmUpItems, srcCluster, dstCluster, isForce, isWarmUpWithTable);
        this.properties = properties;
    }

    public List<WarmUpItem> getWarmUpItems() {
        return warmUpItems;
    }

    public String getSrcCluster() {
        return srcCluster;
    }

    public String getDstCluster() {
        return dstCluster;
    }

    public boolean isForce() {
        return isForce;
    }

    public boolean isWarmUpWithTable() {
        return isWarmUpWithTable;
    }

    public List<Triple<String, String, String>> getTables() {
        return tables;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        handleWarmUp(ctx, executor);
    }

    /**
     * validate
     */
    public void validate(ConnectContext connectContext) throws UserException {
        if (!Config.isCloudMode()) {
            throw new UserException("The sql is illegal in disk mode ");
        }

        if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).containClusterName(dstCluster)) {
            throw new AnalysisException("The dstClusterName " + dstCluster + " doesn't exist");
        }

        if (!isWarmUpWithTable
                && !((CloudSystemInfoService) Env.getCurrentSystemInfo()).containClusterName(srcCluster)) {
            boolean contains = false;
            try {
                contains = ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().containsCluster(srcCluster);
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }
            if (!contains) {
                throw new AnalysisException("The srcClusterName doesn't exist");
            }
        }

        if (!isWarmUpWithTable && Objects.equals(dstCluster, srcCluster)) {
            throw new AnalysisException("The dstClusterName: " + dstCluster
                + " is same with srcClusterName: " + srcCluster);
        }

        if (isWarmUpWithTable) {
            for (WarmUpItem warmUpItem : warmUpItems) {
                TableNameInfo tableNameInfo = warmUpItem.getTableNameInfo();
                String partitionName = warmUpItem.getPartitionName();
                tableNameInfo.analyze(connectContext);
                String dbName = tableNameInfo.getDb();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR, dbName);
                }
                Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
                if (db == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR, dbName);
                }
                OlapTable table = (OlapTable) db.getTableNullable(tableNameInfo.getTbl());
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableNameInfo.getTbl());
                }
                if (partitionName.length() != 0 && !table.containsPartition(partitionName)) {
                    throw new AnalysisException("The partition " + partitionName + " doesn't exist");
                }
                tables.add(Triple.of(dbName, tableNameInfo.getTbl(), partitionName));
            }
        }
    }

    private void handleWarmUp(ConnectContext ctx, StmtExecutor executor) throws IOException {
        long jobId = -1;
        try {
            jobId = ((CloudEnv) ctx.getEnv()).getCacheHotspotMgr().createJob(this);
            ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
            builder.addColumn(new Column("JobId", ScalarType.createVarchar(30)));
            List<List<String>> infos = Lists.newArrayList();
            List<String> info = Lists.newArrayList();
            info.add(String.valueOf(jobId));
            infos.add(info);
            ShowResultSet resultSet = new ShowResultSet(builder.build(), infos);
            if (resultSet == null) {
                // state changed in execute
                return;
            }
            if (executor.isProxy()) {
                executor.setProxyShowResultSet(resultSet);
                return;
            }
            executor.sendResultSet(resultSet);
        } catch (AnalysisException e) {
            LOG.info("failed to create a warm up job, error: {}", e.getMessage());
            ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitWarmUpClusterCommand(this, context);
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
