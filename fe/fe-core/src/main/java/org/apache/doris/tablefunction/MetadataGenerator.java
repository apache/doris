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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.HivePartitionValues;
import org.apache.doris.datasource.hudi.source.HudiCachedMetaClientProcessor;
import org.apache.doris.datasource.hudi.source.HudiMetadataCacheMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergMetadataCache;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskRefreshMode;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.AsyncMvMetrics;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVJobManager;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;
import org.apache.doris.mtmv.MTMVRefreshTriggerInfo;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.mtmv.SyncMvMetrics;
import org.apache.doris.mysql.privilege.PrivObject;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.mysql.privilege.User;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.FrontendConjunctsUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.plsql.metastore.PlsqlManager;
import org.apache.doris.plsql.metastore.PlsqlProcedureKey;
import org.apache.doris.plsql.metastore.PlsqlStoredProcedure;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.AnalysisState;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.FrontendService.Client;
import org.apache.doris.thrift.TBackendsMetadataParams;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.THudiMetadataParams;
import org.apache.doris.thrift.THudiQueryType;
import org.apache.doris.thrift.TJobsMetadataParams;
import org.apache.doris.thrift.TMaterializedViewsMetadataParams;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPartitionValuesMetadataParams;
import org.apache.doris.thrift.TPartitionsMetadataParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTasksMetadataParams;
import org.apache.doris.thrift.TUserIdentity;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MetadataGenerator {
    private static final Logger LOG = LogManager.getLogger(MetadataGenerator.class);

    private static final ImmutableMap<String, Integer> ACTIVE_QUERIES_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> WORKLOAD_GROUPS_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> ROUTINE_INFO_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> WORKLOAD_SCHED_POLICY_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> TABLE_OPTIONS_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> WORKLOAD_GROUP_PRIVILEGES_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> TABLE_PROPERTIES_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> META_CACHE_STATS_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> PARTITIONS_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> VIEW_DEPENDENCY_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> ASYNC_MVIEW_STATUS_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> ACTIVITY_ASYNC_MVIEW_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> MVIEW_TASKS_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> SYNC_MVIEW_STATUS_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> ACTIVITY_SYNC_MVIEW_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> ANALYZE_TABLE_LEVEL_STATUS_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> ANALYZE_JOB_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> ANALYZE_TASK_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> ACTIVITY_AUTO_ANALYZE_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> GRANTS_TO_ROLES_COLUMN_TO_INDEX;
    private static final ImmutableMap<String, Integer> GRANTS_TO_USERS_COLUMN_TO_INDEX;

    static {
        Builder<String, Integer> activeQueriesbuilder = new Builder();
        List<Column> activeQueriesColList = SchemaTable.TABLE_MAP.get("active_queries").getFullSchema();
        for (int i = 0; i < activeQueriesColList.size(); i++) {
            activeQueriesbuilder.put(activeQueriesColList.get(i).getName().toLowerCase(), i);
        }
        ACTIVE_QUERIES_COLUMN_TO_INDEX = activeQueriesbuilder.build();

        Builder<String, Integer> workloadGroupBuilder = new Builder();
        for (int i = 0; i < WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.size(); i++) {
            workloadGroupBuilder.put(WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.get(i).toLowerCase(), i);
        }
        WORKLOAD_GROUPS_COLUMN_TO_INDEX = workloadGroupBuilder.build();

        Builder<String, Integer> routineInfoBuilder = new Builder();
        for (int i = 0; i < PlsqlManager.ROUTINE_INFO_TITLE_NAMES.size(); i++) {
            routineInfoBuilder.put(PlsqlManager.ROUTINE_INFO_TITLE_NAMES.get(i).toLowerCase(), i);
        }
        ROUTINE_INFO_COLUMN_TO_INDEX = routineInfoBuilder.build();

        Builder<String, Integer> policyBuilder = new Builder();
        List<Column> policyColList = SchemaTable.TABLE_MAP.get("workload_policy").getFullSchema();
        for (int i = 0; i < policyColList.size(); i++) {
            policyBuilder.put(policyColList.get(i).getName().toLowerCase(), i);
        }
        WORKLOAD_SCHED_POLICY_COLUMN_TO_INDEX = policyBuilder.build();

        Builder<String, Integer> optionBuilder = new Builder();
        List<Column> optionColList = SchemaTable.TABLE_MAP.get("table_options").getFullSchema();
        for (int i = 0; i < optionColList.size(); i++) {
            optionBuilder.put(optionColList.get(i).getName().toLowerCase(), i);
        }
        TABLE_OPTIONS_COLUMN_TO_INDEX = optionBuilder.build();

        Builder<String, Integer> wgPrivsBuilder = new Builder();
        List<Column> wgPrivsColList = SchemaTable.TABLE_MAP.get("workload_group_privileges").getFullSchema();
        for (int i = 0; i < wgPrivsColList.size(); i++) {
            wgPrivsBuilder.put(wgPrivsColList.get(i).getName().toLowerCase(), i);
        }
        WORKLOAD_GROUP_PRIVILEGES_COLUMN_TO_INDEX = wgPrivsBuilder.build();

        Builder<String, Integer> propertiesBuilder = new Builder();
        List<Column> propertiesColList = SchemaTable.TABLE_MAP.get("table_properties").getFullSchema();
        for (int i = 0; i < propertiesColList.size(); i++) {
            propertiesBuilder.put(propertiesColList.get(i).getName().toLowerCase(), i);
        }
        TABLE_PROPERTIES_COLUMN_TO_INDEX = propertiesBuilder.build();

        Builder<String, Integer> metaCacheBuilder = new Builder();
        List<Column> metaCacheColList = SchemaTable.TABLE_MAP.get("catalog_meta_cache_statistics").getFullSchema();
        for (int i = 0; i < metaCacheColList.size(); i++) {
            metaCacheBuilder.put(metaCacheColList.get(i).getName().toLowerCase(), i);
        }
        META_CACHE_STATS_COLUMN_TO_INDEX = metaCacheBuilder.build();

        Builder<String, Integer> partitionsBuilder = new Builder();
        List<Column> partitionsColList = SchemaTable.TABLE_MAP.get("partitions").getFullSchema();
        for (int i = 0; i < partitionsColList.size(); i++) {
            partitionsBuilder.put(partitionsColList.get(i).getName().toLowerCase(), i);
        }
        PARTITIONS_COLUMN_TO_INDEX = partitionsBuilder.build();

        Builder<String, Integer> viewDependencyBuilder = new Builder();
        List<Column> viewDependencyBuilderColList = SchemaTable.TABLE_MAP.get("view_dependency").getFullSchema();
        for (int i = 0; i < viewDependencyBuilderColList.size(); i++) {
            viewDependencyBuilder.put(viewDependencyBuilderColList.get(i).getName().toLowerCase(), i);
        }
        VIEW_DEPENDENCY_COLUMN_TO_INDEX = viewDependencyBuilder.build();


        Builder<String, Integer> asyncMviewStatusBuilder = new Builder();
        List<Column> asyncMviewStatusBuilderColList = SchemaTable.TABLE_MAP.get("async_mview_status").getFullSchema();
        for (int i = 0; i < asyncMviewStatusBuilderColList.size(); i++) {
            asyncMviewStatusBuilder.put(asyncMviewStatusBuilderColList.get(i).getName().toLowerCase(), i);
        }
        ASYNC_MVIEW_STATUS_COLUMN_TO_INDEX = asyncMviewStatusBuilder.build();

        Builder<String, Integer> activityAsyncMviewBuilder = new Builder();
        List<Column> activityAsyncMviewBuilderColList = SchemaTable.TABLE_MAP.get("activity_async_mview")
                .getFullSchema();
        for (int i = 0; i < activityAsyncMviewBuilderColList.size(); i++) {
            activityAsyncMviewBuilder.put(activityAsyncMviewBuilderColList.get(i).getName().toLowerCase(), i);
        }
        ACTIVITY_ASYNC_MVIEW_COLUMN_TO_INDEX = activityAsyncMviewBuilder.build();

        Builder<String, Integer> mviewTasksBuilder = new Builder();
        List<Column> mviewTasksBuilderColList = SchemaTable.TABLE_MAP.get("mview_tasks").getFullSchema();
        for (int i = 0; i < mviewTasksBuilderColList.size(); i++) {
            mviewTasksBuilder.put(mviewTasksBuilderColList.get(i).getName().toLowerCase(), i);
        }
        MVIEW_TASKS_COLUMN_TO_INDEX = mviewTasksBuilder.build();

        Builder<String, Integer> syncMviewStatusBuilder = new Builder();
        List<Column> syncMviewStatusBuilderColList = SchemaTable.TABLE_MAP.get("sync_mview_status").getFullSchema();
        for (int i = 0; i < syncMviewStatusBuilderColList.size(); i++) {
            syncMviewStatusBuilder.put(syncMviewStatusBuilderColList.get(i).getName().toLowerCase(), i);
        }
        SYNC_MVIEW_STATUS_COLUMN_TO_INDEX = syncMviewStatusBuilder.build();

        Builder<String, Integer> activitySyncMviewBuilder = new Builder();
        List<Column> activitySyncMviewBuilderColList = SchemaTable.TABLE_MAP.get("activity_sync_mview").getFullSchema();
        for (int i = 0; i < activitySyncMviewBuilderColList.size(); i++) {
            activitySyncMviewBuilder.put(activitySyncMviewBuilderColList.get(i).getName().toLowerCase(), i);
        }
        ACTIVITY_SYNC_MVIEW_COLUMN_TO_INDEX = activitySyncMviewBuilder.build();

        Builder<String, Integer> analyzeTableLevelStatusBuilder = new Builder();
        List<Column> analyzeTableLevelStatusBuilderColList = SchemaTable.TABLE_MAP.get("analyze_table_level_status")
                .getFullSchema();
        for (int i = 0; i < analyzeTableLevelStatusBuilderColList.size(); i++) {
            analyzeTableLevelStatusBuilder.put(analyzeTableLevelStatusBuilderColList.get(i).getName().toLowerCase(), i);
        }
        ANALYZE_TABLE_LEVEL_STATUS_COLUMN_TO_INDEX = analyzeTableLevelStatusBuilder.build();

        Builder<String, Integer> analyzeJobBuilder = new Builder();
        List<Column> analyzeJobBuilderColList = SchemaTable.TABLE_MAP.get("analyze_job").getFullSchema();
        for (int i = 0; i < analyzeJobBuilderColList.size(); i++) {
            analyzeJobBuilder.put(analyzeJobBuilderColList.get(i).getName().toLowerCase(), i);
        }
        ANALYZE_JOB_COLUMN_TO_INDEX = analyzeJobBuilder.build();

        Builder<String, Integer> analyzeTaskBuilder = new Builder();
        List<Column> analyzeTaskBuilderColList = SchemaTable.TABLE_MAP.get("analyze_task").getFullSchema();
        for (int i = 0; i < analyzeTaskBuilderColList.size(); i++) {
            analyzeTaskBuilder.put(analyzeTaskBuilderColList.get(i).getName().toLowerCase(), i);
        }
        ANALYZE_TASK_COLUMN_TO_INDEX = analyzeTaskBuilder.build();

        Builder<String, Integer> activityAutoAnalyzeBuilder = new Builder();
        List<Column> activityAutoAnalyzeBuilderColList = SchemaTable.TABLE_MAP.get("analyze_task").getFullSchema();
        for (int i = 0; i < activityAutoAnalyzeBuilderColList.size(); i++) {
            activityAutoAnalyzeBuilder.put(activityAutoAnalyzeBuilderColList.get(i).getName().toLowerCase(), i);
        }
        ACTIVITY_AUTO_ANALYZE_COLUMN_TO_INDEX = activityAutoAnalyzeBuilder.build();

        Builder<String, Integer> grantsToRolesBuilder = new Builder();
        List<Column> grantsToRolesBuilderColList = SchemaTable.TABLE_MAP.get("grants_to_roles").getFullSchema();
        for (int i = 0; i < grantsToRolesBuilderColList.size(); i++) {
            grantsToRolesBuilder.put(grantsToRolesBuilderColList.get(i).getName().toLowerCase(), i);
        }
        GRANTS_TO_ROLES_COLUMN_TO_INDEX = grantsToRolesBuilder.build();

        Builder<String, Integer> grantsToUsersBuilder = new Builder();
        List<Column> grantsToUsersBuilderColList = SchemaTable.TABLE_MAP.get("grants_to_users").getFullSchema();
        for (int i = 0; i < grantsToUsersBuilderColList.size(); i++) {
            grantsToUsersBuilder.put(grantsToUsersBuilderColList.get(i).getName().toLowerCase(), i);
        }
        GRANTS_TO_USERS_COLUMN_TO_INDEX = grantsToUsersBuilder.build();
    }

    public static TFetchSchemaTableDataResult getMetadataTable(TFetchSchemaTableDataRequest request) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetadataTable() start.");
        }
        if (!request.isSetMetadaTableParams() || !request.getMetadaTableParams().isSetMetadataType()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Metadata table params is not set.");
            }
            return errorResult("Metadata table params is not set. ");
        }
        TFetchSchemaTableDataResult result;
        TMetadataTableRequestParams params = request.getMetadaTableParams();
        TMetadataType metadataType = request.getMetadaTableParams().getMetadataType();
        switch (metadataType) {
            case HUDI:
                result = hudiMetadataResult(params);
                break;
            case BACKENDS:
                result = backendsMetadataResult(params);
                break;
            case FRONTENDS:
                result = frontendsMetadataResult(params);
                break;
            case FRONTENDS_DISKS:
                result = frontendsDisksMetadataResult(params);
                break;
            case CATALOGS:
                result = catalogsMetadataResult(params);
                break;
            case MATERIALIZED_VIEWS:
                result = mtmvMetadataResult(params);
                break;
            case PARTITIONS:
                result = partitionMetadataResult(params);
                break;
            case JOBS:
                result = jobMetadataResult(params);
                break;
            case TASKS:
                result = taskMetadataResult(params);
                break;
            case PARTITION_VALUES:
                result = partitionValuesMetadataResult(params);
                break;
            default:
                return errorResult("Metadata table params is not set.");
        }
        if (result.getStatus().getStatusCode() == TStatusCode.OK) {
            if (metadataType != TMetadataType.PARTITION_VALUES) {
                // partition_values' result already sorted by column names
                filterColumns(result, params.getColumnsName(), params.getMetadataType(), params);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetadataTable() end.");
        }
        return result;
    }

    public static TFetchSchemaTableDataResult getSchemaTableData(TFetchSchemaTableDataRequest request)
            throws TException {
        if (!request.isSetSchemaTableParams()) {
            return errorResult("schema table params is not set.");
        }
        TFetchSchemaTableDataResult result;
        TSchemaTableRequestParams schemaTableParams = request.getSchemaTableParams();
        ImmutableMap<String, Integer> columnIndex;
        switch (request.getSchemaTableName()) {
            case ACTIVE_QUERIES:
                result = queriesMetadataResult(schemaTableParams, request);
                columnIndex = ACTIVE_QUERIES_COLUMN_TO_INDEX;
                break;
            case WORKLOAD_GROUPS:
                result = workloadGroupsMetadataResult(schemaTableParams);
                columnIndex = WORKLOAD_GROUPS_COLUMN_TO_INDEX;
                break;
            case ROUTINES_INFO:
                result = routineInfoMetadataResult(schemaTableParams);
                columnIndex = ROUTINE_INFO_COLUMN_TO_INDEX;
                break;
            case WORKLOAD_SCHEDULE_POLICY:
                result = workloadSchedPolicyMetadataResult(schemaTableParams);
                columnIndex = WORKLOAD_SCHED_POLICY_COLUMN_TO_INDEX;
                break;
            case TABLE_OPTIONS:
                result = tableOptionsMetadataResult(schemaTableParams);
                columnIndex = TABLE_OPTIONS_COLUMN_TO_INDEX;
                break;
            case WORKLOAD_GROUP_PRIVILEGES:
                result = workloadGroupPrivsMetadataResult(schemaTableParams);
                columnIndex = WORKLOAD_GROUP_PRIVILEGES_COLUMN_TO_INDEX;
                break;
            case TABLE_PROPERTIES:
                result = tablePropertiesMetadataResult(schemaTableParams);
                columnIndex = TABLE_PROPERTIES_COLUMN_TO_INDEX;
                break;
            case CATALOG_META_CACHE_STATS:
                result = metaCacheStatsMetadataResult(schemaTableParams);
                columnIndex = META_CACHE_STATS_COLUMN_TO_INDEX;
                break;
            case PARTITIONS:
                result = partitionsMetadataResult(schemaTableParams);
                columnIndex = PARTITIONS_COLUMN_TO_INDEX;
                break;
            case VIEW_DEPENDENCY:
                result = viewDependencyMetadataResult(schemaTableParams);
                columnIndex = VIEW_DEPENDENCY_COLUMN_TO_INDEX;
                break;
            case ASYNC_MVIEW_STATUS:
                result = asyncMviewStatusMetadataResult(schemaTableParams);
                columnIndex = ASYNC_MVIEW_STATUS_COLUMN_TO_INDEX;
                break;
            case ACTIVITY_ASYNC_MVIEW:
                result = activityAsyncMviewMetadataResult(schemaTableParams);
                columnIndex = ACTIVITY_ASYNC_MVIEW_COLUMN_TO_INDEX;
                break;
            case MVIEW_TASKS:
                result = mviewTasksMetadataResult(schemaTableParams);
                columnIndex = MVIEW_TASKS_COLUMN_TO_INDEX;
                break;
            case SYNC_MVIEW_STATUS:
                result = syncMviewStatusMetadataResult(schemaTableParams);
                columnIndex = SYNC_MVIEW_STATUS_COLUMN_TO_INDEX;
                break;
            case ACTIVITY_SYNC_MVIEW:
                result = activitySyncMviewMetadataResult(schemaTableParams);
                columnIndex = ACTIVITY_SYNC_MVIEW_COLUMN_TO_INDEX;
                break;
            case ANALYZE_TABLE_LEVEL_STATUS:
                result = analyzeTableLevelStatusMetadataResult(schemaTableParams);
                columnIndex = ANALYZE_TABLE_LEVEL_STATUS_COLUMN_TO_INDEX;
                break;
            case ANALYZE_JOB:
                result = analyzeJobMetadataResult(schemaTableParams);
                columnIndex = ANALYZE_JOB_COLUMN_TO_INDEX;
                break;
            case ANALYZE_TASK:
                result = analyzeTaskMetadataResult(schemaTableParams);
                columnIndex = ANALYZE_TASK_COLUMN_TO_INDEX;
                break;
            case ACTIVITY_AUTO_ANALYZE:
                result = activityAutoAnalyzeMetadataResult(schemaTableParams);
                columnIndex = ACTIVITY_AUTO_ANALYZE_COLUMN_TO_INDEX;
                break;
            case GRANTS_TO_ROLES:
                result = grantsToRolesMetadataResult(schemaTableParams);
                columnIndex = GRANTS_TO_ROLES_COLUMN_TO_INDEX;
                break;
            case GRANTS_TO_USERS:
                result = grantsToUsersMetadataResult(schemaTableParams);
                columnIndex = GRANTS_TO_USERS_COLUMN_TO_INDEX;
                break;
            default:
                return errorResult("invalid schema table name.");
        }
        if (schemaTableParams.isSetColumnsName() && result.getStatus().getStatusCode() == TStatusCode.OK) {
            filterColumns(result, schemaTableParams.getColumnsName(), columnIndex);
        }
        return result;
    }

    @NotNull
    public static TFetchSchemaTableDataResult errorResult(String msg) {
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
        result.status.addToErrorMsgs(msg);
        return result;
    }

    private static TFetchSchemaTableDataResult hudiMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetHudiMetadataParams()) {
            return errorResult("Hudi metadata params is not set.");
        }

        THudiMetadataParams hudiMetadataParams = params.getHudiMetadataParams();
        THudiQueryType hudiQueryType = hudiMetadataParams.getHudiQueryType();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(hudiMetadataParams.getCatalog());
        if (catalog == null) {
            return errorResult("The specified catalog does not exist:" + hudiMetadataParams.getCatalog());
        }
        if (!(catalog instanceof ExternalCatalog)) {
            return errorResult("The specified catalog is not an external catalog: "
                    + hudiMetadataParams.getCatalog());
        }

        ExternalTable dorisTable;
        try {
            dorisTable = (ExternalTable) catalog.getDbOrAnalysisException(hudiMetadataParams.getDatabase())
                    .getTableOrAnalysisException(hudiMetadataParams.getTable());
        } catch (AnalysisException e) {
            return errorResult("The specified db or table does not exist");
        }

        if (!(dorisTable instanceof HMSExternalTable)) {
            return errorResult("The specified table is not a hudi table: " + hudiMetadataParams.getTable());
        }

        HudiCachedMetaClientProcessor hudiMetadataCache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getHudiMetadataCacheMgr().getHudiMetaClientProcessor(catalog);
        String hudiBasePathString = ((HMSExternalCatalog) catalog).getClient()
                .getTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName()).getSd().getLocation();
        Configuration conf = ((HMSExternalCatalog) catalog).getConfiguration();

        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        switch (hudiQueryType) {
            case TIMELINE:
                HoodieTimeline timeline = hudiMetadataCache.getHoodieTableMetaClient(dorisTable.getOrBuildNameMapping(),
                        hudiBasePathString, conf).getActiveTimeline();
                for (HoodieInstant instant : timeline.getInstants()) {
                    TRow trow = new TRow();
                    trow.addToColumnValue(new TCell().setStringVal(instant.requestedTime()));
                    trow.addToColumnValue(new TCell().setStringVal(instant.getAction()));
                    trow.addToColumnValue(new TCell().setStringVal(instant.getState().name()));
                    trow.addToColumnValue(new TCell().setStringVal(instant.getCompletionTime()));
                    dataBatch.add(trow);
                }
                break;
            default:
                return errorResult("Unsupported hudi inspect type: " + hudiQueryType);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult backendsMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetBackendsMetadataParams()) {
            return errorResult("backends metadata param is not set.");
        }
        TBackendsMetadataParams backendsParam = params.getBackendsMetadataParams();
        final SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        List<Long> backendIds = systemInfoService.getAllBackendIds(false);

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        long start = System.currentTimeMillis();
        Stopwatch watch = Stopwatch.createUnstarted();

        List<TRow> dataBatch = Lists.newArrayList();
        for (long backendId : backendIds) {
            Backend backend = systemInfoService.getBackend(backendId);
            if (backend == null) {
                continue;
            }

            watch.start();
            Integer tabletNum = systemInfoService.getTabletNumByBackendId(backendId);
            watch.stop();

            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(backendId));
            trow.addToColumnValue(new TCell().setStringVal(backend.getHost()));
            if (Strings.isNullOrEmpty(backendsParam.cluster_name)) {
                trow.addToColumnValue(new TCell().setIntVal(backend.getHeartbeatPort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getBePort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getHttpPort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getBrpcPort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getArrowFlightSqlPort()));
            }
            trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(backend.getLastStartTime())));
            trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(backend.getLastUpdateMs())));
            trow.addToColumnValue(new TCell().setBoolVal(backend.isAlive()));
            trow.addToColumnValue(new TCell().setBoolVal(backend.isDecommissioned()));
            trow.addToColumnValue(new TCell().setLongVal(tabletNum));

            // capacity
            // data used
            trow.addToColumnValue(new TCell().setLongVal(backend.getDataUsedCapacityB()));

            // trash used
            trow.addToColumnValue(new TCell().setLongVal(backend.getTrashUsedCapacityB()));

            // available
            long availB = backend.getAvailableCapacityB();
            trow.addToColumnValue(new TCell().setLongVal(availB));

            // total
            long totalB = backend.getTotalCapacityB();
            trow.addToColumnValue(new TCell().setLongVal(totalB));

            // used percent
            double used = 0.0;
            if (totalB <= 0) {
                used = 0.0;
            } else {
                used = (double) (totalB - availB) * 100 / totalB;
            }
            trow.addToColumnValue(new TCell().setDoubleVal(used));
            trow.addToColumnValue(new TCell().setDoubleVal(backend.getMaxDiskUsedPct() * 100));

            // remote used capacity
            trow.addToColumnValue(new TCell().setLongVal(backend.getRemoteUsedCapacityB()));

            // tags
            trow.addToColumnValue(new TCell().setStringVal(backend.getTagMapString()));
            // err msg
            trow.addToColumnValue(new TCell().setStringVal(backend.getHeartbeatErrMsg()));
            // version
            trow.addToColumnValue(new TCell().setStringVal(backend.getVersion()));
            // status
            trow.addToColumnValue(new TCell().setStringVal(new Gson().toJson(backend.getBackendStatus())));
            // heartbeat failure counter
            trow.addToColumnValue(new TCell().setIntVal(backend.getHeartbeatFailureCounter()));

            // node role, show the value only when backend is alive.
            trow.addToColumnValue(new TCell().setStringVal(backend.isAlive() ? backend.getNodeRoleTag().value : ""));

            dataBatch.add(trow);
        }

        // backends proc node get result too slow, add log to observer.
        if (LOG.isDebugEnabled()) {
            LOG.debug("backends proc get tablet num cost: {}, total cost: {}",
                    watch.elapsed(TimeUnit.MILLISECONDS), (System.currentTimeMillis() - start));
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult frontendsMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetFrontendsMetadataParams()) {
            return errorResult("frontends metadata param is not set.");
        }

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        List<TRow> dataBatch = Lists.newArrayList();
        List<List<String>> infos = Lists.newArrayList();
        FrontendsProcNode.getFrontendsInfo(Env.getCurrentEnv(), infos);
        for (List<String> info : infos) {
            TRow trow = new TRow();
            for (String item : info) {
                trow.addToColumnValue(new TCell().setStringVal(item));
            }
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult frontendsDisksMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetFrontendsMetadataParams()) {
            return errorResult("frontends metadata param is not set.");
        }

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        List<TRow> dataBatch = Lists.newArrayList();
        List<List<String>> infos = Lists.newArrayList();
        FrontendsProcNode.getFrontendsDiskInfo(Env.getCurrentEnv(), infos);
        for (List<String> info : infos) {
            TRow trow = new TRow();
            for (String item : info) {
                trow.addToColumnValue(new TCell().setStringVal(item));
            }
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult catalogsMetadataResult(TMetadataTableRequestParams params) {
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        UserIdentity currentUserIdentity = UserIdentity.fromThrift(params.getCurrentUserIdent());
        List<CatalogIf> info = Env.getCurrentEnv().getCatalogMgr().listCatalogsWithCheckPriv(currentUserIdentity);
        List<TRow> dataBatch = Lists.newArrayList();
        for (CatalogIf catalog : info) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(catalog.getId()));
            trow.addToColumnValue(new TCell().setStringVal(catalog.getName()));
            trow.addToColumnValue(new TCell().setStringVal(catalog.getType()));

            Map<String, String> properties = CatalogMgr.getCatalogPropertiesWithPrintable(catalog);
            for (Entry<String, String> entry : properties.entrySet()) {
                TRow subTrow = new TRow(trow);
                subTrow.addToColumnValue(new TCell().setStringVal(entry.getKey()));
                subTrow.addToColumnValue(new TCell().setStringVal(entry.getValue()));
                dataBatch.add(subTrow);
            }
            if (properties.isEmpty()) {
                trow.addToColumnValue(new TCell().setStringVal("NULL"));
                trow.addToColumnValue(new TCell().setStringVal("NULL"));
                dataBatch.add(trow);
            }
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult workloadGroupsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        List<List<String>> workloadGroupsInfo = Env.getCurrentEnv().getWorkloadGroupMgr()
                .getResourcesInfo(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (List<String> rGroupsInfo : workloadGroupsInfo) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(0)))); // id
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(1))); // name
            trow.addToColumnValue(new TCell().setStringVal((rGroupsInfo.get(2)))); // min_cpu_percent
            trow.addToColumnValue(new TCell().setStringVal((rGroupsInfo.get(3)))); // max_cpu_percent
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(4))); // min_memory_percent
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(5))); // max_memory_percent
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(6)))); // max concurrent
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(7)))); // max queue size
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(8)))); // queue timeout
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(9)))); // scan thread num
            // max remote scan thread num
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(10))));
            // min remote scan thread num
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(11))));
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(12))); // spill low watermark
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(13))); // spill high watermark
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(14))); // compute group
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(15)))); // read bytes per second
            trow.addToColumnValue(
                    new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(16)))); // remote read bytes per second
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult viewDependencyMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> viewSchemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "VIEW_SCHEMA");
        List<Expression> viewTypeConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "VIEW_TYPE");
        List<Expression> viewNameConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "VIEW_NAME");
        Collection<DatabaseIf<? extends TableIf>> allDbs = Env.getCurrentEnv().getInternalCatalog().getAllDbs();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        for (DatabaseIf<? extends TableIf> db : allDbs) {
            String dbName = db.getFullName();
            if (FrontendConjunctsUtils.isFiltered(viewSchemaConjuncts, "VIEW_SCHEMA", dbName)) {
                continue;
            }
            List<? extends TableIf> tables = db.getTables();
            for (TableIf table : tables) {
                if (FrontendConjunctsUtils.isFiltered(viewTypeConjuncts, "VIEW_TYPE", table.getType().name())) {
                    continue;
                }
                if (table instanceof MTMV) {
                    String tableName = table.getName();
                    if (FrontendConjunctsUtils.isFiltered(viewNameConjuncts, "VIEW_NAME", tableName)) {
                        continue;
                    }
                    MTMVRelation relation = ((MTMV) table).getRelation();
                    Set<BaseTableInfo> tablesOneLevel = relation.getBaseTablesOneLevel();
                    for (BaseTableInfo info : tablesOneLevel) {
                        TRow trow = new TRow();
                        trow.addToColumnValue(new TCell().setStringVal(InternalCatalog.INTERNAL_CATALOG_NAME));
                        trow.addToColumnValue(new TCell().setStringVal(dbName));
                        trow.addToColumnValue(new TCell().setStringVal(tableName));
                        trow.addToColumnValue(new TCell().setStringVal(table.getType().name()));
                        trow.addToColumnValue(new TCell().setStringVal(info.getCtlName()));
                        trow.addToColumnValue(new TCell().setStringVal(info.getDbName()));
                        trow.addToColumnValue(new TCell().setStringVal(info.getTableName()));
                        trow.addToColumnValue(new TCell().setStringVal(info.getType()));
                        dataBatch.add(trow);
                    }
                } else if (table instanceof View) {
                    String tableName = table.getName();
                    if (FrontendConjunctsUtils.isFiltered(viewNameConjuncts, "VIEW_NAME", tableName)) {
                        continue;
                    }
                    String inlineViewDef = ((View) table).getInlineViewDef();
                    Map<List<String>, TableIf> tablesMap = PlanUtils.tableCollect(inlineViewDef, ctx);
                    for (Entry<List<String>, TableIf> info : tablesMap.entrySet()) {
                        List<String> fullName = info.getKey();
                        TableIf tbl = info.getValue();
                        TRow trow = new TRow();
                        trow.addToColumnValue(new TCell().setStringVal("internal"));
                        trow.addToColumnValue(new TCell().setStringVal(dbName));
                        trow.addToColumnValue(new TCell().setStringVal(tableName));
                        trow.addToColumnValue(new TCell().setStringVal(table.getType().name()));
                        trow.addToColumnValue(new TCell().setStringVal(fullName.get(0)));
                        trow.addToColumnValue(new TCell().setStringVal(fullName.get(1)));
                        trow.addToColumnValue(new TCell().setStringVal(fullName.get(2)));
                        trow.addToColumnValue(new TCell().setStringVal(tbl.getType().name()));
                        dataBatch.add(trow);
                    }
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult asyncMviewStatusMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> mviewIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "ASYNC_MVIEW_ID");
        List<Expression> mviewSchemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts,
                "ASYNC_MVIEW_SCHEMA");
        List<Expression> mviewNameConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "ASYNC_MVIEW_NAME");
        Collection<DatabaseIf<? extends TableIf>> allDbs = Env.getCurrentEnv().getInternalCatalog().getAllDbs();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (DatabaseIf<? extends TableIf> db : allDbs) {
            String dbName = db.getFullName();
            if (FrontendConjunctsUtils.isFiltered(mviewSchemaConjuncts, "ASYNC_MVIEW_SCHEMA", dbName)) {
                continue;
            }
            List<? extends TableIf> tables = db.getTables();
            for (TableIf table : tables) {
                if (table instanceof MTMV) {
                    MTMV mtmv = (MTMV) table;
                    // todo: check auth
                    String tableName = table.getName();
                    if (FrontendConjunctsUtils.isFiltered(mviewNameConjuncts, "ASYNC_MVIEW_NAME", tableName)) {
                        continue;
                    }
                    long tableId = table.getId();
                    if (FrontendConjunctsUtils.isFiltered(mviewIdConjuncts, "ASYNC_MVIEW_ID", tableId)) {
                        continue;
                    }
                    TRow trow = new TRow();
                    MTMVStatus mtmvStatus = mtmv.getStatus();
                    // ASYNC_MVIEW_ID
                    trow.addToColumnValue(new TCell().setLongVal(tableId));
                    // ASYNC_MVIEW_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(InternalCatalog.INTERNAL_CATALOG_NAME));
                    // ASYNC_MVIEW_SCHEMA
                    trow.addToColumnValue(new TCell().setStringVal(dbName));
                    // ASYNC_MVIEW_NAME
                    trow.addToColumnValue(new TCell().setStringVal(tableName));
                    // IS_ACTIVE
                    trow.addToColumnValue(new TCell().setBoolVal(mtmvStatus.getState().equals(MTMVState.NORMAL)));
                    // INACTIVE_REASON
                    String inactiveReason = "";
                    if (mtmvStatus.getState().equals(MTMVState.INIT)) {
                        inactiveReason = "init";
                    } else if (mtmvStatus.getState().equals(MTMVState.SCHEMA_CHANGE)) {
                        inactiveReason = mtmvStatus.getSchemaChangeDetail();
                    }
                    trow.addToColumnValue(new TCell().setStringVal(inactiveReason));
                    // PCT_TABLES
                    List<String> pctTables = Lists.newArrayList();
                    if (!mtmv.getMvPartitionInfo().getPartitionType().equals(MTMVPartitionType.SELF_MANAGE)) {
                        BaseTableInfo relatedTableInfo = mtmv.getMvPartitionInfo().getRelatedTableInfo();
                        String relatedCol = mtmv.getMvPartitionInfo().getRelatedCol();
                        pctTables.add(String.format("%s.%s.%s.%s", relatedTableInfo.getCtlName(),
                                relatedTableInfo.getDbName(), relatedTableInfo.getTableName(), relatedCol));
                    }
                    trow.addToColumnValue(new TCell().setStringVal(GsonUtils.GSON.toJson(pctTables)));
                    // IS_SYNC_WITH_BASE_TABLES
                    MTMVRefreshContext mtmvRefreshContext;
                    try {
                        mtmvRefreshContext = MTMVRefreshContext.buildContext(mtmv);
                    } catch (AnalysisException e) {
                        LOG.warn(e.getMessage(), e);
                        continue;
                    }
                    List<String> needRefreshPartitions = MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                            mtmvRefreshContext, mtmv.getRelation().getBaseTablesOneLevel());
                    trow.addToColumnValue(new TCell().setBoolVal(CollectionUtils.isEmpty(needRefreshPartitions)));
                    // UNSYNC_PARTITIONS_IN_ASYNC_MVIEW
                    trow.addToColumnValue(new TCell().setStringVal(GsonUtils.GSON.toJson(needRefreshPartitions)));
                    // REFRESH_TYPE
                    MTMVRefreshTriggerInfo refreshTriggerInfo = mtmv.getRefreshInfo().getRefreshTriggerInfo();
                    trow.addToColumnValue(new TCell().setStringVal(refreshTriggerInfo.getRefreshTrigger()
                            .name()));
                    // REFRESH_JOB_STATUS
                    // SCHEDULE_PERIOD
                    String schedulePeriod = "";
                    if (refreshTriggerInfo.getRefreshTrigger().equals(RefreshTrigger.SCHEDULE)) {
                        schedulePeriod = refreshTriggerInfo.getIntervalTrigger().toString();
                    }
                    trow.addToColumnValue(new TCell().setStringVal(schedulePeriod));
                    // PARTITION_TYPE
                    trow.addToColumnValue(
                            new TCell().setStringVal(mtmv.getMvPartitionInfo().getPartitionType().name()));
                    // ROW_COUNT
                    trow.addToColumnValue(new TCell().setLongVal(mtmv.getRowCount()));
                    // PARTITION_COUNT
                    trow.addToColumnValue(new TCell().setLongVal(mtmv.getPartitionNum()));
                    // DEFINITION
                    trow.addToColumnValue(new TCell().setStringVal(mtmv.getQuerySql()));
                    // PROPERTIES
                    trow.addToColumnValue(new TCell().setStringVal(GsonUtils.GSON.toJson(mtmv.getMvProperties())));
                    dataBatch.add(trow);
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult activityAsyncMviewMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> mviewIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "ASYNC_MVIEW_ID");
        List<Expression> mviewSchemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts,
                "ASYNC_MVIEW_SCHEMA");
        List<Expression> mviewNameConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "ASYNC_MVIEW_NAME");
        Collection<DatabaseIf<? extends TableIf>> allDbs = Env.getCurrentEnv().getInternalCatalog().getAllDbs();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (DatabaseIf<? extends TableIf> db : allDbs) {
            String dbName = db.getFullName();
            if (FrontendConjunctsUtils.isFiltered(mviewSchemaConjuncts, "ASYNC_MVIEW_SCHEMA", dbName)) {
                continue;
            }
            List<? extends TableIf> tables = db.getTables();
            for (TableIf table : tables) {
                if (table instanceof MTMV) {
                    MTMV mtmv = (MTMV) table;
                    // todo: check auth
                    String tableName = table.getName();
                    if (FrontendConjunctsUtils.isFiltered(mviewNameConjuncts, "ASYNC_MVIEW_NAME", tableName)) {
                        continue;
                    }
                    long tableId = table.getId();
                    if (FrontendConjunctsUtils.isFiltered(mviewIdConjuncts, "ASYNC_MVIEW_ID", tableId)) {
                        continue;
                    }
                    TRow trow = new TRow();
                    AsyncMvMetrics asyncMvMetrics = mtmv.getAsyncMvMetrics();
                    // ASYNC_MVIEW_ID
                    trow.addToColumnValue(new TCell().setLongVal(tableId));
                    // ASYNC_MVIEW_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(InternalCatalog.INTERNAL_CATALOG_NAME));
                    // ASYNC_MVIEW_SCHEMA
                    trow.addToColumnValue(new TCell().setStringVal(dbName));
                    // ASYNC_MVIEW_NAME
                    trow.addToColumnValue(new TCell().setStringVal(tableName));
                    long rewriteFullSuccess = asyncMvMetrics.getRewriteFullSuccess().getValue();
                    long rewritePartialSuccess = asyncMvMetrics.getRewritePartialSuccess().getValue();
                    // REWRITE_SUCCESS
                    trow.addToColumnValue(new TCell().setLongVal(rewriteFullSuccess + rewritePartialSuccess));
                    // REWRITE_FULL_SUCCESS
                    trow.addToColumnValue(new TCell().setLongVal(rewriteFullSuccess));
                    // REWRITE_PARTIAL_SUCCESS
                    trow.addToColumnValue(new TCell().setLongVal(rewritePartialSuccess));
                    // REWRITE_SUCCESS_WITH_HINT
                    trow.addToColumnValue(
                            new TCell().setLongVal(asyncMvMetrics.getRewriteSuccessWithHint().getValue()));
                    long staleDate = asyncMvMetrics.getRewriteFailureStaleData().getValue();
                    long cboRejected = asyncMvMetrics.getRewriteFailureCboRejected().getValue();
                    long shapeMismatch = asyncMvMetrics.getRewriteFailureShapeMismatch().getValue();
                    long withHint = asyncMvMetrics.getRewriteFailureWithHint().getValue();
                    // REWRITE_FAILURE
                    trow.addToColumnValue(new TCell().setLongVal(staleDate + cboRejected + shapeMismatch + withHint));
                    // REWRITE_FAILURE_STALE_DATA
                    trow.addToColumnValue(new TCell().setLongVal(staleDate));
                    // REWRITE_FAILURE_SHAPE_MISMATCH
                    trow.addToColumnValue(new TCell().setLongVal(shapeMismatch));
                    // REWRITE_FAILURE_CBO_REJECTED
                    trow.addToColumnValue(new TCell().setLongVal(cboRejected));
                    // REWRITE_FAILURE_WITH_HINT
                    trow.addToColumnValue(new TCell().setLongVal(withHint));
                    // MANUAL_REFRESHES_ON_AUTO
                    trow.addToColumnValue(new TCell().setLongVal(asyncMvMetrics.getManualRefreshesOnAuto().getValue()));
                    // MANUAL_REFRESHES_ON_PARTITIONS
                    trow.addToColumnValue(
                            new TCell().setLongVal(asyncMvMetrics.getManualRefreshesOnPartitions().getValue()));
                    // MANUAL_REFRESHES_ON_COMPLETE
                    trow.addToColumnValue(
                            new TCell().setLongVal(asyncMvMetrics.getManualRefreshesOnComplete().getValue()));
                    // AUTO_REFRESHES_ON_SCHEDULE
                    trow.addToColumnValue(
                            new TCell().setLongVal(asyncMvMetrics.getAutoRefreshesOnSchedule().getValue()));
                    // AUTO_REFRESHES_ON_COMMIT
                    trow.addToColumnValue(new TCell().setLongVal(asyncMvMetrics.getAutoRefreshesOnCommit().getValue()));
                    // LAST_REWRITE_TIME
                    // todo long type ==> date type
                    trow.addToColumnValue(new TCell().setLongVal(asyncMvMetrics.getLastRewriteTime()));
                    // LAST_QUERY_TIME
                    trow.addToColumnValue(new TCell().setLongVal(asyncMvMetrics.getLastQueryTime()));
                    // REFRESHES_SKIPPED
                    trow.addToColumnValue(new TCell().setLongVal(asyncMvMetrics.getRefreshesSkipped().getValue()));
                    // REFRESHES_FAST
                    trow.addToColumnValue(new TCell().setLongVal(asyncMvMetrics.getRefreshesFast().getValue()));
                    // REFRESHES_PCT
                    trow.addToColumnValue(new TCell().setLongVal(asyncMvMetrics.getRefreshesPct().getValue()));
                    // REFRESHES_COMPLETE
                    trow.addToColumnValue(new TCell().setLongVal(asyncMvMetrics.getRefreshesComplete().getValue()));
                    dataBatch.add(trow);
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult mviewTasksMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> mviewIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "ASYNC_MVIEW_ID");
        List<Expression> mviewSchemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts,
                "ASYNC_MVIEW_SCHEMA");
        List<Expression> mviewNameConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "ASYNC_MVIEW_NAME");
        List<Expression> taskIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TASK_ID");

        Collection<DatabaseIf<? extends TableIf>> allDbs = Env.getCurrentEnv().getInternalCatalog().getAllDbs();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (DatabaseIf<? extends TableIf> db : allDbs) {
            String dbName = db.getFullName();
            if (FrontendConjunctsUtils.isFiltered(mviewSchemaConjuncts, "ASYNC_MVIEW_SCHEMA", dbName)) {
                continue;
            }
            List<? extends TableIf> tables = db.getTables();
            for (TableIf table : tables) {
                if (table instanceof MTMV) {
                    MTMV mtmv = (MTMV) table;
                    // todo: check auth
                    String tableName = table.getName();
                    if (FrontendConjunctsUtils.isFiltered(mviewNameConjuncts, "ASYNC_MVIEW_NAME", tableName)) {
                        continue;
                    }
                    long mtmvId = mtmv.getId();
                    if (FrontendConjunctsUtils.isFiltered(mviewIdConjuncts, "ASYNC_MVIEW_ID", mtmvId)) {
                        continue;
                    }
                    MTMVJob mtmvJob = MTMVJobManager.getJobByMTMV(mtmv);
                    List<MTMVTask> mtmvTasks = mtmvJob.queryAllTasks();
                    for (MTMVTask mtmvTask : mtmvTasks) {
                        long taskId = mtmvTask.getTaskId();
                        if (FrontendConjunctsUtils.isFiltered(taskIdConjuncts, "TASK_ID", taskId)) {
                            continue;
                        }
                        TRow trow = new TRow();
                        // TASK_ID
                        trow.addToColumnValue(new TCell().setLongVal(taskId));
                        // ASYNC_MVIEW_ID
                        trow.addToColumnValue(new TCell().setLongVal(mtmvId));
                        // ASYNC_MVIEW_CATALOG
                        trow.addToColumnValue(new TCell().setStringVal(InternalCatalog.INTERNAL_CATALOG_NAME));
                        // ASYNC_MVIEW_SCHEMA
                        trow.addToColumnValue(new TCell().setStringVal(dbName));
                        // ASYNC_MVIEW_NAME
                        trow.addToColumnValue(new TCell().setStringVal(tableName));
                        // TYPE
                        trow.addToColumnValue(
                                new TCell().setStringVal(mtmvTask.getTaskContext().getTriggerMode().getDisplayName()));
                        // METHOD
                        MTMVTaskRefreshMode refreshMode = mtmvTask.getRefreshMode();
                        trow.addToColumnValue(
                                new TCell().setStringVal(refreshMode == null ? "" : refreshMode.getDisplayName()));
                        // CREATE_TIME
                        // todo long datetime
                        trow.addToColumnValue(new TCell().setLongVal(mtmvTask.getCreateTimeMs()));
                        // START_TIME
                        Long startTimeMs = mtmvTask.getStartTimeMs();
                        trow.addToColumnValue(new TCell().setLongVal(startTimeMs));
                        // FINISHED_TIME
                        long finishTimeMs = mtmvTask.getFinishTimeMs();
                        trow.addToColumnValue(new TCell().setLongVal(finishTimeMs));
                        // DURATION_MS
                        trow.addToColumnValue(
                                new TCell().setLongVal(finishTimeMs == 0 ? 0L : finishTimeMs - startTimeMs));
                        // STATUS
                        trow.addToColumnValue(new TCell().setStringVal(mtmvTask.getStatus().name()));
                        // IS_FORCE_REFRESH
                        trow.addToColumnValue(new TCell().setBoolVal(mtmvTask.getTaskContext().isComplete()));
                        // ERROR_CODE
                        trow.addToColumnValue(new TCell().setIntVal(0));
                        // ERROR_MESSAGE
                        trow.addToColumnValue(new TCell().setStringVal(mtmvTask.getErrMsg()));
                        // MV_REFRESH_PARTITIONS
                        // todo array
                        List<String> needRefreshPartitions = mtmvTask.getNeedRefreshPartitions();
                        List<String> completedPartitions = mtmvTask.getCompletedPartitions();
                        trow.addToColumnValue(new TCell().setStringVal(GsonUtils.GSON.toJson(needRefreshPartitions)));
                        // MV_COMPLETED_PARTITIONS
                        trow.addToColumnValue(new TCell().setStringVal(GsonUtils.GSON.toJson(completedPartitions)));
                        // PROGRESS
                        trow.addToColumnValue(new TCell().setDoubleVal(
                                completedPartitions.size() * 100 / needRefreshPartitions.size()));
                        // LAST_QUERY_ID
                        trow.addToColumnValue(new TCell().setStringVal(mtmvTask.getLastQueryId()));
                        dataBatch.add(trow);
                    }
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult syncMviewStatusMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> mviewIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "SYNC_MVIEW_ID");
        List<Expression> mviewSchemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts,
                "SYNC_MVIEW_SCHEMA");
        List<Expression> mviewTableConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "SYNC_MVIEW_TABLE");
        List<Expression> mviewNameConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "SYNC_MVIEW_NAME");

        Collection<DatabaseIf<? extends TableIf>> allDbs = Env.getCurrentEnv().getInternalCatalog().getAllDbs();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (DatabaseIf<? extends TableIf> db : allDbs) {
            String dbName = db.getFullName();
            if (FrontendConjunctsUtils.isFiltered(mviewSchemaConjuncts, "SYNC_MVIEW_SCHEMA", dbName)) {
                continue;
            }
            List<? extends TableIf> tables = db.getTables();
            for (TableIf table : tables) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    // todo: check auth
                    String tableName = table.getName();
                    if (FrontendConjunctsUtils.isFiltered(mviewTableConjuncts, "SYNC_MVIEW_TABLE", tableName)) {
                        continue;
                    }
                    Map<String, Long> indexNameToId = olapTable.getIndexNameToId();
                    for (Entry<String, Long> entry : indexNameToId.entrySet()) {
                        long mviewId = entry.getValue();
                        String mviewName = entry.getKey();
                        if (mviewId == olapTable.getBaseIndexId()) {
                            continue;
                        }
                        if (FrontendConjunctsUtils.isFiltered(mviewIdConjuncts, "SYNC_MVIEW_ID", mviewId)) {
                            continue;
                        }
                        if (FrontendConjunctsUtils.isFiltered(mviewNameConjuncts, "SYNC_MVIEW_NAME", mviewName)) {
                            continue;
                        }
                        TRow trow = new TRow();
                        // SYNC_MVIEW_ID
                        trow.addToColumnValue(new TCell().setLongVal(mviewId));
                        // SYNC_MVIEW_CATALOG
                        trow.addToColumnValue(new TCell().setStringVal(InternalCatalog.INTERNAL_CATALOG_NAME));
                        // SYNC_MVIEW_SCHEMA
                        trow.addToColumnValue(new TCell().setStringVal(dbName));
                        // SYNC_MVIEW_TABLE
                        trow.addToColumnValue(new TCell().setStringVal(tableName));
                        // SYNC_MVIEW_NAME
                        trow.addToColumnValue(new TCell().setStringVal(mviewName));
                        dataBatch.add(trow);
                    }
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult activitySyncMviewMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> mviewIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "SYNC_MVIEW_ID");
        List<Expression> mviewSchemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts,
                "SYNC_MVIEW_SCHEMA");
        List<Expression> mviewTableConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "SYNC_MVIEW_TABLE");
        List<Expression> mviewNameConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "SYNC_MVIEW_NAME");

        Collection<DatabaseIf<? extends TableIf>> allDbs = Env.getCurrentEnv().getInternalCatalog().getAllDbs();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (DatabaseIf<? extends TableIf> db : allDbs) {
            String dbName = db.getFullName();
            if (FrontendConjunctsUtils.isFiltered(mviewSchemaConjuncts, "SYNC_MVIEW_SCHEMA", dbName)) {
                continue;
            }
            List<? extends TableIf> tables = db.getTables();
            for (TableIf table : tables) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    // todo: check auth
                    String tableName = table.getName();
                    if (FrontendConjunctsUtils.isFiltered(mviewTableConjuncts, "SYNC_MVIEW_TABLE", tableName)) {
                        continue;
                    }
                    Map<String, Long> indexNameToId = olapTable.getIndexNameToId();
                    Map<Long, MaterializedIndexMeta> indexIdToMeta = olapTable.getIndexIdToMeta();
                    for (Entry<String, Long> entry : indexNameToId.entrySet()) {
                        long mviewId = entry.getValue();
                        String mviewName = entry.getKey();
                        if (mviewId == olapTable.getBaseIndexId()) {
                            continue;
                        }
                        MaterializedIndexMeta materializedIndexMeta = indexIdToMeta.get(mviewId);
                        if (materializedIndexMeta == null) {
                            continue;
                        }
                        if (FrontendConjunctsUtils.isFiltered(mviewIdConjuncts, "SYNC_MVIEW_ID", mviewId)) {
                            continue;
                        }
                        if (FrontendConjunctsUtils.isFiltered(mviewNameConjuncts, "SYNC_MVIEW_NAME", mviewName)) {
                            continue;
                        }
                        TRow trow = new TRow();
                        // SYNC_MVIEW_ID
                        trow.addToColumnValue(new TCell().setLongVal(mviewId));
                        // SYNC_MVIEW_CATALOG
                        trow.addToColumnValue(new TCell().setStringVal(InternalCatalog.INTERNAL_CATALOG_NAME));
                        // SYNC_MVIEW_SCHEMA
                        trow.addToColumnValue(new TCell().setStringVal(dbName));
                        // SYNC_MVIEW_TABLE
                        trow.addToColumnValue(new TCell().setStringVal(tableName));
                        // SYNC_MVIEW_NAME
                        trow.addToColumnValue(new TCell().setStringVal(mviewName));
                        SyncMvMetrics syncMvMetrics = materializedIndexMeta.getSyncMvMetrics();
                        // REWRITE_SUCCESS
                        trow.addToColumnValue(new TCell().setLongVal(syncMvMetrics.getRewriteSuccess().getValue()));
                        // REWRITE_SUCCESS_WITH_HINT
                        trow.addToColumnValue(
                                new TCell().setLongVal(syncMvMetrics.getRewriteSuccessWithHint().getValue()));
                        // REWRITE_FAILURE
                        Long cboRejected = syncMvMetrics.getRewriteFailureCboRejected().getValue();
                        Long withHint = syncMvMetrics.getRewriteFailureWithHint().getValue();
                        Long shapeMismatch = syncMvMetrics.getRewriteFailureShapeMismatch().getValue();
                        trow.addToColumnValue(new TCell().setLongVal(cboRejected + withHint + shapeMismatch));
                        // REWRITE_FAILURE_SHAPE_MISMATCH
                        trow.addToColumnValue(new TCell().setLongVal(shapeMismatch));
                        // REWRITE_FAILURE_CBO_REJECTED
                        trow.addToColumnValue(new TCell().setLongVal(cboRejected));
                        // REWRITE_FAILURE_WITH_HINT
                        trow.addToColumnValue(new TCell().setLongVal(withHint));
                        dataBatch.add(trow);
                    }
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult analyzeTableLevelStatusMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> catalogConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_CATALOG");
        List<Expression> schemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_SCHEMA");
        List<Expression> tableConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_NAME");

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        Set<CatalogIf> catalogs = Env.getCurrentEnv().getCatalogMgr().getCopyOfCatalog();
        for (CatalogIf catalog : catalogs) {
            String catalogName = catalog.getName();
            if (FrontendConjunctsUtils.isFiltered(catalogConjuncts, "TABLE_CATALOG", catalogName)) {
                continue;
            }
            Collection<DatabaseIf> dbs = catalog.getAllDbs();
            for (DatabaseIf db : dbs) {
                String dbName = db.getFullName();
                if (FrontendConjunctsUtils.isFiltered(schemaConjuncts, "TABLE_SCHEMA", dbName)) {
                    continue;
                }
                List<TableIf> tables = db.getTables();
                for (TableIf table : tables) {
                    String tableName = table.getName();
                    if (FrontendConjunctsUtils.isFiltered(tableConjuncts, "TABLE_NAME", tableName)) {
                        continue;
                    }
                    TRow trow = new TRow();
                    trow.addToColumnValue(new TCell().setStringVal(catalogName));
                    trow.addToColumnValue(new TCell().setStringVal(dbName));
                    trow.addToColumnValue(new TCell().setStringVal(tableName));
                    Pair<Boolean, String> canAutoAnalyze = table.canAutoAnalyze();
                    trow.addToColumnValue(new TCell().setBoolVal(canAutoAnalyze.first));
                    trow.addToColumnValue(new TCell().setStringVal(canAutoAnalyze.second));
                    List<String> analyzedColumnNames = Lists.newArrayList();
                    String lowestHealthRateColumnName = "";
                    double lowestHealthRate = StatisticsUtil.MAX_HEALTH_RATE;
                    int totalColumnCount = 0;
                    if (table instanceof OlapTable) {
                        OlapTable olapTable = (OlapTable) table;
                        Map<Long, List<Column>> indexIdToSchema = olapTable.getIndexIdToSchema();
                        for (Entry<Long, List<Column>> entry : indexIdToSchema.entrySet()) {
                            Long indexId = entry.getKey();
                            String indexName = olapTable.getIndexNameById(indexId);
                            if (StringUtils.isEmpty(indexName)) {
                                continue;
                            }
                            List<Column> columns = entry.getValue();
                            for (Column column : columns) {
                                totalColumnCount += 1;
                                boolean isUnsupportedType = StatisticsUtil.isUnsupportedType(column.getType());
                                if (!isUnsupportedType) {
                                    String columnNameWithIndexName = indexName + "." + column.getName();
                                    double columnHealthRate = StatisticsUtil.getColumnHealthRate(table,
                                            Pair.of(indexName, column.getName()));
                                    if (columnHealthRate <= lowestHealthRate) {
                                        lowestHealthRate = columnHealthRate;
                                        lowestHealthRateColumnName = columnNameWithIndexName;
                                    }
                                    analyzedColumnNames.add(columnNameWithIndexName);
                                }
                            }
                        }
                    } else {
                        List<Column> columns = table.getColumns();
                        for (Column column : columns) {
                            totalColumnCount += 1;
                            boolean isUnsupportedType = StatisticsUtil.isUnsupportedType(column.getType());
                            if (!isUnsupportedType) {
                                double columnHealthRate = StatisticsUtil.getColumnHealthRate(table,
                                        Pair.of(tableName, column.getName()));
                                if (columnHealthRate <= lowestHealthRate) {
                                    lowestHealthRate = columnHealthRate;
                                    lowestHealthRateColumnName = column.getName();
                                }
                                analyzedColumnNames.add(column.getName());
                            }
                        }
                    }
                    trow.addToColumnValue(new TCell().setIntVal(totalColumnCount));
                    trow.addToColumnValue(new TCell().setIntVal(totalColumnCount - analyzedColumnNames.size()));
                    trow.addToColumnValue(new TCell().setIntVal(analyzedColumnNames.size()));
                    trow.addToColumnValue(new TCell().setStringVal(GsonUtils.GSON.toJson(analyzedColumnNames)));
                    trow.addToColumnValue(new TCell().setDoubleVal(lowestHealthRate));
                    trow.addToColumnValue(new TCell().setStringVal(lowestHealthRateColumnName));
                    dataBatch.add(trow);
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult analyzeJobMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> catalogConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_CATALOG");
        List<Expression> schemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_SCHEMA");
        List<Expression> tableConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_NAME");
        List<Expression> jobIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "JOB_ID");

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        Collection<AnalysisInfo> jobs = Env.getCurrentEnv().getAnalysisManager()
                .getAnalysisJobInfoMap().values();
        NavigableMap<Long, AnalysisInfo> tasks = Env.getCurrentEnv().getAnalysisManager()
                .getAnalysisTaskInfoMap();
        for (AnalysisInfo job : jobs) {
            long jobId = job.jobId;
            if (FrontendConjunctsUtils.isFiltered(jobIdConjuncts, "JOB_ID", jobId)) {
                continue;
            }
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalog(job.catalogId);
            if (catalog == null) {
                continue;
            }
            String catalogName = catalog.getName();
            if (FrontendConjunctsUtils.isFiltered(catalogConjuncts, "TABLE_CATALOG", catalogName)) {
                continue;
            }
            Optional<? extends DatabaseIf<? extends TableIf>> optionalDb = catalog.getDb(job.dbId);
            if (!optionalDb.isPresent()) {
                continue;
            }
            DatabaseIf<? extends TableIf> db = optionalDb.get();
            String dbName = db.getFullName();
            if (FrontendConjunctsUtils.isFiltered(schemaConjuncts, "TABLE_SCHEMA", dbName)) {
                continue;
            }
            Optional<? extends TableIf> optionalTable = db.getTable(job.tblId);
            if (!optionalTable.isPresent()) {
                continue;
            }
            TableIf table = optionalTable.get();
            String tableName = table.getName();
            if (FrontendConjunctsUtils.isFiltered(tableConjuncts, "TABLE_NAME", tableName)) {
                continue;
            }
            TRow trow = new TRow();
            // JOB_ID
            trow.addToColumnValue(new TCell().setLongVal(jobId));
            // TABLE_CATALOG
            trow.addToColumnValue(new TCell().setStringVal(catalogName));
            // TABLE_SCHEMA
            trow.addToColumnValue(new TCell().setStringVal(dbName));
            // TABLE_NAME
            trow.addToColumnValue(new TCell().setStringVal(tableName));
            // COLUMN_NAMES
            trow.addToColumnValue(new TCell().setStringVal(job.getJobColumns()));
            // TYPE
            trow.addToColumnValue(new TCell().setStringVal(job.jobType.name()));
            // METHOD
            trow.addToColumnValue(new TCell().setStringVal(job.analysisMethod.name()));
            // STATUS
            trow.addToColumnValue(new TCell().setStringVal(job.state.name()));
            // PRIORITY
            trow.addToColumnValue(new TCell().setStringVal(job.priority.name()));
            // ERROR_CODE
            trow.addToColumnValue(new TCell().setIntVal(0));
            // ERROR_MESSAGE
            trow.addToColumnValue(new TCell().setStringVal(job.message));
            int pendingNum = 0;
            int runningNum = 0;
            int finishedNum = 0;
            int failedNum = 0;
            long rootFailedTaskId = 0L;
            List<Long> taskIds = job.taskIds;
            for (Long taskId : taskIds) {
                AnalysisInfo task = tasks.get(taskId);
                if (task == null) {
                    continue;
                }
                switch (task.state) {
                    case RUNNING:
                        runningNum++;
                        break;
                    case PENDING:
                        pendingNum++;
                        break;
                    case FAILED:
                        if (rootFailedTaskId == 0) {
                            rootFailedTaskId = taskId;
                        }
                        failedNum++;
                        break;
                    case FINISHED:
                        finishedNum++;
                        break;
                    default:
                        break;
                }
            }
            // TOTAL_TASK_COUNT
            trow.addToColumnValue(new TCell().setIntVal(pendingNum + runningNum + finishedNum + failedNum));
            // FINISHED_TASK_COUNT
            trow.addToColumnValue(new TCell().setIntVal(finishedNum));
            // RUNNING_TASK_COUNT
            trow.addToColumnValue(new TCell().setIntVal(runningNum + pendingNum));
            // FAILED_TASK_COUNT
            trow.addToColumnValue(new TCell().setIntVal(failedNum));
            // ROOT_FAILED_TASK_ID
            trow.addToColumnValue(new TCell().setLongVal(rootFailedTaskId));
            // START_TIME
            trow.addToColumnValue(new TCell().setLongVal(job.startTime));
            // END_TIME
            trow.addToColumnValue(new TCell().setLongVal(job.endTime));
            // DURATION_MS
            trow.addToColumnValue(new TCell().setLongVal(job.endTime - job.startTime));
            dataBatch.add(trow);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult analyzeTaskMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> catalogConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_CATALOG");
        List<Expression> schemaConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_SCHEMA");
        List<Expression> tableConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TABLE_NAME");
        List<Expression> jobIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "JOB_ID");
        List<Expression> taskIdConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "TASK_ID");

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        Collection<AnalysisInfo> jobs = Env.getCurrentEnv().getAnalysisManager()
                .getAnalysisJobInfoMap().values();
        NavigableMap<Long, AnalysisInfo> tasks = Env.getCurrentEnv().getAnalysisManager()
                .getAnalysisTaskInfoMap();
        for (AnalysisInfo job : jobs) {
            long jobId = job.jobId;
            if (FrontendConjunctsUtils.isFiltered(jobIdConjuncts, "JOB_ID", jobId)) {
                continue;
            }
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalog(job.catalogId);
            if (catalog == null) {
                continue;
            }
            String catalogName = catalog.getName();
            if (FrontendConjunctsUtils.isFiltered(catalogConjuncts, "TABLE_CATALOG", catalogName)) {
                continue;
            }
            Optional<? extends DatabaseIf<? extends TableIf>> optionalDb = catalog.getDb(job.dbId);
            if (!optionalDb.isPresent()) {
                continue;
            }
            DatabaseIf<? extends TableIf> db = optionalDb.get();
            String dbName = db.getFullName();
            if (FrontendConjunctsUtils.isFiltered(schemaConjuncts, "TABLE_SCHEMA", dbName)) {
                continue;
            }
            Optional<? extends TableIf> optionalTable = db.getTable(job.tblId);
            if (!optionalTable.isPresent()) {
                continue;
            }
            TableIf table = optionalTable.get();
            String tableName = table.getName();
            if (FrontendConjunctsUtils.isFiltered(tableConjuncts, "TABLE_NAME", tableName)) {
                continue;
            }
            List<Long> taskIds = job.taskIds;
            for (Long taskId : taskIds) {
                AnalysisInfo task = tasks.get(taskId);
                if (task == null) {
                    continue;
                }
                if (FrontendConjunctsUtils.isFiltered(taskIdConjuncts, "TASK_ID", taskId)) {
                    continue;
                }
                TRow trow = new TRow();
                // TASK_ID
                trow.addToColumnValue(new TCell().setLongVal(taskId));
                // JOB_ID
                trow.addToColumnValue(new TCell().setLongVal(jobId));
                // TABLE_CATALOG
                trow.addToColumnValue(new TCell().setStringVal(catalogName));
                // TABLE_SCHEMA
                trow.addToColumnValue(new TCell().setStringVal(dbName));
                // TABLE_NAME
                trow.addToColumnValue(new TCell().setStringVal(tableName));
                // COLUMN_NAME
                trow.addToColumnValue(new TCell().setStringVal(task.colName));
                // STATUS
                trow.addToColumnValue(new TCell().setStringVal(task.state.name()));
                // PRIORITY
                trow.addToColumnValue(new TCell().setStringVal(task.priority.name()));
                // ERROR_CODE
                trow.addToColumnValue(new TCell().setIntVal(0));
                // ERROR_MESSAGE
                trow.addToColumnValue(new TCell().setStringVal(task.message));
                // ERROR_SQL
                trow.addToColumnValue(new TCell().setStringVal(task.sql));
                // START_TIME
                trow.addToColumnValue(new TCell().setLongVal(task.startTime));
                // END_TIME
                trow.addToColumnValue(new TCell().setLongVal(task.endTime));
                // DURATION_MS
                trow.addToColumnValue(new TCell().setLongVal(task.endTime - task.startTime));
                dataBatch.add(trow);
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult activityAutoAnalyzeMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        TRow trow = new TRow();
        // AUTO_ANALYZE_ENABLE
        trow.addToColumnValue(new TCell().setBoolVal(StatisticsUtil.enableAutoAnalyze()));
        // AUTO_ANALYZE_THREAD
        trow.addToColumnValue(new TCell().setIntVal(Config.auto_analyze_simultaneously_running_task_num));
        Pair<LocalTime, LocalTime> startEndTime = StatisticsUtil.findConfigFromGlobalSessionVar();
        // AUTO_ANALYZE_START_TIME
        trow.addToColumnValue(new TCell().setStringVal(startEndTime == null ? null : startEndTime.first.toString()));
        // AUTO_ANALYZE_END_TIME
        trow.addToColumnValue(new TCell().setStringVal(startEndTime == null ? null : startEndTime.second.toString()));
        // AUTO_ANALYZE_TABLE_WIDTH_THRESHOLD
        trow.addToColumnValue(new TCell().setIntVal(StatisticsUtil.getAutoAnalyzeTableWidthThreshold()));
        // AUTO_ANALYZE_TABLE_HEALTH_THRESHOLD
        trow.addToColumnValue(new TCell().setIntVal((int) StatisticsUtil.getTableStatsHealthThreshold()));
        Collection<AnalysisInfo> jobs = Env.getCurrentEnv().getAnalysisManager().getAnalysisJobInfoMap().values();
        long successJob = 0;
        long failedJob = 0;
        for (AnalysisInfo job : jobs) {
            if (job.state == AnalysisState.FINISHED) {
                successJob++;
            }
            if (job.state == AnalysisState.FAILED) {
                failedJob++;
            }
        }
        // SUCCESS_JOB
        trow.addToColumnValue(new TCell().setLongVal(successJob));
        // FAILED_JOB
        trow.addToColumnValue(new TCell().setLongVal(failedJob));
        Collection<AnalysisInfo> tasks = Env.getCurrentEnv().getAnalysisManager().getAnalysisTaskInfoMap().values();
        long successTask = 0;
        long failedTask = 0;
        for (AnalysisInfo task : tasks) {
            if (task.state == AnalysisState.FINISHED) {
                successTask++;
            }
            if (task.state == AnalysisState.FAILED) {
                failedTask++;
            }
        }
        // SUCCESS_TASK
        trow.addToColumnValue(new TCell().setLongVal(successTask));
        // FAILED_TASK
        trow.addToColumnValue(new TCell().setLongVal(failedTask));
        // HIGH_PRIORITY_QUEUE_SIZE
        trow.addToColumnValue(new TCell().setLongVal(Env.getCurrentEnv().getAnalysisManager().highPriorityJobs.size()));
        // MEDIUM_PRIORITY_QUEUE_SIZE
        trow.addToColumnValue(new TCell().setLongVal(Env.getCurrentEnv().getAnalysisManager().midPriorityJobs.size()));
        // LOW_PRIORITY_QUEUE_SIZE
        trow.addToColumnValue(new TCell().setLongVal(Env.getCurrentEnv().getAnalysisManager().lowPriorityJobs.size()));
        // VERY_LOW_PRIORITY_QUEUE_SIZE
        trow.addToColumnValue(
                new TCell().setLongVal(Env.getCurrentEnv().getAnalysisManager().veryLowPriorityJobs.size()));
        dataBatch.add(trow);
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult grantsToRolesMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> roleNameConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "ROLE_NAME");
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        Collection<Role> roles = Env.getCurrentEnv().getAuth().getRoles();
        for (Role role : roles) {
            String roleName = role.getRoleName();
            if (FrontendConjunctsUtils.isFiltered(roleNameConjuncts, "ROLE_NAME", roleName)) {
                continue;
            }
            List<PrivObject> privObjects = role.getPrivObjects();
            for (PrivObject privObject : privObjects) {
                TRow trow = new TRow();
                // ROLE_NAME
                trow.addToColumnValue(new TCell().setStringVal(roleName));
                // OBJECT_CATALOG
                trow.addToColumnValue(new TCell().setStringVal(privObject.getCatalog()));
                // OBJECT_DATABASE
                trow.addToColumnValue(new TCell().setStringVal(privObject.getDatabase()));
                // OBJECT_NAME
                trow.addToColumnValue(new TCell().setStringVal(privObject.getName()));
                // OBJECT_TYPE
                trow.addToColumnValue(new TCell().setStringVal(privObject.getType().name()));
                // PRIVILEGE_TYPE
                trow.addToColumnValue(new TCell().setStringVal(GsonUtils.GSON.toJson(privObject.getType())));
                dataBatch.add(trow);
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult grantsToUsersMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        List<Expression> conjuncts = Collections.EMPTY_LIST;
        if (params.isSetFrontendConjuncts()) {
            conjuncts = FrontendConjunctsUtils.convertToExpression(params.getFrontendConjuncts());
        }
        List<Expression> userNameConjuncts = FrontendConjunctsUtils.filterBySlotName(conjuncts, "USER_NAME");
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        Map<String, List<User>> nameToUsers = Env.getCurrentEnv().getAuth().getUserManager().getNameToUsers();
        for (List<User> users : nameToUsers.values()) {
            for (User user : users) {
                String userName = user.getUserIdentity().toString();
                if (FrontendConjunctsUtils.isFiltered(userNameConjuncts, "USER_NAME", userName)) {
                    continue;
                }
                List<PrivObject> privObjects = Env.getCurrentEnv().getAuth()
                        .getUserPrivObjects(user.getUserIdentity());
                for (PrivObject privObject : privObjects) {
                    TRow trow = new TRow();
                    // USER_NAME
                    trow.addToColumnValue(new TCell().setStringVal(userName));
                    // OBJECT_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(privObject.getCatalog()));
                    // OBJECT_DATABASE
                    trow.addToColumnValue(new TCell().setStringVal(privObject.getDatabase()));
                    // OBJECT_NAME
                    trow.addToColumnValue(new TCell().setStringVal(privObject.getName()));
                    // OBJECT_TYPE
                    trow.addToColumnValue(new TCell().setStringVal(privObject.getType().name()));
                    // PRIVILEGE_TYPE
                    trow.addToColumnValue(new TCell().setStringVal(GsonUtils.GSON.toJson(privObject.getType())));
                    dataBatch.add(trow);
                }

            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult workloadSchedPolicyMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        List<List<String>> workloadPolicyList = Env.getCurrentEnv().getWorkloadSchedPolicyMgr()
                .getWorkloadSchedPolicyTvfInfo(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (List<String> policyRow : workloadPolicyList) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(policyRow.get(0)))); // id
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(1))); // name
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(2))); // condition
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(3))); // action
            trow.addToColumnValue(new TCell().setIntVal(Integer.valueOf(policyRow.get(4)))); // priority
            trow.addToColumnValue(new TCell().setBoolVal(Boolean.valueOf(policyRow.get(5)))); // enabled
            trow.addToColumnValue(new TCell().setIntVal(Integer.valueOf(policyRow.get(6)))); // version
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(7))); // workload group id
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult workloadGroupPrivsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(params.getCurrentUserIdent());

        List<List<String>> rows = new ArrayList<>();
        Env.getCurrentEnv().getAuth().getUserRoleWorkloadGroupPrivs(rows, currentUserIdentity);
        List<TRow> dataBatch = Lists.newArrayList();
        for (List<String> privRow : rows) {
            TRow trow = new TRow();
            String workloadGroupName = privRow.get(1);
            trow.addToColumnValue(new TCell().setStringVal(privRow.get(0))); // GRANTEE
            trow.addToColumnValue(new TCell().setStringVal(workloadGroupName)); // WORKLOAD_GROUP_NAME
            trow.addToColumnValue(new TCell().setStringVal(privRow.get(2))); // PRIVILEGE_TYPE
            trow.addToColumnValue(new TCell().setStringVal(privRow.get(3))); // IS_GRANTABLE
            dataBatch.add(trow);
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult queriesMetadataResult(TSchemaTableRequestParams tSchemaTableParams,
            TFetchSchemaTableDataRequest parentRequest) {
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        String selfNode = Env.getCurrentEnv().getSelfNode().getHost();
        if (ConnectContext.get() != null && !Strings.isNullOrEmpty(ConnectContext.get().getCurrentConnectedFEIp())) {
            selfNode = ConnectContext.get().getCurrentConnectedFEIp();
        }
        selfNode = NetUtils.getHostnameByIp(selfNode);

        List<TRow> dataBatch = Lists.newArrayList();
        Map<String, QueryInfo> queryInfoMap = QeProcessorImpl.INSTANCE.getQueryInfoMap();
        String timeZone = VariableMgr.getDefaultSessionVariable().getTimeZone();
        if (tSchemaTableParams.isSetTimeZone()) {
            timeZone = tSchemaTableParams.getTimeZone();
        }
        for (Entry<String, QueryInfo> entry : queryInfoMap.entrySet()) {
            String queryId = entry.getKey();
            QueryInfo queryInfo = entry.getValue();

            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(queryId));

            long queryStartTime = queryInfo.getStartExecTime();
            if (queryStartTime > 0) {
                trow.addToColumnValue(new TCell().setStringVal(
                        TimeUtils.longToTimeStringWithTimeZone(queryStartTime, timeZone)));
                trow.addToColumnValue(
                        new TCell().setLongVal(System.currentTimeMillis() - queryInfo.getStartExecTime()));
            } else {
                trow.addToColumnValue(new TCell());
                trow.addToColumnValue(new TCell().setLongVal(-1));
            }

            List<TPipelineWorkloadGroup> tgroupList = queryInfo.getCoord().getTWorkloadGroups();
            if (tgroupList != null && tgroupList.size() == 1) {
                trow.addToColumnValue(new TCell().setLongVal(tgroupList.get(0).id));
            } else {
                trow.addToColumnValue(new TCell().setLongVal(-1));
            }

            if (queryInfo.getConnectContext() != null) {
                trow.addToColumnValue(new TCell().setStringVal(queryInfo.getConnectContext().getDatabase()));
            } else {
                trow.addToColumnValue(new TCell().setStringVal(""));
            }
            trow.addToColumnValue(new TCell().setStringVal(selfNode));

            long queueStartTime = queryInfo.getQueueStartTime();
            if (queueStartTime > 0) {
                trow.addToColumnValue(new TCell().setStringVal(
                        TimeUtils.longToTimeStringWithTimeZone(queueStartTime, timeZone)));
            } else {
                trow.addToColumnValue(new TCell());
            }

            long queueEndTime = queryInfo.getQueueEndTime();
            if (queueEndTime > 0) {
                trow.addToColumnValue(new TCell().setStringVal(
                        TimeUtils.longToTimeStringWithTimeZone(queueEndTime, timeZone)));
            } else {
                trow.addToColumnValue(new TCell());
            }

            String queueMsg = queryInfo.getQueueStatus();
            trow.addToColumnValue(new TCell().setStringVal(queueMsg));

            trow.addToColumnValue(new TCell().setStringVal(queryInfo.getSql()));
            dataBatch.add(trow);
        }

        /* Get the query results from other FE also */
        if (tSchemaTableParams.isReplayToOtherFe()) {
            TSchemaTableRequestParams replaySchemaTableParams = new TSchemaTableRequestParams(tSchemaTableParams);
            replaySchemaTableParams.setReplayToOtherFe(false);

            TFetchSchemaTableDataRequest replayFetchSchemaTableReq = new TFetchSchemaTableDataRequest(parentRequest);
            replayFetchSchemaTableReq.setSchemaTableParams(replaySchemaTableParams);

            List<TFetchSchemaTableDataResult> relayResults = forwardToOtherFrontends(replayFetchSchemaTableReq);
            relayResults
                    .forEach(rs -> rs.getDataBatch()
                            .forEach(row -> dataBatch.add(row)));
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static List<TFetchSchemaTableDataResult> forwardToOtherFrontends(TFetchSchemaTableDataRequest request) {
        List<TFetchSchemaTableDataResult> results = new ArrayList<>();
        List<Pair<String, Integer>> frontends = FrontendsProcNode.getFrontendWithRpcPort(Env.getCurrentEnv(), false);

        Client client = null;
        int waitTimeOut = ConnectContext.get() == null ? 300 : ConnectContext.get().getExecTimeoutS();
        for (Pair<String, Integer> fe : frontends) {
            TNetworkAddress thriftAddress = new TNetworkAddress(fe.key(), fe.value());
            try {
                client = ClientPool.frontendPool.borrowObject(thriftAddress, waitTimeOut * 1000);
            } catch (Exception e) {
                LOG.warn("Failed to get frontend {} client. exception: {}", fe.key(), e);
                continue;
            }

            boolean isReturnToPool = false;
            try {
                TFetchSchemaTableDataResult result = client.fetchSchemaTableData(request);
                results.add(result);
                isReturnToPool = true;
            } catch (Exception e) {
                LOG.warn("Failed to finish forward fetch operation to fe: {} . exception: {}", fe.key(), e);
            } finally {
                if (isReturnToPool) {
                    ClientPool.frontendPool.returnObject(thriftAddress, client);
                } else {
                    ClientPool.frontendPool.invalidateObject(thriftAddress, client);
                }
            }
        }

        return results;
    }

    private static void filterColumns(TFetchSchemaTableDataResult result,
            List<String> columnNames, TMetadataType type, TMetadataTableRequestParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("filterColumns() start.");
        }
        List<TRow> fullColumnsRow = result.getDataBatch();
        List<TRow> filterColumnsRows = Lists.newArrayList();
        for (TRow row : fullColumnsRow) {
            TRow filterRow = new TRow();
            try {
                for (String columnName : columnNames) {
                    Integer index = MetadataTableValuedFunction.getColumnIndexFromColumnName(type, columnName, params);
                    filterRow.addToColumnValue(row.getColumnValue().get(index));
                }
            } catch (AnalysisException e) {
                throw new TException(e);
            }
            filterColumnsRows.add(filterRow);
        }
        result.setDataBatch(filterColumnsRows);
        if (LOG.isDebugEnabled()) {
            LOG.debug("filterColumns() end.");
        }
    }

    private static void filterColumns(TFetchSchemaTableDataResult result,
            List<String> columnNames,
            ImmutableMap<String, Integer> columnIndex) throws TException {
        List<TRow> fullColumnsRow = result.getDataBatch();
        List<TRow> filterColumnsRows = Lists.newArrayList();
        for (TRow row : fullColumnsRow) {
            TRow filterRow = new TRow();
            try {
                for (String columnName : columnNames) {
                    Integer index = columnIndex.get(columnName.toLowerCase());
                    filterRow.addToColumnValue(row.getColumnValue().get(index));
                }
            } catch (Throwable e) {
                LOG.info("error happens when filter columns.", e);
                throw new TException(e);
            }
            filterColumnsRows.add(filterRow);
        }
        result.setDataBatch(filterColumnsRows);
    }

    private static TFetchSchemaTableDataResult mtmvMetadataResult(TMetadataTableRequestParams params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmvMetadataResult() start");
        }
        if (!params.isSetMaterializedViewsMetadataParams()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("MaterializedViews metadata params is not set.");
            }
            return errorResult("MaterializedViews metadata params is not set.");
        }

        TMaterializedViewsMetadataParams mtmvMetadataParams = params.getMaterializedViewsMetadataParams();
        String dbName = mtmvMetadataParams.getDatabase();
        if (LOG.isDebugEnabled()) {
            LOG.debug("dbName: " + dbName);
        }
        TUserIdentity currentUserIdent = mtmvMetadataParams.getCurrentUserIdent();
        UserIdentity userIdentity = UserIdentity.fromThrift(currentUserIdent);
        if (LOG.isDebugEnabled()) {
            LOG.debug("userIdentity: " + userIdentity);
        }
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<Table> tables;
        try {
            tables = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(InternalCatalog.INTERNAL_CATALOG_NAME)
                    .getDbOrAnalysisException(dbName).getTables();
        } catch (AnalysisException e) {
            LOG.warn(e.getMessage());
            return errorResult(e.getMessage());
        }

        for (Table table : tables) {
            if (table instanceof MTMV) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(userIdentity, InternalCatalog.INTERNAL_CATALOG_NAME,
                                table.getQualifiedDbName(), table.getName(),
                                PrivPredicate.SHOW)) {
                    continue;
                }
                MTMV mv = (MTMV) table;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mv: {}", mv.toInfoString());
                }

                boolean isSync = MTMVPartitionUtil.isMTMVSync(mv);

                MTMVStatus mtmvStatus = mv.getStatus();
                TRow trow = new TRow();
                trow.addToColumnValue(new TCell().setLongVal(mv.getId()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getName()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getJobInfo().getJobName()));
                trow.addToColumnValue(new TCell().setStringVal(mtmvStatus.getState().name()));
                trow.addToColumnValue(new TCell().setStringVal(mtmvStatus.getSchemaChangeDetail()));
                trow.addToColumnValue(new TCell().setStringVal(mtmvStatus.getRefreshState().name()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getRefreshInfo().toString()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getQuerySql()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getMvProperties().toString()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getMvPartitionInfo().toNameString()));
                trow.addToColumnValue(new TCell().setBoolVal(isSync));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mv end: {}", mv.getName());
                }
                dataBatch.add(trow);
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmvMetadataResult() end");
        }
        return result;
    }

    private static TFetchSchemaTableDataResult partitionMetadataResult(TMetadataTableRequestParams params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("partitionMetadataResult() start");
        }
        if (!params.isSetPartitionsMetadataParams()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Partitions metadata params is not set.");
            }
            return errorResult("Partitions metadata params is not set.");
        }

        TPartitionsMetadataParams partitionsMetadataParams = params.getPartitionsMetadataParams();
        String catalogName = partitionsMetadataParams.getCatalog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("catalogName: " + catalogName);
        }
        String dbName = partitionsMetadataParams.getDatabase();
        if (LOG.isDebugEnabled()) {
            LOG.debug("dbName: " + dbName);
        }
        String tableName = partitionsMetadataParams.getTable();
        if (LOG.isDebugEnabled()) {
            LOG.debug("tableName: " + tableName);
        }

        CatalogIf catalog;
        TableIf table;
        DatabaseIf db;
        try {
            catalog = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(catalogName);
            db = catalog.getDbOrAnalysisException(dbName);
            table = db.getTableOrAnalysisException(tableName);
        } catch (AnalysisException e) {
            LOG.warn(e.getMessage());
            return errorResult(e.getMessage());
        }

        if (catalog instanceof InternalCatalog) {
            return dealInternalCatalog((Database) db, table);
        } else if (catalog instanceof MaxComputeExternalCatalog) {
            return dealMaxComputeCatalog((MaxComputeExternalCatalog) catalog, (ExternalTable) table);
        } else if (catalog instanceof HMSExternalCatalog) {
            return dealHMSCatalog((HMSExternalCatalog) catalog, (ExternalTable) table);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("partitionMetadataResult() end");
        }
        return errorResult("not support catalog: " + catalogName);
    }

    private static TFetchSchemaTableDataResult dealHMSCatalog(HMSExternalCatalog catalog, ExternalTable table) {
        List<TRow> dataBatch = Lists.newArrayList();
        List<String> partitionNames = catalog.getClient()
                .listPartitionNames(table.getRemoteDbName(), table.getRemoteName());
        for (String partition : partitionNames) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(partition));
            dataBatch.add(trow);
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult dealMaxComputeCatalog(MaxComputeExternalCatalog catalog,
            ExternalTable table) {
        List<TRow> dataBatch = Lists.newArrayList();
        List<String> partitionNames = catalog.listPartitionNames(table.getRemoteDbName(), table.getRemoteName());
        for (String partition : partitionNames) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(partition));
            dataBatch.add(trow);
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult dealInternalCatalog(Database db, TableIf table) {
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        if (!(table instanceof OlapTable)) {
            return errorResult("not olap table");
        }
        PartitionsProcDir dir = new PartitionsProcDir(db, (OlapTable) table, false);
        try {
            List<TRow> dataBatch = dir.getPartitionInfosForTvf();
            result.setDataBatch(dataBatch);
        } catch (AnalysisException e) {
            return errorResult(e.getMessage());
        }
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult jobMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetJobsMetadataParams()) {
            return errorResult("Jobs metadata params is not set.");
        }

        TJobsMetadataParams jobsMetadataParams = params.getJobsMetadataParams();
        String type = jobsMetadataParams.getType();
        JobType jobType = JobType.valueOf(type);
        TUserIdentity currentUserIdent = jobsMetadataParams.getCurrentUserIdent();
        UserIdentity userIdentity = UserIdentity.fromThrift(currentUserIdent);
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        List<AbstractJob> jobList = Env.getCurrentEnv().getJobManager().queryJobs(jobType);

        for (AbstractJob job : jobList) {
            if (job instanceof MTMVJob) {
                MTMVJob mtmvJob = (MTMVJob) job;
                if (!mtmvJob.hasPriv(userIdentity, PrivPredicate.SHOW)) {
                    continue;
                }
            }
            dataBatch.add(job.getTvfInfo());
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult taskMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetTasksMetadataParams()) {
            return errorResult("Tasks metadata params is not set.");
        }

        TTasksMetadataParams tasksMetadataParams = params.getTasksMetadataParams();
        String type = tasksMetadataParams.getType();
        JobType jobType = JobType.valueOf(type);
        TUserIdentity currentUserIdent = tasksMetadataParams.getCurrentUserIdent();
        UserIdentity userIdentity = UserIdentity.fromThrift(currentUserIdent);
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        List<AbstractJob> jobList = Env.getCurrentEnv().getJobManager().queryJobs(jobType);

        for (AbstractJob job : jobList) {
            if (job instanceof MTMVJob) {
                MTMVJob mtmvJob = (MTMVJob) job;
                if (!mtmvJob.hasPriv(userIdentity, PrivPredicate.SHOW)) {
                    continue;
                }
            }
            List<AbstractTask> tasks = job.queryAllTasks();
            for (AbstractTask task : tasks) {
                TRow tvfInfo = task.getTvfInfo(job.getJobName());
                if (tvfInfo != null) {
                    dataBatch.add(tvfInfo);
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult routineInfoMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        PlsqlManager plSqlClient = Env.getCurrentEnv().getPlsqlManager();

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();

        Map<PlsqlProcedureKey, PlsqlStoredProcedure> allProc = plSqlClient.getAllPlsqlStoredProcedures();
        for (Entry<PlsqlProcedureKey, PlsqlStoredProcedure> entry : allProc.entrySet()) {
            PlsqlStoredProcedure proc = entry.getValue();
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(proc.getName())); // SPECIFIC_NAME
            trow.addToColumnValue(new TCell().setStringVal(Long.toString(proc.getCatalogId()))); // ROUTINE_CATALOG
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(proc.getCatalogId());
            if (catalog != null) {
                DatabaseIf db = catalog.getDbNullable(proc.getDbId());
                if (db != null) {
                    trow.addToColumnValue(new TCell().setStringVal(db.getFullName())); // ROUTINE_SCHEMA
                } else {
                    trow.addToColumnValue(new TCell().setStringVal("")); // ROUTINE_SCHEMA
                }
            } else {
                trow.addToColumnValue(new TCell().setStringVal("")); // ROUTINE_SCHEMA
            }
            trow.addToColumnValue(new TCell().setStringVal(proc.getName())); // ROUTINE_NAME
            trow.addToColumnValue(new TCell().setStringVal("PROCEDURE")); // ROUTINE_TYPE
            trow.addToColumnValue(new TCell().setStringVal("")); // DTD_IDENTIFIER
            trow.addToColumnValue(new TCell().setStringVal(proc.getSource())); // ROUTINE_BODY
            trow.addToColumnValue(new TCell().setStringVal("")); // ROUTINE_DEFINITION
            trow.addToColumnValue(new TCell().setStringVal("NULL")); // EXTERNAL_NAME
            trow.addToColumnValue(new TCell().setStringVal("")); // EXTERNAL_LANGUAGE
            trow.addToColumnValue(new TCell().setStringVal("SQL")); // PARAMETER_STYLE
            trow.addToColumnValue(new TCell().setStringVal("")); // IS_DETERMINISTIC
            trow.addToColumnValue(new TCell().setStringVal("")); // SQL_DATA_ACCESS
            trow.addToColumnValue(new TCell().setStringVal("NULL")); // SQL_PATH
            trow.addToColumnValue(new TCell().setStringVal("DEFINER")); // SECURITY_TYPE
            trow.addToColumnValue(new TCell().setStringVal(proc.getCreateTime())); // CREATED
            trow.addToColumnValue(new TCell().setStringVal(proc.getModifyTime())); // LAST_ALTERED
            trow.addToColumnValue(new TCell().setStringVal("")); // SQ_MODE
            trow.addToColumnValue(new TCell().setStringVal("")); // ROUTINE_COMMENT
            trow.addToColumnValue(new TCell().setStringVal(proc.getOwnerName())); // DEFINER
            trow.addToColumnValue(new TCell().setStringVal("")); // CHARACTER_SET_CLIENT
            trow.addToColumnValue(new TCell().setStringVal("")); // COLLATION_CONNECTION
            trow.addToColumnValue(new TCell().setStringVal("")); // DATABASE_COLLATION
            dataBatch.add(trow);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void tableOptionsForInternalCatalog(UserIdentity currentUserIdentity,
            CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            olapTable.readLock();
            try {
                TRow trow = new TRow();
                trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
                trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                trow.addToColumnValue(
                        new TCell().setStringVal(olapTable.getKeysType().toMetadata())); // TABLE_MODEL
                trow.addToColumnValue(
                        new TCell().setStringVal(olapTable.getKeyColAsString())); // key columTypes

                DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
                    StringBuilder distributeKey = new StringBuilder();
                    for (Column c : distributionColumns) {
                        if (distributeKey.length() != 0) {
                            distributeKey.append(",");
                        }
                        distributeKey.append(c.getName());
                    }
                    if (distributeKey.length() == 0) {
                        trow.addToColumnValue(new TCell().setStringVal(""));
                    } else {
                        trow.addToColumnValue(
                                new TCell().setStringVal(distributeKey.toString()));
                    }
                    trow.addToColumnValue(new TCell().setStringVal("HASH")); // DISTRIBUTE_TYPE
                } else {
                    trow.addToColumnValue(new TCell().setStringVal("RANDOM")); // DISTRIBUTE_KEY
                    trow.addToColumnValue(new TCell().setStringVal("RANDOM")); // DISTRIBUTE_TYPE
                }
                trow.addToColumnValue(new TCell().setIntVal(distributionInfo.getBucketNum())); // BUCKETS_NUM
                trow.addToColumnValue(new TCell().setIntVal(olapTable.getPartitionNum())); // PARTITION_NUM
                dataBatch.add(trow);
            } finally {
                olapTable.readUnlock();
            }
        }
    }

    private static void tableOptionsForExternalCatalog(UserIdentity currentUserIdentity,
            CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
            trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
            trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
            trow.addToColumnValue(
                    new TCell().setStringVal("")); // TABLE_MODEL
            trow.addToColumnValue(
                    new TCell().setStringVal("")); // key columTypes
            trow.addToColumnValue(new TCell().setStringVal("")); // DISTRIBUTE_KEY
            trow.addToColumnValue(new TCell().setStringVal("")); // DISTRIBUTE_TYPE
            trow.addToColumnValue(new TCell().setIntVal(0)); // BUCKETS_NUM
            trow.addToColumnValue(new TCell().setIntVal(0)); // PARTITION_NUM
            dataBatch.add(trow);
        }
    }

    private static TFetchSchemaTableDataResult tableOptionsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        if (!params.isSetDbId()) {
            return errorResult("current db id is not set.");
        }

        if (!params.isSetCatalog()) {
            return errorResult("current catalog is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        Long dbId = params.getDbId();
        String clg = params.getCatalog();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(clg);
        if (catalog == null) {
            // catalog is NULL let return empty to BE
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        DatabaseIf database = catalog.getDbNullable(dbId);
        if (database == null) {
            // BE gets the database id list from FE and then invokes this interface
            // per database. there is a chance that in between database can be dropped.
            // so need to handle database not exist case and return ok so that BE continue
            // the
            // loop with next database.
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        List<TableIf> tables = database.getTables();
        if (catalog instanceof InternalCatalog) {
            tableOptionsForInternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        } else if (catalog instanceof ExternalCatalog) {
            tableOptionsForExternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void tablePropertiesForInternalCatalog(UserIdentity currentUserIdentity,
            CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            olapTable.readLock();
            try {
                TableProperty property = olapTable.getTableProperty();
                if (property == null) {
                    // if there is no properties, then write empty properties and check next table.
                    TRow trow = new TRow();
                    trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
                    trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                    trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES_NAME
                    trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES_VALUE
                    dataBatch.add(trow);
                    continue;
                }

                Map<String, String> propertiesMap = property.getProperties();
                propertiesMap.forEach((key, value) -> {
                    TRow trow = new TRow();
                    trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
                    trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                    trow.addToColumnValue(new TCell().setStringVal(key)); // PROPERTIES_NAME
                    trow.addToColumnValue(new TCell().setStringVal(value)); // PROPERTIES_VALUE
                    dataBatch.add(trow);
                });
            } finally {
                olapTable.readUnlock();
            }
        } // for table
    }

    private static void tablePropertiesForExternalCatalog(UserIdentity currentUserIdentity,
            CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            // Currently for external catalog, we put properties as empty, can extend in
            // future
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
            trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
            trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
            trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES_NAME
            trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES_VALUE
            dataBatch.add(trow);
        } // for table
    }

    private static TFetchSchemaTableDataResult tablePropertiesMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        if (!params.isSetDbId()) {
            return errorResult("current db id is not set.");
        }

        if (!params.isSetCatalog()) {
            return errorResult("current catalog is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        Long dbId = params.getDbId();
        String clg = params.getCatalog();
        List<TRow> dataBatch = Lists.newArrayList();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(clg);
        if (catalog == null) {
            // catalog is NULL let return empty to BE
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        DatabaseIf database = catalog.getDbNullable(dbId);
        if (database == null) {
            // BE gets the database id list from FE and then invokes this interface
            // per database. there is a chance that in between database can be dropped.
            // so need to handle database not exist case and return ok so that BE continue
            // the loop with next database.
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        List<TableIf> tables = database.getTables();
        if (catalog instanceof InternalCatalog) {
            tablePropertiesForInternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        } else if (catalog instanceof ExternalCatalog) {
            tablePropertiesForExternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult metaCacheStatsMetadataResult(TSchemaTableRequestParams params) {
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        ExternalMetaCacheMgr mgr = Env.getCurrentEnv().getExtMetaCacheMgr();
        for (CatalogIf catalogIf : Env.getCurrentEnv().getCatalogMgr().getCopyOfCatalog()) {
            if (catalogIf instanceof HMSExternalCatalog) {
                HMSExternalCatalog catalog = (HMSExternalCatalog) catalogIf;
                // 1. hive metastore cache
                HiveMetaStoreCache cache = mgr.getMetaStoreCache(catalog);
                if (cache != null) {
                    fillBatch(dataBatch, cache.getStats(), catalog.getName());
                }
                // 2. hudi cache
                HudiMetadataCacheMgr hudiMetadataCacheMgr = mgr.getHudiMetadataCacheMgr();
                fillBatch(dataBatch, hudiMetadataCacheMgr.getCacheStats(catalog), catalog.getName());
            } else if (catalogIf instanceof IcebergExternalCatalog) {
                // 3. iceberg cache
                IcebergMetadataCache icebergCache = mgr.getIcebergMetadataCache();
                fillBatch(dataBatch, icebergCache.getCacheStats(), catalogIf.getName());
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void partitionsForInternalCatalog(UserIdentity currentUserIdentity,
            CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch, String timeZone) {
        for (TableIf table : tables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }

            OlapTable olapTable = (OlapTable) table;
            olapTable.readLock();
            try {
                Collection<Partition> allPartitions = olapTable.getAllPartitions();
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                Joiner joiner = Joiner.on(", ");
                for (Partition partition : allPartitions) {
                    TRow trow = new TRow();
                    long partitionId = partition.getId();
                    trow.addToColumnValue(new TCell().setLongVal(partitionId)); // PARTITION_ID
                    trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
                    trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                    trow.addToColumnValue(new TCell().setStringVal(partition.getName())); // PARTITION_NAME
                    trow.addToColumnValue(new TCell().setStringVal("NULL")); // SUBPARTITION_NAME (always null)

                    trow.addToColumnValue(new TCell().setIntVal(0)); // PARTITION_ORDINAL_POSITION (not available)
                    trow.addToColumnValue(new TCell().setIntVal(0)); // SUBPARTITION_ORDINAL_POSITION (not available)
                    trow.addToColumnValue(new TCell().setStringVal(
                            partitionInfo.getType().toString())); // PARTITION_METHOD
                    trow.addToColumnValue(new TCell().setStringVal("NULL")); // SUBPARTITION_METHOD(always null)
                    PartitionItem item = partitionInfo.getItem(partitionId);
                    if ((partitionInfo.getType() == PartitionType.UNPARTITIONED) || (item == null)) {
                        trow.addToColumnValue(new TCell().setStringVal("NULL")); // if unpartitioned, its null
                        trow.addToColumnValue(
                                new TCell().setStringVal("NULL")); // SUBPARTITION_EXPRESSION (always null)
                        trow.addToColumnValue(new TCell().setStringVal("NULL")); // PARITION DESC, its null
                    } else {
                        trow.addToColumnValue(new TCell().setStringVal(
                                partitionInfo
                                        .getDisplayPartitionColumns().toString())); // PARTITION_EXPRESSION
                        trow.addToColumnValue(
                                new TCell().setStringVal("NULL")); // SUBPARTITION_EXPRESSION (always null)
                        trow.addToColumnValue(new TCell().setStringVal(
                                item.getItemsSql())); // PARITION DESC
                    }
                    trow.addToColumnValue(new TCell().setLongVal(partition.getRowCount())); // TABLE_ROWS (PARTITION)
                    trow.addToColumnValue(new TCell().setLongVal(partition.getAvgRowLength())); // AVG_ROW_LENGTH
                    trow.addToColumnValue(new TCell().setLongVal(partition.getDataLength())); // DATA_LENGTH
                    trow.addToColumnValue(new TCell().setIntVal(0)); // MAX_DATA_LENGTH (not available)
                    trow.addToColumnValue(new TCell().setIntVal(0)); // INDEX_LENGTH (not available)
                    trow.addToColumnValue(new TCell().setIntVal(0)); // DATA_FREE (not available)
                    trow.addToColumnValue(new TCell().setStringVal("NULL")); // CREATE_TIME (not available)
                    // UPDATE_TIME
                    trow.addToColumnValue(new TCell().setStringVal(
                            TimeUtils.longToTimeStringWithTimeZone(partition.getVisibleVersionTime(), timeZone)));
                    trow.addToColumnValue(new TCell().setStringVal("NULL")); // CHECK_TIME (not available)
                    trow.addToColumnValue(new TCell().setIntVal(0)); // CHECKSUM (not available)
                    trow.addToColumnValue(new TCell().setStringVal("")); // PARTITION_COMMENT (not available)
                    trow.addToColumnValue(new TCell().setStringVal("")); // NODEGROUP (not available)
                    trow.addToColumnValue(new TCell().setStringVal("")); // TABLESPACE_NAME (not available)

                    Pair<Double, String> sizePair = DebugUtil.getByteUint(partition.getDataSize(false));
                    String readableDateSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(sizePair.first) + " "
                            + sizePair.second;
                    trow.addToColumnValue(new TCell().setStringVal(readableDateSize));  // LOCAL_DATA_SIZE
                    sizePair = DebugUtil.getByteUint(partition.getRemoteDataSize());
                    readableDateSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(sizePair.first) + " "
                            + sizePair.second;
                    trow.addToColumnValue(new TCell().setStringVal(readableDateSize)); // REMOTE_DATA_SIZE
                    trow.addToColumnValue(new TCell().setStringVal(partition.getState().toString())); // STATE
                    trow.addToColumnValue(new TCell().setStringVal(partitionInfo.getReplicaAllocation(partitionId)
                            .toCreateStmt())); // REPLICA_ALLOCATION
                    trow.addToColumnValue(new TCell().setIntVal(partitionInfo.getReplicaAllocation(partitionId)
                            .getTotalReplicaNum())); // REPLICA_NUM
                    trow.addToColumnValue(new TCell().setStringVal(partitionInfo
                            .getStoragePolicy(partitionId))); // STORAGE_POLICY
                    DataProperty dataProperty = partitionInfo.getDataProperty(partitionId);
                    trow.addToColumnValue(new TCell().setStringVal(dataProperty.getStorageMedium()
                            .name())); // STORAGE_MEDIUM
                    trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(dataProperty
                            .getCooldownTimeMs()))); // COOLDOWN_TIME_MS
                    trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(partition
                            .getLastCheckTime()))); // LAST_CONSISTENCY_CHECK_TIME
                    trow.addToColumnValue(new TCell().setIntVal(partition.getDistributionInfo()
                            .getBucketNum())); // BUCKET_NUM
                    trow.addToColumnValue(new TCell().setLongVal(partition.getCommittedVersion())); // COMMITTED_VERSION
                    trow.addToColumnValue(new TCell().setLongVal(partition.getVisibleVersion())); // VISIBLE_VERSION
                    if (partitionInfo.getType() == PartitionType.RANGE
                            || partitionInfo.getType() == PartitionType.LIST) {
                        List<Column> partitionColumns = partitionInfo.getPartitionColumns();
                        List<String> colNames = new ArrayList<>();
                        for (Column column : partitionColumns) {
                            colNames.add(column.getName());
                        }
                        String colNamesStr = joiner.join(colNames);
                        trow.addToColumnValue(new TCell().setStringVal(colNamesStr));  // PARTITION_KEY
                        trow.addToColumnValue(new TCell().setStringVal(partitionInfo
                                .getPartitionRangeString(partitionId))); // RANGE
                    } else {
                        trow.addToColumnValue(new TCell().setStringVal(""));  // PARTITION_KEY
                        trow.addToColumnValue(new TCell().setStringVal("")); // RANGE
                    }
                    DistributionInfo distributionInfo = partition.getDistributionInfo();
                    if (distributionInfo.getType() == DistributionInfoType.HASH) {
                        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                        List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < distributionColumns.size(); i++) {
                            if (i != 0) {
                                sb.append(", ");
                            }
                            sb.append(distributionColumns.get(i).getName());
                        }
                        trow.addToColumnValue(new TCell().setStringVal(sb.toString())); // DISTRIBUTION
                    } else {
                        trow.addToColumnValue(new TCell().setStringVal("RANDOM")); // DISTRIBUTION
                    }
                    dataBatch.add(trow);
                }
            } finally {
                olapTable.readUnlock();
            }
        } // for table
    }

    private static void partitionsForExternalCatalog(UserIdentity currentUserIdentity,
            CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch, String timeZone) {
        for (TableIf table : tables) {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            // TODO
        } // for table
    }

    private static TFetchSchemaTableDataResult partitionsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        if (!params.isSetDbId()) {
            return errorResult("current db id is not set.");
        }

        if (!params.isSetCatalog()) {
            return errorResult("current catalog is not set.");
        }

        String timezone = VariableMgr.getDefaultSessionVariable().getTimeZone();
        if (params.isSetTimeZone()) {
            timezone = params.getTimeZone();
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        Long dbId = params.getDbId();
        String clg = params.getCatalog();
        List<TRow> dataBatch = Lists.newArrayList();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(clg);
        if (catalog == null) {
            // catalog is NULL let return empty to BE
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        DatabaseIf database = catalog.getDbNullable(dbId);
        if (database == null) {
            // BE gets the database id list from FE and then invokes this interface
            // per database. there is a chance that in between database can be dropped.
            // so need to handle database not exist case and return ok so that BE continue
            // the
            // loop with next database.
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        List<TableIf> tables = database.getTables();
        if (catalog instanceof InternalCatalog) {
            // only olap tables
            partitionsForInternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch, timezone);
        } else if (catalog instanceof ExternalCatalog) {
            partitionsForExternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch, timezone);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void fillBatch(List<TRow> dataBatch, Map<String, Map<String, String>> stats,
            String catalogName) {
        for (Entry<String, Map<String, String>> entry : stats.entrySet()) {
            String cacheName = entry.getKey();
            Map<String, String> cacheStats = entry.getValue();
            for (Entry<String, String> cacheStatsEntry : cacheStats.entrySet()) {
                String metricName = cacheStatsEntry.getKey();
                String metricValue = cacheStatsEntry.getValue();
                TRow trow = new TRow();
                trow.addToColumnValue(new TCell().setStringVal(catalogName)); // CATALOG_NAME
                trow.addToColumnValue(new TCell().setStringVal(cacheName)); // CACHE_NAME
                trow.addToColumnValue(new TCell().setStringVal(metricName)); // METRIC_NAME
                trow.addToColumnValue(new TCell().setStringVal(metricValue)); // METRIC_VALUE
                dataBatch.add(trow);
            }
        }
    }

    private static TFetchSchemaTableDataResult partitionValuesMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetPartitionValuesMetadataParams()) {
            return errorResult("partition values metadata params is not set.");
        }

        TPartitionValuesMetadataParams metaParams = params.getPartitionValuesMetadataParams();
        String ctlName = metaParams.getCatalog();
        String dbName = metaParams.getDatabase();
        String tblName = metaParams.getTable();
        List<TRow> dataBatch;
        try {
            TableIf table = PartitionValuesTableValuedFunction.analyzeAndGetTable(ctlName, dbName, tblName, false);
            TableType tableType = table.getType();
            switch (tableType) {
                case HMS_EXTERNAL_TABLE:
                    dataBatch = partitionValuesMetadataResultForHmsTable((HMSExternalTable) table,
                            params.getColumnsName());
                    break;
                default:
                    return errorResult("not support table type " + tableType.name());
            }
            TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        } catch (Throwable t) {
            LOG.warn("error when get partition values metadata. {}.{}.{}", ctlName, dbName, tblName, t);
            return errorResult("error when get partition values metadata: " + Util.getRootCauseMessage(t));
        }
    }

    private static List<TRow> partitionValuesMetadataResultForHmsTable(HMSExternalTable tbl, List<String> colNames)
            throws AnalysisException {
        List<Column> partitionCols = tbl.getPartitionColumns();
        List<Integer> colIdxs = Lists.newArrayList();
        List<Type> types = Lists.newArrayList();
        for (String colName : colNames) {
            for (int i = 0; i < partitionCols.size(); ++i) {
                if (partitionCols.get(i).getName().equalsIgnoreCase(colName)) {
                    colIdxs.add(i);
                    types.add(partitionCols.get(i).getType());
                }
            }
        }
        if (colIdxs.size() != colNames.size()) {
            throw new AnalysisException(
                    "column " + colNames + " does not match partition columns of table " + tbl.getName());
        }

        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) tbl.getCatalog());
        HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                tbl, tbl.getPartitionColumnTypes(MvccUtil.getSnapshotFromContext(tbl)));
        Map<Long, List<String>> valuesMap = hivePartitionValues.getPartitionValuesMap();
        List<TRow> dataBatch = Lists.newArrayList();
        for (Entry<Long, List<String>> entry : valuesMap.entrySet()) {
            TRow trow = new TRow();
            List<String> values = entry.getValue();
            if (values.size() != partitionCols.size()) {
                continue;
            }

            for (int i = 0; i < colIdxs.size(); ++i) {
                int idx = colIdxs.get(i);
                String partitionValue = values.get(idx);
                if (partitionValue == null || partitionValue.equals(TablePartitionValues.HIVE_DEFAULT_PARTITION)) {
                    trow.addToColumnValue(new TCell().setIsNull(true));
                } else {
                    Type type = types.get(i);
                    switch (type.getPrimitiveType()) {
                        case BOOLEAN:
                            trow.addToColumnValue(new TCell().setBoolVal(Boolean.valueOf(partitionValue)));
                            break;
                        case TINYINT:
                        case SMALLINT:
                        case INT:
                            trow.addToColumnValue(new TCell().setIntVal(Integer.valueOf(partitionValue)));
                            break;
                        case BIGINT:
                            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(partitionValue)));
                            break;
                        case FLOAT:
                            trow.addToColumnValue(new TCell().setDoubleVal(Float.valueOf(partitionValue)));
                            break;
                        case DOUBLE:
                            trow.addToColumnValue(new TCell().setDoubleVal(Double.valueOf(partitionValue)));
                            break;
                        case VARCHAR:
                        case CHAR:
                        case STRING:
                            trow.addToColumnValue(new TCell().setStringVal(partitionValue));
                            break;
                        case DATE:
                        case DATEV2:
                            trow.addToColumnValue(
                                    new TCell().setLongVal(TimeUtils.convertStringToDateV2(partitionValue)));
                            break;
                        case DATETIME:
                        case DATETIMEV2:
                            trow.addToColumnValue(
                                    new TCell().setLongVal(TimeUtils.convertStringToDateTimeV2(partitionValue,
                                            ((ScalarType) type).getScalarScale())));
                            break;
                        default:
                            throw new AnalysisException(
                                    "Unsupported partition column type for $partitions sys table " + type);
                    }
                }
            }
            dataBatch.add(trow);
        }
        return dataBatch;
    }

}
