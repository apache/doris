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

import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.ListPartitionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mtmv.MTMVAnalyzeQueryInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVPropertyUtil;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo.AnalyzerForCreateView;
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo.PlanSlotFinder;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * MTMV info in creating MTMV.
 */
public class CreateMTMVInfo extends CreateTableInfo {
    public static final Logger LOG = LogManager.getLogger(CreateMTMVInfo.class);
    public static final String MTMV_PLANER_DISABLE_RULES = "OLAP_SCAN_PARTITION_PRUNE,PRUNE_EMPTY_PARTITION,"
            + "ELIMINATE_GROUP_BY_KEY_BY_UNIFORM";
    private LogicalPlan logicalQuery;
    private List<SimpleColumnDefinition> simpleColumnDefinitions;
    private MTMVPartitionDefinition mvPartitionDefinition;

    private String querySql;
    private Map<String, String> mvProperties = Maps.newHashMap();
    private final MTMVRefreshInfo refreshInfo;
    private MTMVRelation relation;
    private MTMVPartitionInfo mvPartitionInfo;

    /**
     * constructor for create MTMV
     */
    public CreateMTMVInfo(
            boolean ifNotExists,
            TableNameInfo mvName,
            List<String> keys,
            String comment,
            DistributionDescriptor distribution,
            Map<String, String> properties,
            LogicalPlan logicalQuery,
            String querySql,
            MTMVRefreshInfo refreshInfo,
            List<SimpleColumnDefinition> simpleColumnDefinitions,
            MTMVPartitionDefinition mvPartitionDefinition) {
        super(
                ifNotExists,
                mvName,
                Utils.copyRequiredList(keys),
                comment,
                distribution,
                properties);
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "require logicalQuery object");
        this.querySql = Objects.requireNonNull(querySql, "require querySql object");
        this.refreshInfo = Objects.requireNonNull(refreshInfo, "require refreshInfo object");
        this.simpleColumnDefinitions = Objects
                .requireNonNull(simpleColumnDefinitions, "require simpleColumnDefinitions object");
        this.mvPartitionDefinition = Objects
                .requireNonNull(mvPartitionDefinition, "require mtmvPartitionInfo object");
    }

    /**
     * analyze create table info
     */
    public void analyze(ConnectContext ctx) throws Exception {
        // analyze table name
        tableNameInfo.analyze(ctx);
        if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(tableNameInfo.getCtl())) {
            throw new AnalysisException("Only support creating asynchronous materialized views in internal catalog");
        }
        if (ctx.getSessionVariable().isInDebugMode()) {
            throw new AnalysisException("Create materialized view fail, because is in debug mode");
        }
        try {
            FeNameFormat.checkTableName(tableNameInfo.getTbl());
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(), PrivPredicate.CREATE)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("CREATE",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(),
                    tableNameInfo.getDb() + ": " + tableNameInfo.getTbl());
            throw new AnalysisException(message);
        }
        analyzeProperties();
        analyzeQuery(ctx);
        this.partitionDesc = generatePartitionDesc(ctx);
        if (distribution == null) {
            throw new AnalysisException("Create async materialized view should contain distribution desc");
        }

        if (properties == null) {
            properties = Maps.newHashMap();
        }

        CreateTableInfo.maybeRewriteByAutoBucket(distribution, properties);

        // analyze distribute
        Map<String, ColumnDefinition> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columns.forEach(c -> columnMap.put(c.getName(), c));
        distribution.updateCols(columns.get(0).getName());
        distribution.validate(columnMap, KeysType.DUP_KEYS);
        refreshInfo.validate();

        analyzeProperties();
        rewriteQuerySql(ctx);

        // set CreateTableInfo information
        setTableInformation(ctx);
    }

    private void rewriteQuerySql(ConnectContext ctx) {
        analyzeAndFillRewriteSqlMap(querySql, ctx);
        querySql = BaseViewInfo.rewriteSql(ctx.getStatementContext().getIndexInSqlToString(), querySql);
    }

    private void analyzeAndFillRewriteSqlMap(String sql, ConnectContext ctx) {
        StatementContext stmtCtx = ctx.getStatementContext();
        LogicalPlan parsedViewPlan = new NereidsParser().parseForCreateView(sql);
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContextForStar = CascadesContext.initContext(
                stmtCtx, parsedViewPlan, PhysicalProperties.ANY);
        AnalyzerForCreateView analyzerForStar = new AnalyzerForCreateView(viewContextForStar);
        analyzerForStar.analyze();
        Plan analyzedPlan = viewContextForStar.getRewritePlan();
        // Traverse all slots in the plan, and add the slot's location information
        // and the fully qualified replacement string to the indexInSqlToString of the StatementContext.
        analyzedPlan.accept(PlanSlotFinder.INSTANCE, ctx.getStatementContext());
    }

    private void analyzeProperties() {
        properties = PropertyAnalyzer.getInstance().rewriteOlapProperties(
            tableNameInfo.getCtl(),
            tableNameInfo.getDb(),
            properties);
        if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
            throw new AnalysisException("Not support dynamic partition properties on async materialized view");
        }
        for (String key : MTMVPropertyUtil.MV_PROPERTY_KEYS) {
            if (properties.containsKey(key)) {
                MTMVPropertyUtil.analyzeProperty(key, properties.get(key));
                mvProperties.put(key, properties.get(key));
                properties.remove(key);
            }
        }
    }

    /**
     * analyzeQuery
     */
    public void analyzeQuery(ConnectContext ctx) throws UserException {
        MTMVAnalyzeQueryInfo mtmvAnalyzeQueryInfo = MTMVPlanUtil.analyzeQuery(ctx, this.mvProperties, this.querySql,
                this.mvPartitionDefinition, this.distribution, this.simpleColumnDefinitions, this.properties, this.keys,
                this.logicalQuery);
        this.mvPartitionInfo = mtmvAnalyzeQueryInfo.getMvPartitionInfo();
        this.columns = mtmvAnalyzeQueryInfo.getColumnDefinitions();
        this.relation = mtmvAnalyzeQueryInfo.getRelation();
    }

    private List<Column> getPartitionColumn(String partitionColumnName) {
        for (ColumnDefinition columnDefinition : columns) {
            if (columnDefinition.getName().equalsIgnoreCase(partitionColumnName)) {
                // current only support one partition col
                return Lists.newArrayList(columnDefinition.translateToCatalogStyle());
            }
        }
        throw new AnalysisException("can not find partition column");
    }

    private PartitionDesc generatePartitionDesc(ConnectContext ctx) {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return null;
        }
        // all pct table partition type is same
        MTMVRelatedTableIf relatedTable = MTMVUtil.getRelatedTable(mvPartitionInfo.getPctInfos().get(0).getTableInfo());
        List<AllPartitionDesc> allPartitionDescs = null;
        try {
            allPartitionDescs = MTMVPartitionUtil
                    .getPartitionDescsByRelatedTable(properties, mvPartitionInfo, mvProperties,
                            getPartitionColumn(mvPartitionInfo.getPartitionCol()));
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (allPartitionDescs.size() > ctx.getSessionVariable().getCreateTablePartitionMaxNum()) {
            throw new AnalysisException(String.format(
                    "The number of partitions to be created is [%s], exceeding the maximum value of [%s]. "
                            + "Creating too many partitions can be time-consuming. If necessary, "
                            + "You can set the session variable 'create_table_partition_max_num' to a larger value.",
                    allPartitionDescs.size(), ctx.getSessionVariable().getCreateTablePartitionMaxNum()));
        }
        try {
            PartitionType type = relatedTable.getPartitionType(Optional.empty());
            if (type == PartitionType.RANGE) {
                return new RangePartitionDesc(Lists.newArrayList(mvPartitionInfo.getPartitionCol()),
                        allPartitionDescs);
            } else if (type == PartitionType.LIST) {
                return new ListPartitionDesc(Lists.newArrayList(mvPartitionInfo.getPartitionCol()),
                        allPartitionDescs);
            } else {
                return null;
            }
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    /**
     * set CreateTableInfo Information
     */
    private void setTableInformation(ConnectContext ctx) {
        List<String> ctasColumns = simpleColumnDefinitions.stream()
                .map(SimpleColumnDefinition::getName)
                .collect(Collectors.toList());

        this.setCatalog(tableNameInfo.getCtl());
        this.setDbName(tableNameInfo.getDb());
        this.setTableName(tableNameInfo.getTbl());
        this.setCtasColumns(ctasColumns.isEmpty() ? null : ctasColumns);
        this.setEngineName(CreateTableInfo.ENGINE_OLAP);
        this.setKeysType(KeysType.DUP_KEYS);
        this.setPartitionTableInfo(partitionDesc == null
                ? PartitionTableInfo.EMPTY : partitionDesc.convertToPartitionTableInfo());
        this.setRollups(Lists.newArrayList());
        this.setClusterKeysColumnNames(Lists.newArrayList());
        this.setIndexes(Lists.newArrayList());

        this.analyzeEngine();

        validatePartitionInfo(ctx);
    }

    private void validatePartitionInfo(ConnectContext ctx) {
        Map<String, ColumnDefinition> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columns.forEach(c -> {
            if (columnMap.put(c.getName(), c) != null) {
                try {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME,
                            c.getName());
                } catch (Exception e) {
                    throw new AnalysisException(e.getMessage(), e.getCause());
                }
            }
        });

        getPartitionTableInfo().validatePartitionInfo(
                getEngineName(),
                columns,
                columnMap,
                properties,
                ctx,
                isEnableMergeOnWrite(),
                isExternal());
    }

    public String getQuerySql() {
        return querySql;
    }

    public Map<String, String> getMvProperties() {
        return mvProperties;
    }

    public MTMVRefreshInfo getRefreshInfo() {
        return refreshInfo;
    }

    public MTMVRelation getRelation() {
        return relation;
    }

    public MTMVPartitionInfo getMvPartitionInfo() {
        return mvPartitionInfo;
    }
}
