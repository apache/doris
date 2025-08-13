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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.ImportDeleteOnStmt;
import org.apache.doris.analysis.ImportSequenceStmt;
import org.apache.doris.analysis.ImportWhereStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.RoutineLoadDataSourcePropertyFactory;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.nereids.trees.plans.commands.load.LoadColumnClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadColumnDesc;
import org.apache.doris.nereids.trees.plans.commands.load.LoadDeleteOnClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadPartitionNames;
import org.apache.doris.nereids.trees.plans.commands.load.LoadPrecedingFilterClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSeparator;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSequenceClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadWhereClause;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * info in creating routine load.
 */
public class CreateRoutineLoadInfo {

    // routine load properties
    public static final String DESIRED_CONCURRENT_NUMBER_PROPERTY = "desired_concurrent_number";
    public static final String CURRENT_CONCURRENT_NUMBER_PROPERTY = "current_concurrent_number";
    // max error number in ten thousand records
    public static final String MAX_ERROR_NUMBER_PROPERTY = "max_error_number";
    public static final String MAX_FILTER_RATIO_PROPERTY = "max_filter_ratio";
    // the following 3 properties limit the time and batch size of a single routine load task
    public static final String MAX_BATCH_INTERVAL_SEC_PROPERTY = "max_batch_interval";
    public static final String MAX_BATCH_ROWS_PROPERTY = "max_batch_rows";
    public static final String MAX_BATCH_SIZE_PROPERTY = "max_batch_size";
    public static final String EXEC_MEM_LIMIT_PROPERTY = "exec_mem_limit";

    public static final String PARTIAL_COLUMNS = "partial_columns";
    public static final String WORKLOAD_GROUP = "workload_group";
    public static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";
    public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";
    public static final String LOAD_TO_SINGLE_TABLET = "load_to_single_tablet";

    public static final String STRICT_MODE = "strict_mode";
    public static final String TIMEZONE = "timezone";
    public static final java.util.function.Predicate<Long> DESIRED_CONCURRENT_NUMBER_PRED = (v) -> v > 0L;
    public static final java.util.function.Predicate<Long> MAX_ERROR_NUMBER_PRED = (v) -> v >= 0L;
    public static final java.util.function.Predicate<Double> MAX_FILTER_RATIO_PRED = (v) -> v >= 0 && v <= 1;
    public static final java.util.function.Predicate<Long> MAX_BATCH_INTERVAL_PRED = (v) -> v >= 1;
    public static final java.util.function.Predicate<Long> MAX_BATCH_ROWS_PRED = (v) -> v >= 200000;
    public static final java.util.function.Predicate<Long> MAX_BATCH_SIZE_PRED = (v) -> v >= 100 * 1024 * 1024
            && v <= (long) (1024 * 1024 * 1024) * 10;
    public static final java.util.function.Predicate<Long> EXEC_MEM_LIMIT_PRED = (v) -> v >= 0L;
    public static final Predicate<Long> SEND_BATCH_PARALLELISM_PRED = (v) -> v > 0L;

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(MAX_ERROR_NUMBER_PROPERTY)
            .add(MAX_FILTER_RATIO_PROPERTY)
            .add(MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(MAX_BATCH_ROWS_PROPERTY)
            .add(MAX_BATCH_SIZE_PROPERTY)
            .add(STRICT_MODE)
            .add(TIMEZONE)
            .add(STRICT_MODE)
            .add(TIMEZONE)
            .add(EXEC_MEM_LIMIT_PROPERTY)
            .add(SEND_BATCH_PARALLELISM)
            .add(LOAD_TO_SINGLE_TABLET)
            .add(PARTIAL_COLUMNS)
            .add(WORKLOAD_GROUP)
            .add(FileFormatProperties.PROP_FORMAT)
            .add(JsonFileFormatProperties.PROP_JSON_PATHS)
            .add(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY)
            .add(JsonFileFormatProperties.PROP_NUM_AS_STRING)
            .add(JsonFileFormatProperties.PROP_FUZZY_PARSE)
            .add(JsonFileFormatProperties.PROP_JSON_ROOT)
            .add(CsvFileFormatProperties.PROP_ENCLOSE)
            .add(CsvFileFormatProperties.PROP_ESCAPE)
            .build();

    private static final Logger LOG = LogManager.getLogger(CreateRoutineLoadInfo.class);

    private final LabelNameInfo labelNameInfo;
    private String tableName;
    private final Map<String, LoadProperty> loadPropertyMap;
    private final Map<String, String> jobProperties;
    private final String typeName;

    // the following variables will be initialized after analyze
    // -1 as unset, the default value will set in RoutineLoadJob
    private String name;
    private String dbName;
    private RoutineLoadDesc routineLoadDesc;
    private int desiredConcurrentNum = 1;
    private long maxErrorNum = -1;
    private double maxFilterRatio = -1;
    private long maxBatchIntervalS = -1;
    private long maxBatchRows = -1;
    private long maxBatchSizeBytes = -1;
    private boolean strictMode = true;
    private long execMemLimit = 2 * 1024 * 1024 * 1024L;
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private int sendBatchParallelism = 1;
    private boolean loadToSingleTablet = false;
    private FileFormatProperties fileFormatProperties;

    private String workloadGroupName;

    /**
     * support partial columns load(Only Unique Key Columns)
     */
    private boolean isPartialUpdate = false;

    private String comment = "";

    private LoadTask.MergeType mergeType;

    private boolean isMultiTable = false;

    private AbstractDataSourceProperties dataSourceProperties;

    /**
     * constructor for create table
     */
    public CreateRoutineLoadInfo(LabelNameInfo labelNameInfo, String tableName,
                                 Map<String, LoadProperty> loadPropertyMap,
                                 Map<String, String> jobProperties, String typeName,
                                 Map<String, String> dataSourceProperties, LoadTask.MergeType mergeType,
                                 String comment) {
        this.labelNameInfo = labelNameInfo;
        if (StringUtils.isBlank(tableName)) {
            this.isMultiTable = true;
        }
        this.tableName = tableName;
        this.loadPropertyMap = loadPropertyMap;
        this.jobProperties = jobProperties == null ? Maps.newHashMap() : jobProperties;
        this.typeName = typeName.toUpperCase();
        this.dataSourceProperties = RoutineLoadDataSourcePropertyFactory
            .createDataSource(typeName, dataSourceProperties, this.isMultiTable);
        this.mergeType = mergeType;
        this.isPartialUpdate = this.jobProperties.getOrDefault(PARTIAL_COLUMNS, "false").equalsIgnoreCase("true");
        if (comment != null) {
            this.comment = comment;
        }
    }

    public String getName() {
        return name;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDBName() {
        return dbName;
    }

    public LabelNameInfo getLabelNameInfo() {
        return labelNameInfo;
    }

    public Map<String, LoadProperty> getLoadPropertyMap() {
        return loadPropertyMap;
    }

    public Map<String, String> getJobProperties() {
        return jobProperties;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getDesiredConcurrentNum() {
        return desiredConcurrentNum;
    }

    public long getMaxErrorNum() {
        return maxErrorNum;
    }

    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }

    public long getMaxBatchIntervalS() {
        return maxBatchIntervalS;
    }

    public long getMaxBatchRows() {
        return maxBatchRows;
    }

    public long getMaxBatchSizeBytes() {
        return maxBatchSizeBytes;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public long getExecMemLimit() {
        return execMemLimit;
    }

    public String getTimezone() {
        return timezone;
    }

    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    public boolean isLoadToSingleTablet() {
        return loadToSingleTablet;
    }

    public boolean isPartialUpdate() {
        return isPartialUpdate;
    }

    public String getComment() {
        return comment;
    }

    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    public boolean isMultiTable() {
        return isMultiTable;
    }

    public AbstractDataSourceProperties getDataSourceProperties() {
        return dataSourceProperties;
    }

    public FileFormatProperties getFileFormatProperties() {
        return fileFormatProperties;
    }

    public String getWorkloadGroupName() {
        return workloadGroupName;
    }

    /**
     * analyze create table info
     */
    public void validate(ConnectContext ctx) throws UserException {
        // check dbName and tableName
        checkDBTable(ctx);
        // check name
        try {
            FeNameFormat.checkCommonName(NAME_TYPE, name);
        } catch (org.apache.doris.common.AnalysisException e) {
            // 64 is the length of regular expression matching
            // (FeNameFormat.COMMON_NAME_REGEX/UNDERSCORE_COMMON_NAME_REGEX)
            throw new AnalysisException(e.getMessage()
                + " Maybe routine load job name is longer than 64 or contains illegal characters");
        }
        // check load properties include column separator etc.
        routineLoadDesc = checkLoadProperties(ctx, loadPropertyMap, dbName, tableName, isMultiTable, mergeType);
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();
        // check data source properties
        checkDataSourceProperties();
        // analyze merge type
        if (routineLoadDesc != null) {
            if (mergeType != LoadTask.MergeType.MERGE && routineLoadDesc.getDeleteCondition() != null) {
                throw new AnalysisException("not support DELETE ON clause when merge type is not MERGE.");
            }
            if (mergeType == LoadTask.MergeType.MERGE && routineLoadDesc.getDeleteCondition() == null) {
                throw new AnalysisException("Excepted DELETE ON clause when merge type is MERGE.");
            }
        } else if (mergeType == LoadTask.MergeType.MERGE) {
            throw new AnalysisException("Excepted DELETE ON clause when merge type is MERGE.");
        }
    }

    private void checkDBTable(ConnectContext ctx) throws AnalysisException {
        labelNameInfo.validate(ctx);
        dbName = labelNameInfo.getDb();
        name = labelNameInfo.getLabel();

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        if (isPartialUpdate && isMultiTable) {
            throw new AnalysisException("Partial update is not supported in multi-table load.");
        }
        if (isMultiTable) {
            return;
        }
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("Table name should not be null");
        }
        Table table = db.getTableOrAnalysisException(tableName);
        if (mergeType != LoadTask.MergeType.APPEND
                && (table.getType() != Table.TableType.OLAP
                || ((OlapTable) table).getKeysType() != KeysType.UNIQUE_KEYS)) {
            throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
        }
        if (mergeType != LoadTask.MergeType.APPEND
                && !(table.getType() == Table.TableType.OLAP && ((OlapTable) table).hasDeleteSign())) {
            throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
        }
        if (isPartialUpdate && !((OlapTable) table).getEnableUniqueKeyMergeOnWrite()) {
            throw new AnalysisException("load by PARTIAL_COLUMNS is only supported in unique table MoW");
        }
    }

    /**
     * check load properties
     * @param ctx connect context
     * @param loadPropertyMap load property map
     * @param dbName database name
     * @param tableName table name
     * @param isMultiTable whether is multi table
     * @param mergeType merge type
     * @return routine load desc
     * @throws UserException user exception
     */
    public static RoutineLoadDesc checkLoadProperties(ConnectContext ctx, Map<String, LoadProperty> loadPropertyMap,
                        String dbName, String tableName, boolean isMultiTable, LoadTask.MergeType mergeType)
                        throws UserException {
        Separator columnSeparator = null;
        // TODO(yangzhengguo01): add line delimiter to properties
        Separator lineDelimiter = null;
        ImportColumnsStmt importColumnsStmt = null;
        ImportWhereStmt precedingImportWhereStmt = null;
        ImportWhereStmt importWhereStmt = null;
        ImportSequenceStmt importSequenceStmt = null;
        PartitionNames partitionNames = null;
        ImportDeleteOnStmt importDeleteOnStmt = null;

        if (loadPropertyMap != null) {
            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
            Table table = Strings.isNullOrEmpty(tableName) ? null : db.getTableOrAnalysisException(tableName);
            for (LoadProperty loadProperty : loadPropertyMap.values()) {
                loadProperty.validate();
                if (loadProperty instanceof LoadSeparator) {
                    String oriSeparator = ((LoadSeparator) loadProperty).getOriSeparator();
                    String separator = Separator.convertSeparator(oriSeparator);
                    columnSeparator = new Separator(separator, oriSeparator);
                } else if (loadProperty instanceof LoadColumnClause) {
                    if (isMultiTable) {
                        throw new AnalysisException("Multi-table load does not support setting columns info");
                    }
                    List<ImportColumnDesc> importColumnDescList = new ArrayList<>();
                    for (LoadColumnDesc columnDesc : ((LoadColumnClause) loadProperty).getColumns()) {
                        if (columnDesc.getExpression() != null) {
                            Expr expr = PlanUtils.translateToLegacyExpr(columnDesc.getExpression(), table, ctx);
                            importColumnDescList.add(new ImportColumnDesc(columnDesc.getColumnName(), expr));
                        } else {
                            importColumnDescList.add(new ImportColumnDesc(columnDesc.getColumnName(), null));
                        }
                    }
                    importColumnsStmt = new ImportColumnsStmt(importColumnDescList);
                } else if (loadProperty instanceof LoadWhereClause) {
                    if (isMultiTable) {
                        throw new AnalysisException("Multi-table load does not support setting columns info");
                    }
                    Expr expr = PlanUtils.translateToLegacyExpr(((LoadWhereClause) loadProperty).getExpression(),
                            table, ctx);
                    importWhereStmt = new ImportWhereStmt(expr, false);
                } else if (loadProperty instanceof LoadPrecedingFilterClause) {
                    if (isMultiTable) {
                        throw new AnalysisException("Multi-table load does not support setting columns info");
                    }
                    Expr expr = PlanUtils
                            .translateToLegacyExpr(((LoadPrecedingFilterClause) loadProperty).getExpression(), null,
                                    ctx);
                    precedingImportWhereStmt = new ImportWhereStmt(expr, true);
                } else if (loadProperty instanceof LoadPartitionNames) {
                    partitionNames = new PartitionNames(((LoadPartitionNames) loadProperty).isTemp(),
                            ((LoadPartitionNames) loadProperty).getPartitionNames());
                } else if (loadProperty instanceof LoadDeleteOnClause) {
                    Expr expr = PlanUtils.translateToLegacyExpr(((LoadDeleteOnClause) loadProperty).getExpression(),
                            table, ctx);
                    importDeleteOnStmt = new ImportDeleteOnStmt(expr);
                } else if (loadProperty instanceof LoadSequenceClause) {
                    importSequenceStmt = new ImportSequenceStmt(
                            ((LoadSequenceClause) loadProperty).getSequenceColName());
                }
            }
        }
        return new RoutineLoadDesc(columnSeparator, lineDelimiter, importColumnsStmt,
            precedingImportWhereStmt, importWhereStmt,
            partitionNames, importDeleteOnStmt == null ? null : importDeleteOnStmt.getExpr(), mergeType,
            importSequenceStmt == null ? null : importSequenceStmt.getSequenceColName());
    }

    private void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        desiredConcurrentNum = ((Long) Util.getLongPropertyOrDefault(
            jobProperties.get(DESIRED_CONCURRENT_NUMBER_PROPERTY),
            Config.max_routine_load_task_concurrent_num, DESIRED_CONCURRENT_NUMBER_PRED,
            DESIRED_CONCURRENT_NUMBER_PROPERTY + " must be greater than 0")).intValue();

        maxErrorNum = Util.getLongPropertyOrDefault(jobProperties.get(MAX_ERROR_NUMBER_PROPERTY),
            RoutineLoadJob.DEFAULT_MAX_ERROR_NUM, MAX_ERROR_NUMBER_PRED,
            MAX_ERROR_NUMBER_PROPERTY + " should >= 0");

        maxFilterRatio = Util.getDoublePropertyOrDefault(jobProperties.get(MAX_FILTER_RATIO_PROPERTY),
            RoutineLoadJob.DEFAULT_MAX_FILTER_RATIO, MAX_FILTER_RATIO_PRED,
            MAX_FILTER_RATIO_PROPERTY + " should between 0 and 1");

        maxBatchIntervalS = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_INTERVAL_SEC_PROPERTY),
            RoutineLoadJob.DEFAULT_MAX_INTERVAL_SECOND, MAX_BATCH_INTERVAL_PRED,
            MAX_BATCH_INTERVAL_SEC_PROPERTY + " should >= 1");

        maxBatchRows = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_ROWS_PROPERTY),
            RoutineLoadJob.DEFAULT_MAX_BATCH_ROWS, MAX_BATCH_ROWS_PRED,
            MAX_BATCH_ROWS_PROPERTY + " should > 200000");

        maxBatchSizeBytes = Util.getLongPropertyOrDefault(jobProperties.get(MAX_BATCH_SIZE_PROPERTY),
            RoutineLoadJob.DEFAULT_MAX_BATCH_SIZE, MAX_BATCH_SIZE_PRED,
            MAX_BATCH_SIZE_PROPERTY + " should between 100MB and 10GB");

        strictMode = Util.getBooleanPropertyOrDefault(jobProperties.get(STRICT_MODE),
            RoutineLoadJob.DEFAULT_STRICT_MODE,
            STRICT_MODE + " should be a boolean");
        execMemLimit = Util.getLongPropertyOrDefault(jobProperties.get(EXEC_MEM_LIMIT_PROPERTY),
            RoutineLoadJob.DEFAULT_EXEC_MEM_LIMIT, EXEC_MEM_LIMIT_PRED,
            EXEC_MEM_LIMIT_PROPERTY + " must be greater than 0");

        sendBatchParallelism = ((Long) Util.getLongPropertyOrDefault(jobProperties.get(SEND_BATCH_PARALLELISM),
            ConnectContext.get().getSessionVariable().getSendBatchParallelism(), SEND_BATCH_PARALLELISM_PRED,
            SEND_BATCH_PARALLELISM + " must be greater than 0")).intValue();
        loadToSingleTablet = Util.getBooleanPropertyOrDefault(jobProperties.get(LOAD_TO_SINGLE_TABLET),
            RoutineLoadJob.DEFAULT_LOAD_TO_SINGLE_TABLET,
            LOAD_TO_SINGLE_TABLET + " should be a boolean");

        String inputWorkloadGroupStr = jobProperties.get(WORKLOAD_GROUP);
        if (!StringUtils.isEmpty(inputWorkloadGroupStr)) {
            ConnectContext tmpCtx = new ConnectContext();
            tmpCtx.setCurrentUserIdentity(ConnectContext.get().getCurrentUserIdentity());
            tmpCtx.getSessionVariable().setWorkloadGroup(inputWorkloadGroupStr);
            if (Config.isCloudMode()) {
                tmpCtx.setCloudCluster(ConnectContext.get().getCloudCluster());
            }
            List<WorkloadGroup> wgList = Env.getCurrentEnv().getWorkloadGroupMgr()
                    .getWorkloadGroup(tmpCtx);
            if (wgList.size() == 0) {
                throw new UserException("Can not find workload group " + inputWorkloadGroupStr);
            }
            this.workloadGroupName = inputWorkloadGroupStr;
        }

        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        }
        timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties.getOrDefault(TIMEZONE, timezone));

        String format = jobProperties.getOrDefault(FileFormatProperties.PROP_FORMAT, "csv");
        fileFormatProperties = FileFormatProperties.createFileFormatProperties(format);
        fileFormatProperties.analyzeFileFormatProperties(jobProperties, false);
    }

    private void checkDataSourceProperties() throws UserException {
        this.dataSourceProperties.setTimezone(this.timezone);
        this.dataSourceProperties.analyze();
    }

    /**
     * getRoutineLoadDesc
     *
     * @return routineLoadDesc
     */
    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }
}
