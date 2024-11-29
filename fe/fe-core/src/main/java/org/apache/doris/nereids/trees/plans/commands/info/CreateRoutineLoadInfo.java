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

import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.ImportDeleteOnStmt;
import org.apache.doris.analysis.ImportSequenceStmt;
import org.apache.doris.analysis.ImportWhereStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
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
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.RoutineLoadDataSourcePropertyFactory;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.commands.load.LoadColumnClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadColumnDesc;
import org.apache.doris.nereids.trees.plans.commands.load.LoadDeleteOnClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadPartitionNames;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSeparator;
import org.apache.doris.nereids.trees.plans.commands.load.LoadSequenceClause;
import org.apache.doris.nereids.trees.plans.commands.load.LoadWhereClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

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

    public static final String FORMAT = "format"; // the value is csv or json, default is csv
    public static final String STRIP_OUTER_ARRAY = "strip_outer_array";
    public static final String JSONPATHS = "jsonpaths";
    public static final String JSONROOT = "json_root";
    public static final String NUM_AS_STRING = "num_as_string";
    public static final String FUZZY_PARSE = "fuzzy_parse";
    public static final String PARTIAL_COLUMNS = "partial_columns";
    public static final String WORKLOAD_GROUP = "workload_group";
    public static final String ENDPOINT_REGEX = "[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";
    public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";
    public static final String LOAD_TO_SINGLE_TABLET = "load_to_single_tablet";
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
            .add(FORMAT)
            .add(JSONPATHS)
            .add(STRIP_OUTER_ARRAY)
            .add(NUM_AS_STRING)
            .add(FUZZY_PARSE)
            .add(JSONROOT)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.TIMEZONE)
            .add(EXEC_MEM_LIMIT_PROPERTY)
            .add(SEND_BATCH_PARALLELISM)
            .add(LOAD_TO_SINGLE_TABLET)
            .add(PARTIAL_COLUMNS)
            .add(WORKLOAD_GROUP)
            .add(LoadStmt.KEY_ENCLOSE)
            .add(LoadStmt.KEY_ESCAPE)
            .build();

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
    /**
     * RoutineLoad support json data.
     * Require Params:
     * 1) dataFormat = "json"
     * 2) jsonPaths = "$.XXX.xxx"
     */
    private String format = ""; //default is csv.
    private String jsonPaths = "";
    private String jsonRoot = ""; // MUST be a jsonpath string
    private boolean stripOuterArray = false;
    private boolean numAsString = false;
    private boolean fuzzyParse = false;

    private byte enclose;

    private byte escape;

    private long workloadGroupId = -1;

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
        checkLoadProperties(ctx);
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

    private void checkLoadProperties(ConnectContext ctx) throws UserException {
        Separator columnSeparator = null;
        // TODO(yangzhengguo01): add line delimiter to properties
        Separator lineDelimiter = null;
        ImportColumnsStmt importColumnsStmt = null;
        ImportWhereStmt precedingImportWhereStmt = null;
        ImportWhereStmt importWhereStmt = null;
        ImportSequenceStmt importSequenceStmt = null;
        PartitionNames partitionNames = null;
        ImportDeleteOnStmt importDeleteOnStmt = null;
        CascadesContext cascadesContext = null;
        ExpressionAnalyzer analyzer = null;
        PlanTranslatorContext context = null;
        if (!isMultiTable) {
            List<String> nameParts = Lists.newArrayList();
            nameParts.add(dbName);
            nameParts.add(tableName);
            Plan unboundRelation = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), nameParts);
            cascadesContext = CascadesContext.initContext(ctx.getStatementContext(), unboundRelation,
                PhysicalProperties.ANY);
            Rewriter.getWholeTreeRewriterWithCustomJobs(cascadesContext,
                ImmutableList.of(Rewriter.bottomUp(new BindRelation()))).execute();
            Plan boundRelation = cascadesContext.getRewritePlan();
            // table could have delete sign in LogicalFilter above
            if (cascadesContext.getRewritePlan() instanceof LogicalFilter) {
                boundRelation = (Plan) ((LogicalFilter) cascadesContext.getRewritePlan()).child();
            }
            context = new PlanTranslatorContext(cascadesContext);
            List<Slot> slots = boundRelation.getOutput();
            Scope scope = new Scope(slots);
            analyzer = new ExpressionAnalyzer(null, scope, cascadesContext, false, false);

            Map<SlotReference, SlotRef> translateMap = Maps.newHashMap();

            TupleDescriptor tupleDescriptor = context.generateTupleDesc();
            tupleDescriptor.setTable(((OlapScan) boundRelation).getTable());
            for (int i = 0; i < boundRelation.getOutput().size(); i++) {
                SlotReference slotReference = (SlotReference) boundRelation.getOutput().get(i);
                SlotRef slotRef = new SlotRef(null, slotReference.getName());
                translateMap.put(slotReference, slotRef);
                context.createSlotDesc(tupleDescriptor, slotReference, ((OlapScan) boundRelation).getTable());
            }
        }

        if (loadPropertyMap != null) {
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
                            Expr expr = translateToLegacyExpr(columnDesc.getExpression(), analyzer,
                                    context, cascadesContext);
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
                    Expr expr = translateToLegacyExpr(((LoadWhereClause) loadProperty).getExpression(),
                            analyzer, context, cascadesContext);
                    if (((LoadWhereClause) loadProperty).isPreceding()) {
                        precedingImportWhereStmt = new ImportWhereStmt(expr,
                                ((LoadWhereClause) loadProperty).isPreceding());
                    } else {
                        importWhereStmt = new ImportWhereStmt(expr, ((LoadWhereClause) loadProperty).isPreceding());
                    }
                } else if (loadProperty instanceof LoadPartitionNames) {
                    partitionNames = new PartitionNames(((LoadPartitionNames) loadProperty).isTemp(),
                            ((LoadPartitionNames) loadProperty).getPartitionNames());
                } else if (loadProperty instanceof LoadDeleteOnClause) {
                    Expr expr = translateToLegacyExpr(((LoadDeleteOnClause) loadProperty).getExpression(),
                            analyzer, context, cascadesContext);
                    importDeleteOnStmt = new ImportDeleteOnStmt(expr);
                } else if (loadProperty instanceof LoadSequenceClause) {
                    importSequenceStmt = new ImportSequenceStmt(
                            ((LoadSequenceClause) loadProperty).getSequenceColName());
                }
            }
        }
        routineLoadDesc = new RoutineLoadDesc(columnSeparator, lineDelimiter, importColumnsStmt,
            precedingImportWhereStmt, importWhereStmt,
            partitionNames, importDeleteOnStmt == null ? null : importDeleteOnStmt.getExpr(), mergeType,
            importSequenceStmt == null ? null : importSequenceStmt.getSequenceColName());
    }

    private Expr translateToLegacyExpr(Expression expr, ExpressionAnalyzer analyzer, PlanTranslatorContext context,
                                       CascadesContext cascadesContext) {
        Expression expression;
        try {
            expression = analyzer.analyze(expr, new ExpressionRewriteContext(cascadesContext));
        } catch (org.apache.doris.nereids.exceptions.AnalysisException e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("In where clause '"
                + expr.toSql() + "', "
                + Utils.convertFirstChar(e.getMessage()));
        }
        return ExpressionTranslator.translate(expression, context);
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

        strictMode = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.STRICT_MODE),
            RoutineLoadJob.DEFAULT_STRICT_MODE,
            LoadStmt.STRICT_MODE + " should be a boolean");
        execMemLimit = Util.getLongPropertyOrDefault(jobProperties.get(EXEC_MEM_LIMIT_PROPERTY),
            RoutineLoadJob.DEFAULT_EXEC_MEM_LIMIT, EXEC_MEM_LIMIT_PRED,
            EXEC_MEM_LIMIT_PROPERTY + " must be greater than 0");

        sendBatchParallelism = ((Long) Util.getLongPropertyOrDefault(jobProperties.get(SEND_BATCH_PARALLELISM),
            ConnectContext.get().getSessionVariable().getSendBatchParallelism(), SEND_BATCH_PARALLELISM_PRED,
            SEND_BATCH_PARALLELISM + " must be greater than 0")).intValue();
        loadToSingleTablet = Util.getBooleanPropertyOrDefault(jobProperties.get(LoadStmt.LOAD_TO_SINGLE_TABLET),
            RoutineLoadJob.DEFAULT_LOAD_TO_SINGLE_TABLET,
            LoadStmt.LOAD_TO_SINGLE_TABLET + " should be a boolean");

        String encloseStr = jobProperties.get(LoadStmt.KEY_ENCLOSE);
        if (encloseStr != null) {
            if (encloseStr.length() != 1) {
                throw new AnalysisException("enclose must be single-char");
            } else {
                enclose = encloseStr.getBytes()[0];
            }
        }
        String escapeStr = jobProperties.get(LoadStmt.KEY_ESCAPE);
        if (escapeStr != null) {
            if (escapeStr.length() != 1) {
                throw new AnalysisException("enclose must be single-char");
            } else {
                escape = escapeStr.getBytes()[0];
            }
        }

        String inputWorkloadGroupStr = jobProperties.get(WORKLOAD_GROUP);
        if (!StringUtils.isEmpty(inputWorkloadGroupStr)) {
            this.workloadGroupId = Env.getCurrentEnv().getWorkloadGroupMgr()
                .getWorkloadGroup(ConnectContext.get().getCurrentUserIdentity(), inputWorkloadGroupStr);
        }

        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        }
        timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties.getOrDefault(LoadStmt.TIMEZONE, timezone));

        format = jobProperties.get(FORMAT);
        if (format != null) {
            if (format.equalsIgnoreCase("csv")) {
                format = ""; // if it's not json, then it's mean csv and set empty
            } else if (format.equalsIgnoreCase("json")) {
                format = "json";
                jsonPaths = jobProperties.getOrDefault(JSONPATHS, "");
                jsonRoot = jobProperties.getOrDefault(JSONROOT, "");
                stripOuterArray = Boolean.parseBoolean(jobProperties.getOrDefault(STRIP_OUTER_ARRAY, "false"));
                numAsString = Boolean.parseBoolean(jobProperties.getOrDefault(NUM_AS_STRING, "false"));
                fuzzyParse = Boolean.parseBoolean(jobProperties.getOrDefault(FUZZY_PARSE, "false"));
            } else {
                throw new UserException("Format type is invalid. format=`" + format + "`");
            }
        } else {
            format = "csv"; // default csv
        }
    }

    private void checkDataSourceProperties() throws UserException {
        this.dataSourceProperties.setTimezone(this.timezone);
        this.dataSourceProperties.analyze();
    }

    /**
     * make legacy create routine load statement after validate by nereids
     * @return legacy create routine load statement
     */
    public CreateRoutineLoadStmt translateToLegacyStmt(ConnectContext ctx) {
        return new CreateRoutineLoadStmt(labelNameInfo.transferToLabelName(), dbName, name, tableName, null,
            ctx.getStatementContext().getOriginStatement(), ctx.getUserIdentity(),
            jobProperties, typeName, routineLoadDesc,
            desiredConcurrentNum, maxErrorNum, maxFilterRatio, maxBatchIntervalS, maxBatchRows, maxBatchSizeBytes,
            execMemLimit, sendBatchParallelism, timezone, format, jsonPaths, jsonRoot, enclose, escape, workloadGroupId,
            loadToSingleTablet, strictMode, isPartialUpdate, stripOuterArray, numAsString, fuzzyParse,
            dataSourceProperties
        );
    }
}
