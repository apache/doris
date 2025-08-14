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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.RoutineLoadDataSourcePropertyFactory;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.load.LoadProperty;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.eclipse.jetty.util.StringUtil;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * ALTER ROUTINE LOAD db.label
 * PROPERTIES(
 * ...
 * )
 * FROM kafka (
 * ...
 * )
 */
public class AlterRoutineLoadCommand extends AlterCommand {

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";
    private static final ImmutableSet<String> CONFIGURABLE_JOB_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY)
            .add(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY)
            .add(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY)
            .add(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY)
            .add(CreateRoutineLoadInfo.PARTIAL_COLUMNS)
            .add(CreateRoutineLoadInfo.STRICT_MODE)
            .add(CreateRoutineLoadInfo.TIMEZONE)
            .add(CreateRoutineLoadInfo.WORKLOAD_GROUP)
            .add(JsonFileFormatProperties.PROP_JSON_PATHS)
            .add(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY)
            .add(JsonFileFormatProperties.PROP_NUM_AS_STRING)
            .add(JsonFileFormatProperties.PROP_FUZZY_PARSE)
            .add(JsonFileFormatProperties.PROP_JSON_ROOT)
            .add(CsvFileFormatProperties.PROP_ENCLOSE)
            .add(CsvFileFormatProperties.PROP_ESCAPE)
            .build();

    private final LabelNameInfo labelNameInfo;
    private final Map<String, LoadProperty> loadPropertyMap;
    private RoutineLoadDesc routineLoadDesc;
    private final Map<String, String> jobProperties;
    private final Map<String, String> dataSourceMapProperties;
    private boolean isPartialUpdate;

    // save analyzed job properties.
    // analyzed data source properties are saved in dataSourceProperties.
    private Map<String, String> analyzedJobProperties = Maps.newHashMap();
    private AbstractDataSourceProperties dataSourceProperties;

    /**
     * AlterRoutineLoadCommand
     */
    public AlterRoutineLoadCommand(LabelNameInfo labelNameInfo,
                                   Map<String, LoadProperty> loadPropertyMap,
                                   Map<String, String> jobProperties,
                                   Map<String, String> dataSourceMapProperties) {
        super(PlanType.ALTER_ROUTINE_LOAD_COMMAND);
        Objects.requireNonNull(labelNameInfo, "labelNameInfo is null");
        Objects.requireNonNull(jobProperties, "jobProperties is null");
        Objects.requireNonNull(dataSourceMapProperties, "dataSourceMapProperties is null");
        this.labelNameInfo = labelNameInfo;
        this.loadPropertyMap = loadPropertyMap == null ? Maps.newHashMap() : loadPropertyMap;
        this.jobProperties = jobProperties;
        this.dataSourceMapProperties = dataSourceMapProperties;
        this.isPartialUpdate = this.jobProperties.getOrDefault(CreateRoutineLoadInfo.PARTIAL_COLUMNS, "false")
            .equalsIgnoreCase("true");
    }

    public AlterRoutineLoadCommand(LabelNameInfo labelNameInfo,
                                   Map<String, String> jobProperties,
                                   Map<String, String> dataSourceMapProperties) {
        this(labelNameInfo, Maps.newHashMap(), jobProperties, dataSourceMapProperties);
    }

    public String getDbName() {
        return labelNameInfo.getDb();
    }

    public String getJobName() {
        return labelNameInfo.getLabel();
    }

    public Map<String, String> getAnalyzedJobProperties() {
        return analyzedJobProperties;
    }

    public boolean hasDataSourceProperty() {
        return MapUtils.isNotEmpty(dataSourceMapProperties);
    }

    public Map<String, String> getDataSourceMapProperties() {
        return dataSourceMapProperties;
    }

    public AbstractDataSourceProperties getDataSourceProperties() {
        return dataSourceProperties;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().getRoutineLoadManager().alterRoutineLoadJob(this);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
        labelNameInfo.validate(ctx);
        FeNameFormat.checkCommonName(NAME_TYPE, labelNameInfo.getLabel());
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();
        // check load properties
        RoutineLoadJob job = Env.getCurrentEnv().getRoutineLoadManager()
                .getJob(getDbName(), getJobName());
        this.routineLoadDesc = CreateRoutineLoadInfo.checkLoadProperties(ctx, loadPropertyMap,
                job.getDbFullName(), job.getTableName(), job.isMultiTable(), job.getMergeType());
        // check data source properties
        checkDataSourceProperties();
        checkPartialUpdate();
        if (analyzedJobProperties.isEmpty() && MapUtils.isEmpty(dataSourceMapProperties)
                && routineLoadDesc == null) {
            throw new AnalysisException("No properties are specified");
        }
    }

    private void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !CONFIGURABLE_JOB_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY)) {
            long desiredConcurrentNum = ((Long) Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY),
                    -1, CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PRED,
                    CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY + " should > 0")).intValue();
            analyzedJobProperties.put(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY,
                    String.valueOf(desiredConcurrentNum));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY)) {
            long maxErrorNum = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY),
                    -1, CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PRED,
                    CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY + " should >= 0");
            analyzedJobProperties.put(CreateRoutineLoadInfo.MAX_ERROR_NUMBER_PROPERTY,
                    String.valueOf(maxErrorNum));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY)) {
            double maxFilterRatio = Util.getDoublePropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY),
                    -1, CreateRoutineLoadInfo.MAX_FILTER_RATIO_PRED,
                    CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY + " should between 0 and 1");
            analyzedJobProperties.put(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY,
                    String.valueOf(maxFilterRatio));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY)) {
            long maxBatchIntervalS = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY),
                    -1, CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_PRED,
                    CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY + " should >= 1");
            analyzedJobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_INTERVAL_SEC_PROPERTY,
                    String.valueOf(maxBatchIntervalS));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY)) {
            long maxBatchRows = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY),
                    -1, CreateRoutineLoadInfo.MAX_BATCH_ROWS_PRED,
                    CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY + " should > 200000");
            analyzedJobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_ROWS_PROPERTY,
                    String.valueOf(maxBatchRows));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY)) {
            long maxBatchSizeBytes = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY),
                    -1, CreateRoutineLoadInfo.MAX_BATCH_SIZE_PRED,
                    CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY + " should between 100MB and 10GB");
            analyzedJobProperties.put(CreateRoutineLoadInfo.MAX_BATCH_SIZE_PROPERTY,
                    String.valueOf(maxBatchSizeBytes));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.STRICT_MODE)) {
            boolean strictMode = Boolean.valueOf(jobProperties.get(CreateRoutineLoadInfo.STRICT_MODE));
            analyzedJobProperties.put(CreateRoutineLoadInfo.STRICT_MODE, String.valueOf(strictMode));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.TIMEZONE)) {
            String timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties
                    .get(CreateRoutineLoadInfo.TIMEZONE));
            analyzedJobProperties.put(CreateRoutineLoadInfo.TIMEZONE, timezone);
        }

        if (jobProperties.containsKey(JsonFileFormatProperties.PROP_JSON_PATHS)) {
            analyzedJobProperties.put(JsonFileFormatProperties.PROP_JSON_PATHS,
                    jobProperties.get(JsonFileFormatProperties.PROP_JSON_PATHS));
        }

        if (jobProperties.containsKey(JsonFileFormatProperties.PROP_JSON_ROOT)) {
            analyzedJobProperties.put(JsonFileFormatProperties.PROP_JSON_ROOT,
                    jobProperties.get(JsonFileFormatProperties.PROP_JSON_ROOT));
        }

        if (jobProperties.containsKey(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY)) {
            boolean stripOuterArray = Boolean.parseBoolean(
                    jobProperties.get(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY));
            analyzedJobProperties.put(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, String.valueOf(stripOuterArray));
        }

        if (jobProperties.containsKey(JsonFileFormatProperties.PROP_NUM_AS_STRING)) {
            boolean numAsString = Boolean.parseBoolean(jobProperties.get(JsonFileFormatProperties.PROP_NUM_AS_STRING));
            analyzedJobProperties.put(JsonFileFormatProperties.PROP_NUM_AS_STRING, String.valueOf(numAsString));
        }

        if (jobProperties.containsKey(JsonFileFormatProperties.PROP_FUZZY_PARSE)) {
            boolean fuzzyParse = Boolean.parseBoolean(jobProperties.get(JsonFileFormatProperties.PROP_FUZZY_PARSE));
            analyzedJobProperties.put(JsonFileFormatProperties.PROP_FUZZY_PARSE, String.valueOf(fuzzyParse));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.PARTIAL_COLUMNS)) {
            analyzedJobProperties.put(CreateRoutineLoadInfo.PARTIAL_COLUMNS,
                    String.valueOf(isPartialUpdate));
        }

        if (jobProperties.containsKey(CreateRoutineLoadInfo.WORKLOAD_GROUP)) {
            String workloadGroup = jobProperties.get(CreateRoutineLoadInfo.WORKLOAD_GROUP);
            if (!StringUtil.isEmpty(workloadGroup)) {
                // NOTE: delay check workload group's existence check when alter routine load job
                // because we can only get clusterId when alter job.
                analyzedJobProperties.put(CreateRoutineLoadInfo.WORKLOAD_GROUP,
                        jobProperties.get(CreateRoutineLoadInfo.WORKLOAD_GROUP));
            }
        }

        if (jobProperties.containsKey(CsvFileFormatProperties.PROP_ENCLOSE)) {
            analyzedJobProperties.put(CsvFileFormatProperties.PROP_ENCLOSE,
                    jobProperties.get(CsvFileFormatProperties.PROP_ENCLOSE));
        }

        if (jobProperties.containsKey(CsvFileFormatProperties.PROP_ESCAPE)) {
            analyzedJobProperties.put(CsvFileFormatProperties.PROP_ESCAPE,
                    jobProperties.get(CsvFileFormatProperties.PROP_ESCAPE));
        }
    }

    private void checkDataSourceProperties() throws UserException {
        if (MapUtils.isEmpty(dataSourceMapProperties)) {
            return;
        }
        RoutineLoadJob job = Env.getCurrentEnv().getRoutineLoadManager()
                .getJob(getDbName(), getJobName());
        this.dataSourceProperties = RoutineLoadDataSourcePropertyFactory
            .createDataSource(job.getDataSourceType().name(), dataSourceMapProperties, job.isMultiTable());
        dataSourceProperties.setAlter(true);
        dataSourceProperties.setTimezone(job.getTimezone());
        dataSourceProperties.analyze();
    }

    private void checkPartialUpdate() throws UserException {
        if (!isPartialUpdate) {
            return;
        }
        RoutineLoadJob job = Env.getCurrentEnv().getRoutineLoadManager()
                .getJob(getDbName(), getDbName());
        if (job.isMultiTable()) {
            throw new AnalysisException("load by PARTIAL_COLUMNS is not supported in multi-table load.");
        }
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(job.getDbFullName());
        Table table = db.getTableOrAnalysisException(job.getTableName());
        if (isPartialUpdate && !((OlapTable) table).getEnableUniqueKeyMergeOnWrite()) {
            throw new AnalysisException("load by PARTIAL_COLUMNS is only supported in unique table MoW");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterRoutineLoadCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
