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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.RoutineLoadDataSourcePropertyFactory;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.collections.MapUtils;

import java.util.Map;
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
public class AlterRoutineLoadStmt extends DdlStmt {

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    private static final ImmutableSet<String> CONFIGURABLE_JOB_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY)
            .add(CreateRoutineLoadStmt.JSONPATHS)
            .add(CreateRoutineLoadStmt.JSONROOT)
            .add(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY)
            .add(CreateRoutineLoadStmt.NUM_AS_STRING)
            .add(CreateRoutineLoadStmt.FUZZY_PARSE)
            .add(CreateRoutineLoadStmt.PARTIAL_COLUMNS)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.TIMEZONE)
            .add(CreateRoutineLoadStmt.WORKLOAD_GROUP)
            .add(LoadStmt.KEY_ENCLOSE)
            .add(LoadStmt.KEY_ESCAPE)
            .build();

    private final LabelName labelName;
    private final Map<String, String> jobProperties;
    private final Map<String, String> dataSourceMapProperties;

    private boolean isPartialUpdate;

    // save analyzed job properties.
    // analyzed data source properties are saved in dataSourceProperties.
    private Map<String, String> analyzedJobProperties = Maps.newHashMap();

    public AlterRoutineLoadStmt(LabelName labelName, Map<String, String> jobProperties,
                                Map<String, String> dataSourceProperties) {
        this.labelName = labelName;
        this.jobProperties = jobProperties != null ? jobProperties : Maps.newHashMap();
        this.dataSourceMapProperties = dataSourceProperties != null ? dataSourceProperties : Maps.newHashMap();
        this.isPartialUpdate = this.jobProperties.getOrDefault(CreateRoutineLoadStmt.PARTIAL_COLUMNS, "false")
                .equalsIgnoreCase("true");
    }

    public String getDbName() {
        return labelName.getDbName();
    }

    public String getLabel() {
        return labelName.getLabelName();
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

    @Getter
    public AbstractDataSourceProperties dataSourceProperties;

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        labelName.analyze(analyzer);
        FeNameFormat.checkCommonName(NAME_TYPE, labelName.getLabelName());
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();
        // check data source properties
        checkDataSourceProperties();
        checkPartialUpdate();

        if (analyzedJobProperties.isEmpty() && MapUtils.isEmpty(dataSourceMapProperties)) {
            throw new AnalysisException("No properties are specified");
        }
    }

    private void checkPartialUpdate() throws UserException {
        if (!isPartialUpdate) {
            return;
        }
        RoutineLoadJob job = Env.getCurrentEnv().getRoutineLoadManager()
                .getJob(getDbName(), getLabel());
        if (job.isMultiTable()) {
            throw new AnalysisException("load by PARTIAL_COLUMNS is not supported in multi-table load.");
        }
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(job.getDbFullName());
        Table table = db.getTableOrAnalysisException(job.getTableName());
        if (isPartialUpdate && !((OlapTable) table).getEnableUniqueKeyMergeOnWrite()) {
            throw new AnalysisException("load by PARTIAL_COLUMNS is only supported in unique table MoW");
        }
    }

    private void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !CONFIGURABLE_JOB_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)) {
            long desiredConcurrentNum = ((Long) Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY),
                    -1, CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PRED,
                    CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY + " should > 0")).intValue();
            analyzedJobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY,
                    String.valueOf(desiredConcurrentNum));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)) {
            long maxErrorNum = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PRED,
                    CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY + " should >= 0");
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY,
                    String.valueOf(maxErrorNum));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
            double maxFilterRatio = Util.getDoublePropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_FILTER_RATIO_PRED,
                    CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY + " should between 0 and 1");
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY,
                    String.valueOf(maxFilterRatio));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)) {
            long maxBatchIntervalS = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY + " should >= 1");
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY,
                    String.valueOf(maxBatchIntervalS));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)) {
            long maxBatchRows = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_ROWS_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY + " should > 200000");
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY,
                    String.valueOf(maxBatchRows));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY)) {
            long maxBatchSizeBytes = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_SIZE_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY + " should between 100MB and 10GB");
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY,
                    String.valueOf(maxBatchSizeBytes));
        }

        if (jobProperties.containsKey(LoadStmt.STRICT_MODE)) {
            boolean strictMode = Boolean.valueOf(jobProperties.get(LoadStmt.STRICT_MODE));
            analyzedJobProperties.put(LoadStmt.STRICT_MODE, String.valueOf(strictMode));
        }

        if (jobProperties.containsKey(LoadStmt.TIMEZONE)) {
            String timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties.get(LoadStmt.TIMEZONE));
            analyzedJobProperties.put(LoadStmt.TIMEZONE, timezone);
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.JSONPATHS)) {
            analyzedJobProperties.put(CreateRoutineLoadStmt.JSONPATHS,
                    jobProperties.get(CreateRoutineLoadStmt.JSONPATHS));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.JSONROOT)) {
            analyzedJobProperties.put(CreateRoutineLoadStmt.JSONROOT,
                    jobProperties.get(CreateRoutineLoadStmt.JSONROOT));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY)) {
            boolean stripOuterArray = Boolean.parseBoolean(jobProperties.get(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY));
            analyzedJobProperties.put(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY, String.valueOf(stripOuterArray));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.NUM_AS_STRING)) {
            boolean numAsString = Boolean.parseBoolean(jobProperties.get(CreateRoutineLoadStmt.NUM_AS_STRING));
            analyzedJobProperties.put(CreateRoutineLoadStmt.NUM_AS_STRING, String.valueOf(numAsString));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.FUZZY_PARSE)) {
            boolean fuzzyParse = Boolean.parseBoolean(jobProperties.get(CreateRoutineLoadStmt.FUZZY_PARSE));
            analyzedJobProperties.put(CreateRoutineLoadStmt.FUZZY_PARSE, String.valueOf(fuzzyParse));
        }
        if (jobProperties.containsKey(CreateRoutineLoadStmt.PARTIAL_COLUMNS)) {
            analyzedJobProperties.put(CreateRoutineLoadStmt.PARTIAL_COLUMNS,
                    String.valueOf(isPartialUpdate));
        }
        if (jobProperties.containsKey(CreateRoutineLoadStmt.WORKLOAD_GROUP)) {
            String workloadGroup = jobProperties.get(CreateRoutineLoadStmt.WORKLOAD_GROUP);
            long wgId = Env.getCurrentEnv().getWorkloadGroupMgr()
                    .getWorkloadGroup(ConnectContext.get().getCurrentUserIdentity(), workloadGroup);
            analyzedJobProperties.put(CreateRoutineLoadStmt.WORKLOAD_GROUP, String.valueOf(wgId));
        }
        if (jobProperties.containsKey(LoadStmt.KEY_ENCLOSE)) {
            analyzedJobProperties.put(LoadStmt.KEY_ENCLOSE, jobProperties.get(LoadStmt.KEY_ENCLOSE));
        }
        if (jobProperties.containsKey(LoadStmt.KEY_ESCAPE)) {
            analyzedJobProperties.put(LoadStmt.KEY_ESCAPE, jobProperties.get(LoadStmt.KEY_ESCAPE));
        }
    }

    private void checkDataSourceProperties() throws UserException {
        if (MapUtils.isEmpty(dataSourceMapProperties)) {
            return;
        }
        RoutineLoadJob job = Env.getCurrentEnv().getRoutineLoadManager()
                .getJob(getDbName(), getLabel());
        this.dataSourceProperties = RoutineLoadDataSourcePropertyFactory
                .createDataSource(job.getDataSourceType().name(), dataSourceMapProperties, job.isMultiTable());
        dataSourceProperties.setAlter(true);
        dataSourceProperties.setTimezone(job.getTimezone());
        dataSourceProperties.analyze();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }

}
