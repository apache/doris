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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.routineload.RoutineLoadJob;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

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
            .add(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY)
            .add(CreateRoutineLoadStmt.JSONPATHS)
            .add(CreateRoutineLoadStmt.JSONROOT)
            .add(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY)
            .add(CreateRoutineLoadStmt.NUM_AS_STRING)
            .add(CreateRoutineLoadStmt.FUZZY_PARSE)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.TIMEZONE)
            .build();

    private final LabelName labelName;
    private final Map<String, String> jobProperties;
    private final RoutineLoadDataSourceProperties dataSourceProperties;

    // save analyzed job properties.
    // analyzed data source properties are saved in dataSourceProperties.
    private Map<String, String> analyzedJobProperties = Maps.newHashMap();

    public AlterRoutineLoadStmt(LabelName labelName, Map<String, String> jobProperties,
                                RoutineLoadDataSourceProperties dataSourceProperties) {
        this.labelName = labelName;
        this.jobProperties = jobProperties != null ? jobProperties : Maps.newHashMap();
        this.dataSourceProperties = dataSourceProperties;
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
        return dataSourceProperties.hasAnalyzedProperties();
    }

    public RoutineLoadDataSourceProperties getDataSourceProperties() {
        return dataSourceProperties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        labelName.analyze(analyzer);
        FeNameFormat.checkCommonName(NAME_TYPE, labelName.getLabelName());
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();
        // check data source properties
        checkDataSourceProperties();

        if (analyzedJobProperties.isEmpty() && !dataSourceProperties.hasAnalyzedProperties()) {
            throw new AnalysisException("No properties are specified");
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

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)) {
            long maxBatchIntervalS = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY + " should between 5 and 60");
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
                    CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY + " should between 100MB and 1GB");
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
            analyzedJobProperties.put(CreateRoutineLoadStmt.JSONPATHS, jobProperties.get(CreateRoutineLoadStmt.JSONPATHS));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.JSONROOT)) {
            analyzedJobProperties.put(CreateRoutineLoadStmt.JSONROOT, jobProperties.get(CreateRoutineLoadStmt.JSONROOT));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY)) {
            boolean stripOuterArray = Boolean.valueOf(jobProperties.get(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY));
            analyzedJobProperties.put(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY, String.valueOf(stripOuterArray));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.NUM_AS_STRING)) {
            boolean numAsString = Boolean.valueOf(jobProperties.get(CreateRoutineLoadStmt.NUM_AS_STRING));
            analyzedJobProperties.put(CreateRoutineLoadStmt.NUM_AS_STRING, String.valueOf(numAsString));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.FUZZY_PARSE)) {
            boolean fuzzyParse = Boolean.valueOf(jobProperties.get(CreateRoutineLoadStmt.FUZZY_PARSE));
            analyzedJobProperties.put(CreateRoutineLoadStmt.FUZZY_PARSE, String.valueOf(fuzzyParse));
        }
    }

    private void checkDataSourceProperties() throws UserException {
        if (!FeConstants.runningUnitTest) {
            RoutineLoadJob job = Catalog.getCurrentCatalog().getRoutineLoadManager().checkPrivAndGetJob(getDbName(), getLabel());
            dataSourceProperties.setTimezone(job.getTimezone());
        } else {
            dataSourceProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        }
        dataSourceProperties.analyze();
    }
}
