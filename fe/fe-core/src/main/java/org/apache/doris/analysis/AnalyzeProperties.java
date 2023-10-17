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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;

import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.util.CronExpression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

// TODO: Remove map
public class AnalyzeProperties {

    public static final String PROPERTY_SYNC = "sync";
    public static final String PROPERTY_INCREMENTAL = "incremental";
    public static final String PROPERTY_AUTOMATIC = "automatic";
    public static final String PROPERTY_SAMPLE_PERCENT = "sample.percent";
    public static final String PROPERTY_SAMPLE_ROWS = "sample.rows";
    public static final String PROPERTY_NUM_BUCKETS = "num.buckets";
    public static final String PROPERTY_ANALYSIS_TYPE = "analysis.type";
    public static final String PROPERTY_PERIOD_SECONDS = "period.seconds";
    public static final String PROPERTY_FORCE_FULL = "force.full";
    public static final String PROPERTY_PARTITION_COLUMN_FROM_SQL = "partition.column.from.sql";

    public static final AnalyzeProperties DEFAULT_PROP = new AnalyzeProperties(new HashMap<String, String>() {
        {
            put(AnalyzeProperties.PROPERTY_SYNC, "false");
            put(AnalyzeProperties.PROPERTY_AUTOMATIC, "false");
            put(AnalyzeProperties.PROPERTY_ANALYSIS_TYPE, AnalysisType.FUNDAMENTALS.toString());
        }
    });

    public static final String PROPERTY_PERIOD_CRON = "period.cron";

    private CronExpression cronExpression;

    @SerializedName("analyzeProperties")
    private final Map<String, String> properties;

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(PROPERTY_SYNC)
            .add(PROPERTY_INCREMENTAL)
            .add(PROPERTY_AUTOMATIC)
            .add(PROPERTY_SAMPLE_PERCENT)
            .add(PROPERTY_SAMPLE_ROWS)
            .add(PROPERTY_NUM_BUCKETS)
            .add(PROPERTY_ANALYSIS_TYPE)
            .add(PROPERTY_PERIOD_SECONDS)
            .add(PROPERTY_PERIOD_CRON)
            .add(PROPERTY_FORCE_FULL)
            .add(PROPERTY_PARTITION_COLUMN_FROM_SQL)
            .build();

    public AnalyzeProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void check() throws AnalysisException {
        String msgTemplate = "%s = %s is invalid property";
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();

        if (optional.isPresent()) {
            String msg = String.format(msgTemplate, optional.get(), properties.get(optional.get()));
            throw new AnalysisException(msg);
        }
        checkSampleValue();
        checkPeriodSeconds();
        checkNumBuckets();
        checkSync(msgTemplate);
        checkAnalysisMode(msgTemplate);
        checkAnalysisType(msgTemplate);
        checkScheduleType(msgTemplate);
        checkPeriod();
    }

    public boolean isSync() {
        return Boolean.parseBoolean(properties.get(PROPERTY_SYNC));
    }

    public boolean isIncremental() {
        return Boolean.parseBoolean(properties.get(PROPERTY_INCREMENTAL));
    }

    public boolean isAutomatic() {
        return Boolean.parseBoolean(properties.get(PROPERTY_AUTOMATIC));
    }

    public int getSamplePercent() {
        if (!properties.containsKey(PROPERTY_SAMPLE_PERCENT)) {
            return 0;
        }
        return Integer.parseInt(properties.get(PROPERTY_SAMPLE_PERCENT));
    }

    public int getSampleRows() {
        if (!properties.containsKey(PROPERTY_SAMPLE_ROWS)) {
            return 0;
        }
        return Integer.parseInt(properties.get(PROPERTY_SAMPLE_ROWS));
    }

    public int getNumBuckets() {
        if (!properties.containsKey(PROPERTY_NUM_BUCKETS)) {
            return 0;
        }
        return Integer.parseInt(properties.get(PROPERTY_NUM_BUCKETS));
    }

    public long getPeriodTimeInMs() {
        if (!properties.containsKey(PROPERTY_PERIOD_SECONDS)) {
            return 0;
        }
        int minutes = Integer.parseInt(properties.get(PROPERTY_PERIOD_SECONDS));
        return TimeUnit.SECONDS.toMillis(minutes);
    }

    public CronExpression getCron() {
        return cronExpression;
    }

    private void checkPeriodSeconds() throws AnalysisException {
        if (properties.containsKey(PROPERTY_PERIOD_SECONDS)) {
            checkNumericProperty(PROPERTY_PERIOD_SECONDS, properties.get(PROPERTY_PERIOD_SECONDS),
                    1, Integer.MAX_VALUE, true, "needs at least 1 seconds");
        }
    }

    private void checkSampleValue() throws AnalysisException {
        if (properties.containsKey(PROPERTY_SAMPLE_PERCENT)
                && properties.containsKey(PROPERTY_SAMPLE_ROWS)) {
            throw new AnalysisException("only one sampling parameter can be specified simultaneously");
        }

        if (properties.containsKey(PROPERTY_SAMPLE_PERCENT)) {
            checkNumericProperty(PROPERTY_SAMPLE_PERCENT, properties.get(PROPERTY_SAMPLE_PERCENT),
                    1, 100, true, "should be >= 1 and <= 100");
        }

        if (properties.containsKey(PROPERTY_SAMPLE_ROWS)) {
            checkNumericProperty(PROPERTY_SAMPLE_ROWS, properties.get(PROPERTY_SAMPLE_ROWS),
                    0, Integer.MAX_VALUE, false, "needs at least 1 row");
        }
    }

    private void checkNumBuckets() throws AnalysisException {
        if (properties.containsKey(PROPERTY_NUM_BUCKETS)) {
            checkNumericProperty(PROPERTY_NUM_BUCKETS, properties.get(PROPERTY_NUM_BUCKETS),
                    1, Integer.MAX_VALUE, true, "needs at least 1 buckets");
        }

        if (properties.containsKey(PROPERTY_NUM_BUCKETS)
                && AnalysisType.valueOf(properties.get(PROPERTY_ANALYSIS_TYPE)) != AnalysisType.HISTOGRAM) {
            throw new AnalysisException(PROPERTY_NUM_BUCKETS + " can only be specified when collecting histograms");
        }
    }

    private void checkSync(String msgTemplate) throws AnalysisException {
        if (properties.containsKey(PROPERTY_SYNC)) {
            try {
                Boolean.valueOf(properties.get(PROPERTY_SYNC));
            } catch (NumberFormatException e) {
                String msg = String.format(msgTemplate, PROPERTY_SYNC, properties.get(PROPERTY_SYNC));
                throw new AnalysisException(msg);
            }
        }
    }

    private void checkAnalysisMode(String msgTemplate) throws AnalysisException {
        if (properties.containsKey(PROPERTY_INCREMENTAL)) {
            try {
                Boolean.valueOf(properties.get(PROPERTY_INCREMENTAL));
            } catch (NumberFormatException e) {
                String msg = String.format(msgTemplate, PROPERTY_INCREMENTAL, properties.get(PROPERTY_INCREMENTAL));
                throw new AnalysisException(msg);
            }
        }
        if (properties.containsKey(PROPERTY_INCREMENTAL)
                && AnalysisType.valueOf(properties.get(PROPERTY_ANALYSIS_TYPE)) == AnalysisType.HISTOGRAM) {
            throw new AnalysisException(PROPERTY_INCREMENTAL + " analysis of histograms is not supported");
        }
    }

    private void checkAnalysisType(String msgTemplate) throws AnalysisException {
        if (properties.containsKey(PROPERTY_ANALYSIS_TYPE)) {
            try {
                AnalysisType.valueOf(properties.get(PROPERTY_ANALYSIS_TYPE));
            } catch (NumberFormatException e) {
                String msg = String.format(msgTemplate, PROPERTY_ANALYSIS_TYPE, properties.get(PROPERTY_ANALYSIS_TYPE));
                throw new AnalysisException(msg);
            }
        }
    }

    private void checkScheduleType(String msgTemplate) throws AnalysisException {
        if (properties.containsKey(PROPERTY_AUTOMATIC)) {
            try {
                Boolean.valueOf(properties.get(PROPERTY_AUTOMATIC));
            } catch (NumberFormatException e) {
                String msg = String.format(msgTemplate, PROPERTY_AUTOMATIC, properties.get(PROPERTY_AUTOMATIC));
                throw new AnalysisException(msg);
            }
        }
        if (properties.containsKey(PROPERTY_AUTOMATIC)
                && properties.containsKey(PROPERTY_INCREMENTAL)) {
            throw new AnalysisException(PROPERTY_INCREMENTAL + " is invalid when analyze automatically statistics");
        }
        if (properties.containsKey(PROPERTY_AUTOMATIC)
                && properties.containsKey(PROPERTY_PERIOD_SECONDS)) {
            throw new AnalysisException(PROPERTY_PERIOD_SECONDS + " is invalid when analyze automatically statistics");
        }
    }

    private void checkPeriod() throws AnalysisException {
        if (properties.containsKey(PROPERTY_PERIOD_SECONDS)
                && properties.containsKey(PROPERTY_PERIOD_CRON)) {
            throw new AnalysisException(PROPERTY_PERIOD_SECONDS + " and " + PROPERTY_PERIOD_CRON
                    + " couldn't be set simultaneously");
        }
        String cronExprStr = properties.get(PROPERTY_PERIOD_CRON);
        if (cronExprStr != null) {
            try {
                cronExpression = new CronExpression(cronExprStr);
            } catch (java.text.ParseException e) {
                throw new AnalysisException("Invalid cron expression: " + cronExprStr);
            }
        }
    }

    private void checkNumericProperty(String key, String value, int lowerBound, int upperBound,
            boolean includeBoundary, String errorMsg) throws AnalysisException {
        if (!StringUtils.isNumeric(value)) {
            String msg = String.format("%s = %s is an invalid property.", key, value);
            throw new AnalysisException(msg);
        }
        int intValue = Integer.parseInt(value);
        boolean isOutOfBounds = (includeBoundary && (intValue < lowerBound || intValue > upperBound))
                || (!includeBoundary && (intValue <= lowerBound || intValue >= upperBound));
        if (isOutOfBounds) {
            throw new AnalysisException(key + " " + errorMsg);
        }
    }

    public boolean isSample() {
        return properties.containsKey(PROPERTY_SAMPLE_PERCENT)
                || properties.containsKey(PROPERTY_SAMPLE_ROWS);
    }

    public boolean forceFull() {
        return properties.containsKey(PROPERTY_FORCE_FULL);
    }

    public boolean isSampleRows() {
        return properties.containsKey(PROPERTY_SAMPLE_ROWS);
    }

    public boolean usingSqlForPartitionColumn() {
        return properties.containsKey(PROPERTY_PARTITION_COLUMN_FROM_SQL);
    }

    public String toSQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("PROPERTIES(");
        sb.append(new PrintableMap<>(properties, " = ",
                true,
                false));
        sb.append(")");
        return sb.toString();
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public AnalysisType getAnalysisType() {
        return AnalysisType.valueOf(properties.get(PROPERTY_ANALYSIS_TYPE));
    }
}
