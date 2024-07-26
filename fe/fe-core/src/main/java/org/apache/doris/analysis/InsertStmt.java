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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This is the unified abstract stmt for all load kinds of load in {@link LoadType}
 * All contents of native InsertStmt is moved to {@link NativeInsertStmt}
 * Currently this abstract class keep the native insert methods for compatibility, and will eventually be moved
 * to {@link NativeInsertStmt}
 */
@Deprecated
public abstract class InsertStmt extends DdlStmt implements NotFallbackInParser {

    public static class Properties {

        public static final String EXEC_MEM_LIMIT = "exec_mem_limit";

        public static final String TIMEZONE = "timezone";

        public static final String STRICT_MODE = "strict_mode";

        public static final String MAX_FILTER_RATIO = "max_filter_ratio";

        public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";

        public static final String TIMEOUT_PROPERTY = "timeout";

        public static final String LOAD_TO_SINGLE_TABLET = "load_to_single_tablet";

        public static final String TRIM_DOUBLE_QUOTES = "trim_double_quotes";

        public static final String SKIP_LINES = "skip_lines";

        public static final String LINE_DELIMITER = "line_delimiter";

        public static final String COLUMN_SEPARATOR = "column_separator";

        // ------------------------ just for routine load ------------------------

        public static final String MAX_BATCH_SIZE = "max_batch_size";

        public static final String MAX_BATCH_ROWS = "max_batch_rows";

        public static final String MAX_BATCH_INTERVAL = "max_batch_interval";

        // -----------------------------------------------------------------------

        // TODO: to be discovered in developing

    }

    protected LabelName label;

    protected Map<String, String> properties;

    protected List<Table> targetTables;

    protected String comments;

    /**
     * TODO: change Function to Util.getXXXPropertyOrDefault()
     */
    public static final ImmutableMap<String, Function<String, ?>> PROPERTIES_MAP =
            new Builder<String, Function<String, ?>>()
                    .put(Properties.EXEC_MEM_LIMIT, (Function<String, Long>) Long::valueOf)
                    .put(Properties.TIMEOUT_PROPERTY, (Function<String, Long>) Long::valueOf)
                    .put(Properties.TIMEZONE, (Function<String, String>) input -> input)
                    .put(Properties.LOAD_TO_SINGLE_TABLET,
                            (Function<String, Boolean>) Boolean::parseBoolean)
                    .put(Properties.MAX_FILTER_RATIO, (Function<String, Double>) Double::parseDouble)
                    .put(Properties.SEND_BATCH_PARALLELISM,
                            (Function<String, Integer>) Integer::parseInt)
                    .put(Properties.STRICT_MODE, (Function<String, Boolean>) Boolean::parseBoolean)
                    .put(Properties.TRIM_DOUBLE_QUOTES, (Function<String, Boolean>) Boolean::parseBoolean)
                    .put(Properties.SKIP_LINES, (Function<String, Integer>) Integer::valueOf)
                    .put(Properties.LINE_DELIMITER, (Function<String, String>) String::valueOf)
                    .put(Properties.COLUMN_SEPARATOR, (Function<String, String>) String::valueOf)
                    .put(Properties.MAX_BATCH_SIZE, (Function<String, Long>) Long::parseLong)
                    .put(Properties.MAX_BATCH_ROWS, (Function<String, Integer>) Integer::valueOf)
                    .put(Properties.MAX_BATCH_INTERVAL, (Function<String, Long>) Long::valueOf)
                    .build();

    public InsertStmt(LabelName label, Map<String, String> properties, String comment) {
        this.label = label;
        this.properties = properties;
        this.comments = comment != null ? comment : "";
    }

    // ---------------------------- for old insert stmt ----------------------------

    public boolean isValuesOrConstantSelect() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public Table getTargetTable() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void setTargetTable(Table targetTable) {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public long getTransactionId() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public Boolean isRepartition() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public String getDbName() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public String getTbl() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void getTables(Analyzer analyzer, Map<Long, TableIf> tableMap, Set<String> parentViewNameSet)
            throws AnalysisException {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public QueryStmt getQueryStmt() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void setQueryStmt(QueryStmt queryStmt) {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public String getLabel() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public DataSink getDataSink() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public DatabaseIf getDbObj() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public boolean isTransactionBegin() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void prepareExpressions() throws UserException {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void complete() throws UserException {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public DataPartition getDataPartition() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    // ---------------------------------------------------------------------------

    // ------------------------- for unified insert stmt -------------------------

    public boolean needLoadManager() {
        return getLoadType() != LoadType.NATIVE_INSERT && getLoadType() != LoadType.UNKNOWN;
    }

    public LabelName getLoadLabel() {
        return label;
    }

    /**
     * for multi-tables load, we need have several target tbl
     *
     * @return all target table names
     */
    public List<Table> getTargetTableList() {
        return targetTables;
    }

    public abstract List<? extends DataDesc> getDataDescList();

    public abstract ResourceDesc getResourceDesc();

    public abstract LoadType getLoadType();

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComments() {
        return comments;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        analyzeProperties();
    }

    protected void analyzeProperties() throws DdlException {
        checkProperties();
    }

    /**
     * TODO(tsy): find a shorter way to check props
     */
    private void checkProperties() throws DdlException {
        if (MapUtils.isEmpty(properties)) {
            return;
        }

        for (Entry<String, String> entry : properties.entrySet()) {
            if (!InsertStmt.PROPERTIES_MAP.containsKey(entry.getKey())) {
                throw new DdlException(entry.getKey() + " is invalid property");
            }
        }

        // exec mem
        final String execMemProperty = properties.get(InsertStmt.Properties.EXEC_MEM_LIMIT);
        if (execMemProperty != null) {
            try {
                final long execMem = Long.parseLong(execMemProperty);
                if (execMem <= 0) {
                    throw new DdlException(InsertStmt.Properties.EXEC_MEM_LIMIT + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(InsertStmt.Properties.EXEC_MEM_LIMIT + " is not a number.");
            }
        }

        // timeout
        final String timeoutLimitProperty = properties.get(InsertStmt.Properties.TIMEOUT_PROPERTY);
        if (timeoutLimitProperty != null) {
            try {
                final int timeoutLimit = Integer.parseInt(timeoutLimitProperty);
                if (timeoutLimit < 0) {
                    throw new DdlException(InsertStmt.Properties.TIMEOUT_PROPERTY + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(InsertStmt.Properties.TIMEOUT_PROPERTY + " is not a number.");
            }
        }

        // max filter ratio
        final String maxFilterRadioProperty = properties.get(Properties.MAX_FILTER_RATIO);
        if (maxFilterRadioProperty != null) {
            try {
                double maxFilterRatio = Double.parseDouble(maxFilterRadioProperty);
                if (maxFilterRatio < 0.0 || maxFilterRatio > 1.0) {
                    throw new DdlException(Properties.MAX_FILTER_RATIO + " must between 0.0 and 1.0.");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(Properties.MAX_FILTER_RATIO + " is not a number.");
            }
        }

        // strict mode
        final String strictModeProperty = properties.get(Properties.STRICT_MODE);
        if (strictModeProperty != null) {
            if (!strictModeProperty.equalsIgnoreCase("true")
                    && !strictModeProperty.equalsIgnoreCase("false")) {
                throw new DdlException(Properties.STRICT_MODE + " is not a boolean");
            }
        }

        // time zone
        final String timezone = properties.get(Properties.TIMEZONE);
        if (timezone != null) {
            properties.put(Properties.TIMEZONE, TimeUtils.checkTimeZoneValidAndStandardize(
                    properties.getOrDefault(LoadStmt.TIMEZONE, TimeUtils.DEFAULT_TIME_ZONE)));
        }

        // send batch parallelism
        final String sendBatchParallelism = properties.get(Properties.SEND_BATCH_PARALLELISM);
        if (sendBatchParallelism != null) {
            try {
                final int sendBatchParallelismValue = Integer.parseInt(sendBatchParallelism);
                if (sendBatchParallelismValue < 1) {
                    throw new DdlException(Properties.SEND_BATCH_PARALLELISM + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(Properties.SEND_BATCH_PARALLELISM + " is not a number.");
            }
        }
    }

    public NativeInsertStmt getNativeInsertStmt() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    /**
     * TODO: unify the data_desc
     * the unique entrance for data_desc
     */
    interface DataDesc {

        String toSql();

    }

    @Override
    public StmtType stmtType() {
        return StmtType.INSERT;
    }
}
