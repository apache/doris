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

import org.apache.doris.analysis.InsertStmt.Properties;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class is temporary for load refactor and all contents may be moved to InsertStmt after refactoring
 * <p>
 * TODO(tsy): rm after refactor
 */
public abstract class AbstractInsertStmtProxy extends DdlStmt {

    protected LabelName label;

    protected Map<String, String> properties;

    protected List<Table> targetTables;

    protected String comments;

    public LabelName getLabel() {
        return label;
    }

    /**
     * for multi-tables load, we need have several target tbl
     *
     * @return all target table names
     */
    public List<Table> getTargetTables() {
        return targetTables;
    }

    public abstract List<? extends DataDesc> getDataDescList();

    public abstract ResourceDesc getResourceDesc();

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComments() {
        return comments;
    }

    public abstract LoadType getLoadType();

    protected void analyzeProperties() throws DdlException {
        checkProperties();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        analyzeProperties();
    }

    /**
     * TODO(tsy): found a shorter way to check props
     */
    private void checkProperties() throws DdlException {
        if (properties == null) {
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
                final long execMem = Long.valueOf(execMemProperty);
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
                final int timeoutLimit = Integer.valueOf(timeoutLimitProperty);
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
                double maxFilterRatio = Double.valueOf(maxFilterRadioProperty);
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
                final int sendBatchParallelismValue = Integer.valueOf(sendBatchParallelism);
                if (sendBatchParallelismValue < 1) {
                    throw new DdlException(Properties.SEND_BATCH_PARALLELISM + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(Properties.SEND_BATCH_PARALLELISM + " is not a number.");
            }
        }
    }


    /**
     * TODO: unify the data_desc
     * the unique entrance for data_desc
     */
    interface DataDesc {

        String toSql();

    }
}
