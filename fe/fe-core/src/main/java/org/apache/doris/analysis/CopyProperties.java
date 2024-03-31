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

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class CopyProperties {
    protected Map<String, String> properties;
    protected String prefix;

    private static final String FILE_PREFIX = "file.";
    // properties for type, compression, column_separator
    public static final String TYPE = FILE_PREFIX + "type";
    public static final String COMPRESSION = FILE_PREFIX + "compression";
    public static final String COLUMN_SEPARATOR = FILE_PREFIX + LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR;
    // properties for data desc
    public static final String LINE_DELIMITER = FILE_PREFIX + LoadStmt.KEY_IN_PARAM_LINE_DELIMITER;
    public static final String PARAM_STRIP_OUTER_ARRAY = FILE_PREFIX + LoadStmt.KEY_IN_PARAM_STRIP_OUTER_ARRAY;
    public static final String PARAM_FUZZY_PARSE = FILE_PREFIX + LoadStmt.KEY_IN_PARAM_FUZZY_PARSE;
    public static final String PARAM_NUM_AS_STRING = FILE_PREFIX + LoadStmt.KEY_IN_PARAM_NUM_AS_STRING;
    public static final String PARAM_JSONPATHS = FILE_PREFIX + LoadStmt.KEY_IN_PARAM_JSONPATHS;
    public static final String PARAM_JSONROOT = FILE_PREFIX + LoadStmt.KEY_IN_PARAM_JSONROOT;

    public static final String COPY_PREFIX = "copy.";
    // property for size limit, async, on_error
    public static final String SIZE_LIMIT = COPY_PREFIX + "size_limit";
    public static final String ASYNC = COPY_PREFIX + "async";
    public static final String ON_ERROR = COPY_PREFIX + "on_error";
    public static final String ON_ERROR_CONTINUE = "continue";
    public static final String ON_ERROR_ABORT_STATEMENT = "abort_statement";
    public static final String ON_ERROR_MAX_FILTER_RATIO = LoadStmt.MAX_FILTER_RATIO_PROPERTY + "_";
    public static final String STRICT_MODE = COPY_PREFIX + LoadStmt.STRICT_MODE;
    public static final String LOAD_PARALLELISM = COPY_PREFIX + LoadStmt.LOAD_PARALLELISM;
    // If 'copy.force' is true, load files to table without checking if files have been loaded, and copy job will not
    // be recorded in meta service. So it may cause one file is copied to a table many times.
    public static final String FORCE = COPY_PREFIX + "force";
    public static final String USE_DELETE_SIGN = COPY_PREFIX + "use_delete_sign";

    public CopyProperties(Map<String, String> properties, String prefix) {
        this.properties = properties;
        this.prefix = prefix;
    }

    protected void analyzeTypeAndCompression() throws AnalysisException {
        // analyze type and compression: See {@link BrokerScanNode#formatType}, we only support COMPRESSION on CSV
        String compression = properties.get(addKeyPrefix(COMPRESSION));
        String type = properties.get(addKeyPrefix(TYPE));
        if (!StringUtils.isEmpty(compression) && !isTypeEmpty(type) && !(type.equalsIgnoreCase("csv")
                || type.equalsIgnoreCase("json"))) {
            throw new AnalysisException("Compression only support CSV or JSON file type, but input type is " + type);
        }
    }

    protected void analyzeSizeLimit() throws AnalysisException {
        String key = addKeyPrefix(SIZE_LIMIT);
        if (properties.containsKey(key)) {
            String value = properties.get(key);
            try {
                Long.parseLong(value);
            } catch (Exception e) {
                throw new AnalysisException("Property " + key + " with invalid value " + value);
            }
        }
    }

    protected void analyzeLoadParallelism() throws AnalysisException {
        String key = addKeyPrefix(LOAD_PARALLELISM);
        if (properties.containsKey(key)) {
            String value = properties.get(key);
            try {
                Integer.parseInt(value);
            } catch (Exception e) {
                throw new AnalysisException("Property " + key + " with invalid value " + value);
            }
        }
    }

    protected void analyzeOnError() throws AnalysisException {
        String key = addKeyPrefix(ON_ERROR);
        if (properties.containsKey(key)) {
            String value = properties.get(key);
            try {
                if (value.startsWith(ON_ERROR_MAX_FILTER_RATIO)) {
                    double maxFilterRatio = getMaxFilterRatio();
                    if (maxFilterRatio < 0 || maxFilterRatio > 1) {
                        throw new AnalysisException("max_filter_ratio must in [0, 1]");
                    }
                } else if (!value.equalsIgnoreCase(ON_ERROR_CONTINUE) && !value.equalsIgnoreCase(
                        ON_ERROR_ABORT_STATEMENT)) {
                    throw new AnalysisException("Property " + key + " with invalid value " + value);
                }
            } catch (Exception e) {
                throw new AnalysisException("Property " + key + " with invalid value " + value);
            }
        }
    }

    protected void analyzeAsync() throws AnalysisException {
        analyzeBooleanProperty(ASYNC);
    }

    protected void analyzeStrictMode() throws AnalysisException {
        analyzeBooleanProperty(STRICT_MODE);
    }

    protected void analyzeForce() throws AnalysisException {
        analyzeBooleanProperty(FORCE);
    }

    protected void analyzeUseDeleteSign() throws AnalysisException {
        analyzeBooleanProperty(USE_DELETE_SIGN);
    }

    protected void analyzeBooleanProperty(String keyWithoutPrefix) throws AnalysisException {
        String key = addKeyPrefix(keyWithoutPrefix);
        if (properties.containsKey(key)) {
            String value = properties.get(key);
            if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                throw new AnalysisException("Property " + key + " with invalid value " + value);
            }
        }
    }

    /**
     * @return the size limit, note that 0 means no limit
     */
    public long getSizeLimit() {
        String key = addKeyPrefix(SIZE_LIMIT);
        if (properties.containsKey(key)) {
            return Long.parseLong(properties.get(key));
        }
        return 0;
    }

    public double getMaxFilterRatio() {
        String key = addKeyPrefix(ON_ERROR);
        if (properties.containsKey(key)) {
            String value = properties.get(key);
            if (value.startsWith(ON_ERROR_MAX_FILTER_RATIO)) {
                return Double.parseDouble(value.substring(ON_ERROR_MAX_FILTER_RATIO.length()));
            } else if (value.equalsIgnoreCase(ON_ERROR_CONTINUE)) {
                return 1;
            } else {
                return 0;
            }
        }
        return 0;
    }

    public String getFileType() {
        // Use ExternalFileScanNode instead of BrokerScanNode, see {@link LoadScanProvider#formatType}
        // if file format type is set on stage, and we want to override by copy into, can set null
        String type = properties.get(addKeyPrefix(TYPE));
        return isTypeEmpty(type) ? null : type;
    }

    public String getFileTypeIgnoreCompression() {
        return properties.get(addKeyPrefix(TYPE));
    }

    public String getCompression() {
        return properties.get(addKeyPrefix(COMPRESSION));
    }

    public String getColumnSeparator() {
        return properties.get(addKeyPrefix(COLUMN_SEPARATOR));
    }

    public boolean isAsync() {
        String key = addKeyPrefix(ASYNC);
        return properties.containsKey(key) ? Boolean.parseBoolean(properties.get(key)) : true;
    }

    public boolean isForce() {
        String key = addKeyPrefix(FORCE);
        return properties.containsKey(key) ? Boolean.parseBoolean(properties.get(key)) : false;
    }

    public boolean useDeleteSign() {
        String key = addKeyPrefix(USE_DELETE_SIGN);
        return properties.containsKey(key) ? Boolean.parseBoolean(properties.get(key)) : false;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(new PrintableMap<>(properties, "=", true, false)).append(") ");
        return sb.toString();
    }

    private boolean isTypeEmpty(String type) {
        return StringUtils.isEmpty(type) || type.equalsIgnoreCase("null");
    }

    protected String addKeyPrefix(String key) {
        return prefix + key;
    }

    protected String removeFilePrefix(String key) {
        if (key.startsWith(FILE_PREFIX)) {
            return key.substring(FILE_PREFIX.length());
        } else if (key.startsWith(COPY_PREFIX)) {
            return key.substring(COPY_PREFIX.length());
        }
        return key;
    }
}
