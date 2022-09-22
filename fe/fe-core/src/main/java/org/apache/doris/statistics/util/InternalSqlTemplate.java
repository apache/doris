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

package org.apache.doris.statistics.util;

import org.apache.doris.common.InvalidFormatException;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Template for building internal query SQL statements.
 * e.g.:
 *   - "SELECT ${column} FROM ${table}"
 *   - ${column} and ${table} will be replaced with the actual executed table and column.
 */
public class InternalSqlTemplate {
    /** common parameters: tableName, columnName, partitionName */
    public static final String TABLE = "table";
    public static final String PARTITION = "partition";
    public static final String COLUMN = "column";

    /** -------------------------- for statistics begin -------------------------- */
    public static final String MIN_VALUE_SQL = "SELECT MIN(${column}) AS min_value FROM ${table};";
    public static final String PARTITION_MIN_VALUE_SQL = "SELECT MIN(${column}) AS min_value"
            + " FROM ${table} PARTITION (${partition});";

    public static final String MAX_VALUE_SQL = "SELECT MAX(${column}) AS max_value FROM ${table};";
    public static final String PARTITION_MAX_VALUE_SQL = "SELECT MAX(${column}) AS max_value FROM"
            + " ${table} PARTITION (${partition});";

    public static final String NDV_VALUE_SQL = "SELECT NDV(${column}) AS ndv FROM ${table};";
    public static final String PARTITION_NDV_VALUE_SQL = "SELECT NDV(${column}) AS ndv FROM"
            + " ${table} PARTITION (${partition});";

    public static final String MIN_MAX_NDV_VALUE_SQL = "SELECT MIN(${column}) AS min_value, MAX(${column})"
            + " AS max_value, NDV(${column}) AS ndv FROM ${table};";
    public static final String PARTITION_MIN_MAX_NDV_VALUE_SQL = "SELECT MIN(${column}) AS min_value,"
            + " MAX(${column}) AS max_value, NDV(${column}) AS ndv FROM ${table} PARTITION (${partition});";

    public static final String ROW_COUNT_SQL = "SELECT COUNT(1) AS row_count FROM ${table};";
    public static final String PARTITION_ROW_COUNT_SQL = "SELECT COUNT(1) AS row_count FROM ${table} PARTITION"
            + " (${partition});";

    public static final String MAX_SIZE_SQL = "SELECT MAX(LENGTH(${column})) AS max_size FROM ${table};";
    public static final String PARTITION_MAX_SIZE_SQL = "SELECT MAX(LENGTH(${column})) AS max_size FROM"
            + " ${table} PARTITION (${partition});";

    public static final String AVG_SIZE_SQL = "SELECT AVG(LENGTH(${column})) AS avg_size FROM ${table};";
    public static final String PARTITION_AVG_SIZE_SQL = "SELECT AVG(LENGTH(${column})) AS avg_size"
            + " FROM ${table} PARTITION (${partition});";

    public static final String MAX_AVG_SIZE_SQL = "SELECT MAX(LENGTH(${column})) AS max_size,"
            + " AVG(LENGTH(${column})) AS avg_size FROM ${table};";
    public static final String PARTITION_MAX_AVG_SIZE_SQL = "SELECT MAX(LENGTH(${column}))"
            + " AS max_size, AVG(LENGTH(${column})) AS avg_size FROM ${table} PARTITION (${partition});";

    public static final String NUM_NULLS_SQL = "SELECT COUNT(1) AS num_nulls FROM ${table}"
            + " WHERE ${column} IS NULL;";
    public static final String PARTITION_NUM_NULLS_SQL = "SELECT COUNT(1) AS num_nulls FROM"
            + " ${table} PARTITION (${partition}) WHERE ${column} IS NULL;";
    /** ---------------------------- for statistics end ---------------------------- */

    private static final Logger LOG = LogManager.getLogger(InternalSqlTemplate.class);

    private static final Pattern PATTERN = Pattern.compile("\\$\\{\\w+\\}");

    /**
     * Concatenate SQL statements based on templates and parameters. e.g.:
     * template and parameters:
     *  'SELECT ${col} FROM ${table} WHERE id = ${id};',
     *   parameters: {col=colName, table=tableName, id=1}
     *   result sql: 'SELECT colName FROM tableName WHERE id = 1;
     * <p>
     *
     * @param template sql template
     * @param params   k,v parameter, if without parameter, params should be null
     * @return SQL statement with parameters concatenated
     */
    public static String processTemplate(String template, Map<String, String> params) {
        Matcher matcher = PATTERN.matcher(template);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String param = matcher.group();
            String value = params.get(param.substring(2, param.length() - 1));
            matcher.appendReplacement(sb, value == null ? "" : value);
        }

        matcher.appendTail(sb);
        LOG.debug("Template:{}, params: {}, SQL: {}", template, params, sb.toString());

        return sb.toString();
    }

    public static String buildStatsMinValueSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(MIN_VALUE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(MIN_VALUE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionMinValueSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_MIN_VALUE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_MIN_VALUE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsMaxValueSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(MAX_VALUE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(MAX_VALUE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionMaxValueSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_MAX_VALUE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_MAX_VALUE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsNdvValueSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(NDV_VALUE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(NDV_VALUE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionNdvValueSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_NDV_VALUE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_NDV_VALUE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsMinMaxNdvValueSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(MIN_MAX_NDV_VALUE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(MIN_MAX_NDV_VALUE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionMinMaxNdvValueSql(Map<String, String> params)
            throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_MIN_MAX_NDV_VALUE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_MIN_MAX_NDV_VALUE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsRowCountSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(ROW_COUNT_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(ROW_COUNT_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionRowCountSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_ROW_COUNT_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_ROW_COUNT_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsMaxSizeSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(MAX_SIZE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(MAX_SIZE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionMaxSizeSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_MAX_SIZE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_MAX_SIZE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsAvgSizeSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(AVG_SIZE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(AVG_SIZE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionAvgSizeSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_AVG_SIZE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_AVG_SIZE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsMaxAvgSizeSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(MAX_AVG_SIZE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(MAX_AVG_SIZE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionMaxAvgSizeSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_MAX_AVG_SIZE_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_MAX_AVG_SIZE_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsNumNullsSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(NUM_NULLS_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(NUM_NULLS_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    public static String buildStatsPartitionNumNullsSql(Map<String, String> params) throws InvalidFormatException {
        Set<String> requiredParams = getTemplateParams(PARTITION_NUM_NULLS_SQL);
        if (checkParams(requiredParams, params)) {
            return processTemplate(PARTITION_NUM_NULLS_SQL, params);
        } else {
            throw new InvalidFormatException("Wrong parameter format. need params: " + requiredParams);
        }
    }

    private static Set<String> getTemplateParams(String template) {
        Matcher matcher = PATTERN.matcher(template);
        Set<String> requiredParams = Sets.newHashSet();

        while (matcher.find()) {
            String param = matcher.group();
            String value = param.substring(2, param.length() - 1);
            requiredParams.add(value);
        }

        return requiredParams;
    }

    private static boolean checkParams(Set<String> requiredParams, Map<String, String> params) {
        if (params != null && !params.isEmpty()) {
            Set<String> paramsSet = params.keySet();
            return paramsSet.containsAll(requiredParams);
        } else {
            return requiredParams == null;
        }
    }
}
