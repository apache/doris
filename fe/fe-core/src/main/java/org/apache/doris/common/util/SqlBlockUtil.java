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

package org.apache.doris.common.util;

import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

import org.apache.commons.lang3.StringUtils;

public class SqlBlockUtil {

    public static final String STRING_DEFAULT = "NULL";
    public static final String LONG_DEFAULT = "0";
    public static final Long LONG_ZERO = 0L;
    public static final Long LONG_MINUS_ONE = -1L;


    public static void checkSqlAndSqlHashSetBoth(String sql, String sqlHash) throws AnalysisException {
        if (isSqlConditionConfigured(sql) && isSqlConditionConfigured(sqlHash)) {
            throw new AnalysisException("Only sql or sqlHash can be configured");
        }
    }

    // check (sql or sqlHash) and (limitations: partitioNum, tabletNum, cardinality) are not set both
    public static void checkSqlAndLimitationsSetBoth(String sql, String sqlHash,
            String partitionNumString, String tabletNumString, String cardinalityString,
            boolean requirePartitionFilter) throws AnalysisException {
        if (hasSqlCondition(sql, sqlHash)
                && hasScanCondition(partitionNumString, tabletNumString, cardinalityString, requirePartitionFilter)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE);
        }
    }

    // 1. check (sql or sqlHash) and (limitations: partitioNum, tabletNum, cardinality) are not set both
    // 2. check any of limitations is set while sql or sqlHash is not set
    public static void checkPropertiesValidate(String sql, String sqlHash,
            String partitionNumString, String tabletNumString, String cardinalityString,
            boolean requirePartitionFilter)  throws AnalysisException {
        boolean hasSqlCondition = hasSqlCondition(sql, sqlHash);
        boolean hasScanCondition = hasScanCondition(partitionNumString, tabletNumString,
                cardinalityString, requirePartitionFilter);
        if ((hasSqlCondition && hasScanCondition) || (!hasSqlCondition && !hasScanCondition)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE);
        }
    }

    // check at least one of the (limitations: partitioNum, tabletNum, cardinality) is not empty
    public static Boolean isSqlBlockLimitationsEmpty(String partitionNumString,
            String tabletNumString, String cardinalityString, boolean requirePartitionFilter) {
        return StringUtils.isEmpty(partitionNumString)
                && StringUtils.isEmpty(tabletNumString)
                && StringUtils.isEmpty(cardinalityString)
                && !requirePartitionFilter;
    }

    public static Boolean isSqlBlockLimitationsDefault(Long partitionNum, Long tabletNum, Long cardinality,
            Boolean requirePartitionFilter) {
        return partitionNum == LONG_ZERO && tabletNum == LONG_ZERO && cardinality == LONG_ZERO
                && !Boolean.TRUE.equals(requirePartitionFilter);
    }

    public static Boolean isSqlBlockLimitationsNull(Long partitionNum, Long tabletNum, Long cardinality,
            Boolean requirePartitionFilter) {
        return partitionNum == null && tabletNum == null && cardinality == null && requirePartitionFilter == null;
    }

    // alter operation not allowed to change other properties that not set
    public static void checkAlterValidate(SqlBlockRule sqlBlockRule) throws AnalysisException {
        checkSqlAndSqlHashSetBoth(sqlBlockRule.getSql(), sqlBlockRule.getSqlHash());
        boolean hasSqlCondition = hasSqlCondition(sqlBlockRule.getSql(), sqlBlockRule.getSqlHash());
        boolean hasScanCondition = hasScanCondition(sqlBlockRule.getPartitionNum(), sqlBlockRule.getTabletNum(),
                sqlBlockRule.getCardinality(), sqlBlockRule.getRequirePartitionFilter());
        if ((hasSqlCondition && hasScanCondition) || (!hasSqlCondition && !hasScanCondition)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE);
        }
    }

    private static boolean hasSqlCondition(String sql, String sqlHash) {
        return isSqlConditionConfigured(sql) || isSqlConditionConfigured(sqlHash);
    }

    private static boolean hasScanCondition(String partitionNumString, String tabletNumString, String cardinalityString,
            boolean requirePartitionFilter) {
        return !isSqlBlockLimitationsEmpty(partitionNumString, tabletNumString, cardinalityString,
                requirePartitionFilter);
    }

    private static boolean hasScanCondition(Long partitionNum, Long tabletNum, Long cardinality,
            Boolean requirePartitionFilter) {
        return !isSqlBlockLimitationsDefault(partitionNum, tabletNum, cardinality, requirePartitionFilter)
                && !isSqlBlockLimitationsNull(partitionNum, tabletNum, cardinality, requirePartitionFilter);
    }

    private static boolean isSqlConditionConfigured(String value) {
        return StringUtils.isNotEmpty(value) && !STRING_DEFAULT.equals(value);
    }

}
