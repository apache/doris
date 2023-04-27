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
        if (!STRING_DEFAULT.equals(sql) && !STRING_DEFAULT.equals(sqlHash)) {
            throw new AnalysisException("Only sql or sqlHash can be configured");
        }
    }

    // check (sql or sqlHash) and (limitations: partitioNum, tabletNum, cardinality) are not set both
    public static void checkSqlAndLimitationsSetBoth(String sql, String sqlHash,
            String partitionNumString, String tabletNumString, String cardinalityString) throws AnalysisException {
        if ((!STRING_DEFAULT.equals(sql) || !STRING_DEFAULT.equals(sqlHash))
                && !isSqlBlockLimitationsEmpty(partitionNumString, tabletNumString, cardinalityString)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE);
        }
    }

    // 1. check (sql or sqlHash) and (limitations: partitioNum, tabletNum, cardinality) are not set both
    // 2. check any of limitations is set while sql or sqlHash is not set
    public static void checkPropertiesValidate(String sql, String sqlHash,
            String partitionNumString, String tabletNumString, String cardinalityString)  throws AnalysisException {
        if (((!STRING_DEFAULT.equals(sql) || !STRING_DEFAULT.equals(sqlHash))
                && !isSqlBlockLimitationsEmpty(partitionNumString, tabletNumString, cardinalityString))
                || ((STRING_DEFAULT.equals(sql) && STRING_DEFAULT.equals(sqlHash))
                && isSqlBlockLimitationsEmpty(partitionNumString, tabletNumString, cardinalityString))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE);
        }
    }

    // check at least one of the (limitations: partitioNum, tabletNum, cardinality) is not empty
    public static Boolean isSqlBlockLimitationsEmpty(String partitionNumString,
            String tabletNumString, String cardinalityString) {
        return StringUtils.isEmpty(partitionNumString)
                && StringUtils.isEmpty(tabletNumString) && StringUtils.isEmpty(cardinalityString);
    }

    public static Boolean isSqlBlockLimitationsDefault(Long partitionNum, Long tabletNum, Long cardinality) {
        return partitionNum == LONG_ZERO && tabletNum == LONG_ZERO && cardinality == LONG_ZERO;
    }

    public static Boolean isSqlBlockLimitationsNull(Long partitionNum, Long tabletNum, Long cardinality) {
        return partitionNum == null && tabletNum == null && cardinality == null;
    }

    // alter operation not allowed to change other properties that not set
    public static void checkAlterValidate(SqlBlockRule sqlBlockRule) throws AnalysisException {
        if (!STRING_DEFAULT.equals(sqlBlockRule.getSql())) {
            if (!STRING_DEFAULT.equals(sqlBlockRule.getSqlHash())
                    && StringUtils.isNotEmpty(sqlBlockRule.getSqlHash())) {
                throw new AnalysisException("Only sql or sqlHash can be configured");
            } else if (!isSqlBlockLimitationsDefault(sqlBlockRule.getPartitionNum(),
                    sqlBlockRule.getTabletNum(), sqlBlockRule.getCardinality())
                    && !isSqlBlockLimitationsNull(sqlBlockRule.getPartitionNum(),
                    sqlBlockRule.getTabletNum(), sqlBlockRule.getCardinality())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE);
            }
        } else if (!STRING_DEFAULT.equals(sqlBlockRule.getSqlHash())) {
            if (!STRING_DEFAULT.equals(sqlBlockRule.getSql()) && StringUtils.isNotEmpty(sqlBlockRule.getSql())) {
                throw new AnalysisException("Only sql or sqlHash can be configured");
            } else if (!isSqlBlockLimitationsDefault(sqlBlockRule.getPartitionNum(),
                    sqlBlockRule.getTabletNum(), sqlBlockRule.getCardinality())
                    && !isSqlBlockLimitationsNull(sqlBlockRule.getPartitionNum(),
                    sqlBlockRule.getTabletNum(), sqlBlockRule.getCardinality())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE);
            }
        } else if (!isSqlBlockLimitationsDefault(sqlBlockRule.getPartitionNum(),
                sqlBlockRule.getTabletNum(), sqlBlockRule.getCardinality())) {
            if (!STRING_DEFAULT.equals(sqlBlockRule.getSql()) || !STRING_DEFAULT.equals(sqlBlockRule.getSqlHash())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE);
            }
        }
    }

}
