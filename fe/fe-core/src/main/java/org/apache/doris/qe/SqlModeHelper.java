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

package org.apache.doris.qe;


import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlModeHelper {
    private static final Logger LOG = LogManager.getLogger(SqlModeHelper.class);

    // TODO(xuyang): these mode types are copy from MYSQL mode types, which are example
    //  of how they works and to be compatible with MySQL, so for now they are not
    //  really meaningful.
    /* Bits for different SQL MODE modes, you can add custom SQL MODE here */
    /* When a new session is created, its sql mode is set to MODE_DEFAULT */
    public static final long MODE_DEFAULT = 1L;
    public static final long MODE_PIPES_AS_CONCAT = 2L;
    public static final long MODE_ANSI_QUOTES = 4L;
    public static final long MODE_IGNORE_SPACE = 8L;
    public static final long MODE_NOT_USED = 16L;
    public static final long MODE_ONLY_FULL_GROUP_BY = 32L;
    public static final long MODE_NO_UNSIGNED_SUBTRACTION = 64L;
    public static final long MODE_NO_DIR_IN_CREATE = 128L;
    public static final long MODE_NO_AUTO_VALUE_ON_ZERO = 1L << 19;
    public static final long MODE_NO_BACKSLASH_ESCAPES = 1L << 20;
    public static final long MODE_STRICT_TRANS_TABLES = 1L << 21;
    public static final long MODE_STRICT_ALL_TABLES = 1L << 22;
    // NO_ZERO_IN_DATE and NO_ZERO_DATE are removed in mysql 5.7 and merged into STRICT MODE.
    // However, for backward compatibility during upgrade, these modes are kept.
    @Deprecated
    public static final long MODE_NO_ZERO_IN_DATE = 1L << 23;
    @Deprecated
    public static final long MODE_NO_ZERO_DATE = 1L << 24;
    public static final long MODE_INVALID_DATES = 1L << 25;
    public static final long MODE_ERROR_FOR_DIVISION_BY_ZERO = 1L << 26;
    public static final long MODE_HIGH_NOT_PRECEDENCE = 1L << 29;
    public static final long MODE_NO_ENGINE_SUBSTITUTION = 1L << 30;
    public static final long MODE_PAD_CHAR_TO_FULL_LENGTH = 1L << 31;
    public static final long MODE_TIME_TRUNCATE_FRACTIONAL = 1L << 32;

    /* Bits for different COMBINE MODE modes, you can add custom COMBINE MODE here */
    public static final long MODE_ANSI = 1L << 18;
    public static final long MODE_TRADITIONAL = 1L << 27;

    public static final long MODE_LAST = 1L << 33;
    public static final long MODE_REAL_AS_FLOAT = 1L << 34;


    public static final long MODE_ALLOWED_MASK =
            (MODE_REAL_AS_FLOAT | MODE_PIPES_AS_CONCAT | MODE_ANSI_QUOTES | MODE_IGNORE_SPACE | MODE_NOT_USED
                    | MODE_ONLY_FULL_GROUP_BY | MODE_NO_UNSIGNED_SUBTRACTION | MODE_NO_DIR_IN_CREATE
                    | MODE_NO_AUTO_VALUE_ON_ZERO | MODE_NO_BACKSLASH_ESCAPES | MODE_STRICT_TRANS_TABLES
                    | MODE_STRICT_ALL_TABLES | MODE_NO_ZERO_IN_DATE | MODE_NO_ZERO_DATE | MODE_INVALID_DATES
                    | MODE_ERROR_FOR_DIVISION_BY_ZERO | MODE_HIGH_NOT_PRECEDENCE | MODE_NO_ENGINE_SUBSTITUTION
                    | MODE_PAD_CHAR_TO_FULL_LENGTH | MODE_TRADITIONAL | MODE_ANSI | MODE_TIME_TRUNCATE_FRACTIONAL
                    | MODE_DEFAULT);

    public static final long MODE_COMBINE_MASK = (MODE_ANSI | MODE_TRADITIONAL);

    private static final Map<String, Long> sqlModeSet = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    private static final Map<String, Long> combineModeSet = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    static {
        sqlModeSet.put("DEFAULT", MODE_DEFAULT);
        sqlModeSet.put("REAL_AS_FLOAT", MODE_REAL_AS_FLOAT);
        sqlModeSet.put("PIPES_AS_CONCAT", MODE_PIPES_AS_CONCAT);
        sqlModeSet.put("ANSI_QUOTES", MODE_ANSI_QUOTES);
        sqlModeSet.put("IGNORE_SPACE", MODE_IGNORE_SPACE);
        sqlModeSet.put("NOT_USED", MODE_NOT_USED);
        sqlModeSet.put("ONLY_FULL_GROUP_BY", MODE_ONLY_FULL_GROUP_BY);
        sqlModeSet.put("NO_UNSIGNED_SUBTRACTION", MODE_NO_UNSIGNED_SUBTRACTION);
        sqlModeSet.put("NO_DIR_IN_CREATE", MODE_NO_DIR_IN_CREATE);
        sqlModeSet.put("ANSI", MODE_ANSI);
        sqlModeSet.put("NO_AUTO_VALUE_ON_ZERO", MODE_NO_AUTO_VALUE_ON_ZERO);
        sqlModeSet.put("NO_BACKSLASH_ESCAPES", MODE_NO_BACKSLASH_ESCAPES);
        sqlModeSet.put("STRICT_TRANS_TABLES", MODE_STRICT_TRANS_TABLES);
        sqlModeSet.put("STRICT_ALL_TABLES", MODE_STRICT_ALL_TABLES);
        sqlModeSet.put("NO_ZERO_IN_DATE", MODE_NO_ZERO_IN_DATE);
        sqlModeSet.put("NO_ZERO_DATE", MODE_NO_ZERO_DATE);
        sqlModeSet.put("INVALID_DATES", MODE_INVALID_DATES);
        sqlModeSet.put("ERROR_FOR_DIVISION_BY_ZERO", MODE_ERROR_FOR_DIVISION_BY_ZERO);
        sqlModeSet.put("TRADITIONAL", MODE_TRADITIONAL);
        sqlModeSet.put("HIGH_NOT_PRECEDENCE", MODE_HIGH_NOT_PRECEDENCE);
        sqlModeSet.put("NO_ENGINE_SUBSTITUTION", MODE_NO_ENGINE_SUBSTITUTION);
        sqlModeSet.put("PAD_CHAR_TO_FULL_LENGTH", MODE_PAD_CHAR_TO_FULL_LENGTH);
        sqlModeSet.put("TIME_TRUNCATE_FRACTIONAL", MODE_TIME_TRUNCATE_FRACTIONAL);

        combineModeSet.put("ANSI", (MODE_REAL_AS_FLOAT | MODE_PIPES_AS_CONCAT
                | MODE_ANSI_QUOTES | MODE_IGNORE_SPACE | MODE_ONLY_FULL_GROUP_BY));
        combineModeSet.put("TRADITIONAL", (MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES
                | MODE_NO_ZERO_IN_DATE | MODE_NO_ZERO_DATE | MODE_ERROR_FOR_DIVISION_BY_ZERO
                | MODE_NO_ENGINE_SUBSTITUTION));
    }

    // convert long type SQL MODE to string type that user can read
    public static String decode(Long sqlMode) throws DdlException {
        if (sqlMode == MODE_DEFAULT) {
            //For compatibility with older versions， return empty string
            return "";
        }
        if ((sqlMode & ~MODE_ALLOWED_MASK) != 0) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.SQL_MODE, sqlMode);
        }

        List<String> names = new ArrayList<String>();
        for (Map.Entry<String, Long> mode : getSupportedSqlMode().entrySet()) {
            if ((sqlMode & mode.getValue()) != 0) {
                names.add(mode.getKey());
            }
        }

        return Joiner.on(',').join(names);
    }

    // convert string type SQL MODE to long type that session can store
    public static Long encode(String sqlMode) throws DdlException {
        List<String> names =
                Splitter.on(',').trimResults().omitEmptyStrings().splitToList(sqlMode);

        // empty string parse to 0
        long resultCode = 0L;
        for (String key : names) {
            long code = 0L;
            if (StringUtils.isNumeric(key)) {
                code |= expand(Long.valueOf(key));
            } else {
                code = getCodeFromString(key);
                if (code == 0) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.SQL_MODE, key);
                }
            }
            resultCode |= code;
            if ((resultCode & ~MODE_ALLOWED_MASK) != 0) {
                ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, SessionVariable.SQL_MODE, key);
            }
        }
        return resultCode;
    }

    // expand the combine mode if exists
    public static long expand(long sqlMode) throws DdlException {
        for (String key : getCombineMode().keySet()) {
            if ((sqlMode & getSupportedSqlMode().get(key)) != 0) {
                sqlMode |= getCombineMode().get(key);
            }
        }
        return sqlMode;
    }

    // check if this SQL MODE is supported
    public static boolean isSupportedSqlMode(String sqlMode) {
        if (sqlMode == null || !getSupportedSqlMode().containsKey(sqlMode)) {
            return false;
        }
        return true;
    }

    // encode sqlMode from string to long
    private static long getCodeFromString(String sqlMode) {
        long code = 0L;
        if (isSupportedSqlMode(sqlMode)) {
            if (isCombineMode(sqlMode)) {
                code |= getCombineMode().get(sqlMode);
            }
            code |= getSupportedSqlMode().get(sqlMode);
        }
        return code;
    }

    // check if this SQL MODE is combine mode
    public static boolean isCombineMode(String key) {
        return combineModeSet.containsKey(key);
    }

    public static Map<String, Long> getSupportedSqlMode() {
        return sqlModeSet;
    }

    public static Map<String, Long> getCombineMode() {
        return combineModeSet;
    }

    public static boolean hasNoBackSlashEscapes() {
        SessionVariable sessionVariable = ConnectContext.get() == null
                ? VariableMgr.newSessionVariable()
                : ConnectContext.get().getSessionVariable();
        return ((sessionVariable.getSqlMode() & MODE_ALLOWED_MASK)
                & MODE_NO_BACKSLASH_ESCAPES) != 0;
    }

    public static boolean hasPipeAsConcat() {
        SessionVariable sessionVariable = ConnectContext.get() == null
                ? VariableMgr.newSessionVariable()
                : ConnectContext.get().getSessionVariable();
        return ((sessionVariable.getSqlMode() & MODE_ALLOWED_MASK)
                & MODE_PIPES_AS_CONCAT) != 0;
    }

}
