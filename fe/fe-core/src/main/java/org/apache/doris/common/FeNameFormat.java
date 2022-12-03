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

package org.apache.doris.common;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PaloRole;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Strings;

public class FeNameFormat {
    private static final String LABEL_REGEX = "^[-_A-Za-z0-9]{1,128}$";
    private static final String COMMON_NAME_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$";
    private static final String TABLE_NAME_REGEX = "^[a-zA-Z][a-zA-Z0-9_]*$";
    private static final String COLUMN_NAME_REGEX = "^[_a-zA-Z@0-9][a-zA-Z0-9_]{0,255}$";

    private static final String UNICODE_LABEL_REGEX = "^[-_A-Za-z0-9\\p{L}]{1,128}$";
    private static final String UNICODE_COMMON_NAME_REGEX = "^[a-zA-Z\\p{L}][a-zA-Z0-9_\\p{L}]{0,63}$";
    private static final String UNICODE_TABLE_NAME_REGEX = "^[a-zA-Z\\p{L}][a-zA-Z0-9_\\p{L}]*$";
    private static final String UNICODE_COLUMN_NAME_REGEX = "^[_a-zA-Z@0-9\\p{L}][a-zA-Z0-9_\\p{L}]{0,255}$";

    public static final String FORBIDDEN_PARTITION_NAME = "placeholder_";

    public static void checkCatalogName(String catalogName) throws AnalysisException {
        if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalogName) && (Strings.isNullOrEmpty(catalogName)
                || !catalogName.matches(getCommonNameRegex()))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_CATALOG_NAME, catalogName);
        }
    }

    public static void checkDbName(String dbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName) || !dbName.matches(getCommonNameRegex())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }
    }

    public static void checkTableName(String tableName) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName)
                || !tableName.matches(getTableNameRegex())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName,
                    getTableNameRegex());
        }
        if (tableName.length() > Config.table_name_length_limit) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_NAME_LENGTH_LIMIT, tableName,
                    tableName.length(), Config.table_name_length_limit);
        }
    }

    public static void checkPartitionName(String partitionName) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionName) || !partitionName.matches(getCommonNameRegex())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PARTITION_NAME, partitionName);
        }

        if (partitionName.startsWith(FORBIDDEN_PARTITION_NAME)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PARTITION_NAME, partitionName);
        }
    }

    public static void checkColumnName(String columnName) throws AnalysisException {
        if (Strings.isNullOrEmpty(columnName) || !columnName.matches(getColumnNameRegex())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                    columnName, getColumnNameRegex());
        }
        if (columnName.startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                    columnName, getColumnNameRegex());
        }
        if (columnName.startsWith(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                    columnName, FeNameFormat.COLUMN_NAME_REGEX);
        }
    }

    public static void checkLabel(String label) throws AnalysisException {
        if (Strings.isNullOrEmpty(label) || !label.matches(getLabelRegex())) {
            throw new AnalysisException("Label format error. regex: " + getLabelRegex() + ", label: " + label);
        }
    }

    public static void checkUserName(String userName) throws AnalysisException {
        if (Strings.isNullOrEmpty(userName) || !userName.matches(getCommonNameRegex())) {
            throw new AnalysisException("invalid user name: " + userName);
        }
    }

    public static void checkRoleName(String role, boolean canBeAdmin, String errMsg) throws AnalysisException {
        if (Strings.isNullOrEmpty(role) || !role.matches(getCommonNameRegex())) {
            throw new AnalysisException("invalid role format: " + role);
        }

        boolean res = false;
        if (CaseSensibility.ROLE.getCaseSensibility()) {
            res = role.equals(PaloRole.OPERATOR_ROLE) || (!canBeAdmin && role.equals(PaloRole.ADMIN_ROLE));
        } else {
            res = role.equalsIgnoreCase(PaloRole.OPERATOR_ROLE)
                    || (!canBeAdmin && role.equalsIgnoreCase(PaloRole.ADMIN_ROLE));
        }

        if (res) {
            throw new AnalysisException(errMsg + ": " + role);
        }
    }

    public static void checkResourceName(String resourceName) throws AnalysisException {
        checkCommonName("resource", resourceName);
    }

    public static void checkCommonName(String type, String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name) || !name.matches(getCommonNameRegex())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FORMAT, type, name);
        }
    }

    private static boolean isEnableUnicodeNameSupport() {
        boolean isEnable;
        if (ConnectContext.get() != null) {
            isEnable = ConnectContext.get().getSessionVariable().isEnableUnicodeNameSupport();
        } else { // for unittest to use default variable
            isEnable = VariableMgr.getDefaultSessionVariable().isEnableUnicodeNameSupport();
        }
        return isEnable;
    }

    public static String getColumnNameRegex() {
        if (FeNameFormat.isEnableUnicodeNameSupport()) {
            return UNICODE_COLUMN_NAME_REGEX;
        } else {
            return COLUMN_NAME_REGEX;
        }
    }

    public static String getTableNameRegex() {
        if (FeNameFormat.isEnableUnicodeNameSupport()) {
            return UNICODE_TABLE_NAME_REGEX;
        } else {
            return TABLE_NAME_REGEX;
        }
    }

    public static String getLabelRegex() {
        if (FeNameFormat.isEnableUnicodeNameSupport()) {
            return UNICODE_LABEL_REGEX;
        } else {
            return LABEL_REGEX;
        }
    }

    public static String getCommonNameRegex() {
        if (FeNameFormat.isEnableUnicodeNameSupport()) {
            return UNICODE_COMMON_NAME_REGEX;
        } else {
            return COMMON_NAME_REGEX;
        }
    }
}
