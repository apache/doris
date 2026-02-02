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

import org.apache.doris.common.Config;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.MysqlHandshakePacket;

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.util.List;

// You can place your global variable in this class with public and VariableMgr.VarAttr annotation.
// You can get this variable from MySQL client with statement `SELECT @@variable_name`,
// and change its value through `SET variable_name = xxx`
// NOTE: If you want access your variable safe, please hold VariableMgr's lock before access.
public final class GlobalVariable {

    public static final int VARIABLE_VERSION_0 = 0;
    public static final int VARIABLE_VERSION_100 = 100;
    public static final int VARIABLE_VERSION_101 = 101;
    public static final int VARIABLE_VERSION_200 = 200;
    public static final int VARIABLE_VERSION_300 = 300;
    public static final int VARIABLE_VERSION_400 = 400;
    public static final int CURRENT_VARIABLE_VERSION = VARIABLE_VERSION_400;
    public static final String VARIABLE_VERSION = "variable_version";

    public static final String VERSION_COMMENT = "version_comment";
    public static final String VERSION = "version";
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";
    public static final String LICENSE = "license";
    public static final String LANGUAGE = "language";
    public static final String INIT_CONNECT = "init_connect";
    public static final String SYSTEM_TIME_ZONE = "system_time_zone";
    public static final String QUERY_CACHE_SIZE = "query_cache_size";
    public static final String DEFAULT_ROWSET_TYPE = "default_rowset_type";
    public static final String PERFORMANCE_SCHEMA = "performance_schema";
    public static final String DEFAULT_PASSWORD_LIFETIME = "default_password_lifetime";
    public static final String PASSWORD_HISTORY = "password_history";
    public static final String VALIDATE_PASSWORD_POLICY = "validate_password_policy";
    public static final String SHOW_FULL_DBNAME_IN_INFO_SCHEMA_DB = "show_full_dbname_in_info_schema_db";

    public static final long VALIDATE_PASSWORD_POLICY_DISABLED = 0;
    public static final long VALIDATE_PASSWORD_POLICY_STRONG = 2;

    public static final String VALIDATE_PASSWORD_DICTIONARY_FILE = "validate_password_dictionary_file";

    public static final String SQL_CONVERTER_SERVICE_URL = "sql_converter_service_url";
    public static final String ENABLE_AUDIT_PLUGIN = "enable_audit_plugin";
    public static final String AUDIT_PLUGIN_MAX_BATCH_BYTES = "audit_plugin_max_batch_bytes";
    public static final String AUDIT_PLUGIN_MAX_BATCH_INTERVAL_SEC = "audit_plugin_max_batch_interval_sec";
    public static final String AUDIT_PLUGIN_MAX_SQL_LENGTH = "audit_plugin_max_sql_length";
    public static final String AUDIT_PLUGIN_MAX_INSERT_STMT_LENGTH = "audit_plugin_max_insert_stmt_length";
    public static final String AUDIT_PLUGIN_LOAD_TIMEOUT = "audit_plugin_load_timeout";

    public static final String ENABLE_GET_ROW_COUNT_FROM_FILE_LIST = "enable_get_row_count_from_file_list";
    public static final String READ_ONLY = "read_only";
    public static final String SUPER_READ_ONLY = "super_read_only";
    public static final String DEFAULT_USING_META_CACHE_FOR_EXTERNAL_CATALOG
            = "default_using_meta_cache_for_external_catalog";

    public static final String PARTITION_ANALYZE_BATCH_SIZE = "partition_analyze_batch_size";
    public static final String HUGE_PARTITION_LOWER_BOUND_ROWS = "huge_partition_lower_bound_rows";

    public static final String ENABLE_FETCH_ICEBERG_STATS = "enable_fetch_iceberg_stats";

    public static final String ENABLE_NESTED_NAMESPACE = "enable_nested_namespace";

    public static final String ENABLE_ANSI_QUERY_ORGANIZATION_BEHAVIOR
            = "enable_ansi_query_organization_behavior";
    public static final String ENABLE_NEW_TYPE_COERCION_BEHAVIOR
            = "enable_new_type_coercion_behavior";

    @VariableMgr.VarAttr(name = VARIABLE_VERSION, flag = VariableMgr.INVISIBLE
            | VariableMgr.READ_ONLY | VariableMgr.GLOBAL)
    public static int variableVersion = CURRENT_VARIABLE_VERSION;

    @VariableMgr.VarAttr(name = VERSION_COMMENT, flag = VariableMgr.READ_ONLY)
    public static String versionComment = Version.DORIS_BUILD_VERSION_PREFIX + " version "
            + Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH
            + (Config.isCloudMode() ? " (Cloud Mode)" : "");

    @VariableMgr.VarAttr(name = VERSION)
    public static String version = MysqlHandshakePacket.DEFAULT_SERVER_VERSION;

    // 0: table names are stored as specified and comparisons are case sensitive.
    // 1: table names are stored in lowercase on disk and comparisons are not case sensitive.
    // 2: table names are stored as given but compared in lowercase.
    @VariableMgr.VarAttr(name = LOWER_CASE_TABLE_NAMES, flag = VariableMgr.READ_ONLY | VariableMgr.GLOBAL)
    public static int lowerCaseTableNames = 0;

    @VariableMgr.VarAttr(name = LICENSE, flag = VariableMgr.READ_ONLY)
    public static String license = "Apache License, Version 2.0";

    @VariableMgr.VarAttr(name = LANGUAGE, flag = VariableMgr.READ_ONLY)
    public static String language = "/palo/share/english/";

    // A string to be executed by the server for each client that connects
    @VariableMgr.VarAttr(name = INIT_CONNECT, flag = VariableMgr.GLOBAL)
    public static volatile String initConnect = "";

    // A string to be executed by the server for each client that connects
    @VariableMgr.VarAttr(name = SYSTEM_TIME_ZONE, flag = VariableMgr.READ_ONLY)
    public static String systemTimeZone = TimeUtils.getSystemTimeZone().getID();

    // The amount of memory allocated for caching query results
    @VariableMgr.VarAttr(name = QUERY_CACHE_SIZE, flag = VariableMgr.GLOBAL)
    public static volatile long queryCacheSize = 1048576;

    @VariableMgr.VarAttr(name = DEFAULT_ROWSET_TYPE, flag = VariableMgr.GLOBAL)
    public static volatile String defaultRowsetType = "beta";

    // add performance schema to support MYSQL JDBC 8.0.16 or later versions.
    @VariableMgr.VarAttr(name = PERFORMANCE_SCHEMA, flag = VariableMgr.READ_ONLY)
    public static String performanceSchema = "OFF";

    @VariableMgr.VarAttr(name = DEFAULT_PASSWORD_LIFETIME, flag = VariableMgr.GLOBAL)
    public static int defaultPasswordLifetime = 0;

    @VariableMgr.VarAttr(name = PASSWORD_HISTORY, flag = VariableMgr.GLOBAL)
    public static int passwordHistory = 0;
    // 0: DISABLED
    // 2: STRONG
    @VariableMgr.VarAttr(name = VALIDATE_PASSWORD_POLICY, flag = VariableMgr.GLOBAL)
    public static long validatePasswordPolicy = 0;

    @VariableMgr.VarAttr(name = VALIDATE_PASSWORD_DICTIONARY_FILE, flag = VariableMgr.GLOBAL,
            description = {"密码验证字典文件路径。文件为纯文本格式，每行一个词。"
                    + "当 validate_password_policy 为 STRONG(2) 时，密码中不能包含字典中的任何词（不区分大小写）。"
                    + "如果为空，则使用内置字典。",
                    "Path to the password validation dictionary file. "
                            + "The file should be plain text with one word per line. "
                            + "When validate_password_policy is STRONG(2), "
                            + "the password cannot contain any word from the dictionary "
                            + "(case-insensitive). If empty, a built-in dictionary will be used."})
    public static volatile String validatePasswordDictionaryFile = "";

    // If set to true, the db name of TABLE_SCHEMA column in tables in information_schema
    // database will be shown as `ctl.db`. Otherwise, show only `db`.
    // This is used to compatible with some MySQL tools.
    @VariableMgr.VarAttr(name = SHOW_FULL_DBNAME_IN_INFO_SCHEMA_DB, flag = VariableMgr.GLOBAL)
    public static boolean showFullDbNameInInfoSchemaDb = false;

    @VariableMgr.VarAttr(name = SQL_CONVERTER_SERVICE_URL, flag = VariableMgr.GLOBAL)
    public static String sqlConverterServiceUrl = "";

    @VariableMgr.VarAttr(name = ENABLE_AUDIT_PLUGIN, flag = VariableMgr.GLOBAL)
    public static boolean enableAuditLoader = true;

    @VariableMgr.VarAttr(name = AUDIT_PLUGIN_MAX_BATCH_BYTES, flag = VariableMgr.GLOBAL)
    public static long auditPluginMaxBatchBytes = 50 * 1024 * 1024;

    @VariableMgr.VarAttr(name = AUDIT_PLUGIN_MAX_BATCH_INTERVAL_SEC, flag = VariableMgr.GLOBAL)
    public static long auditPluginMaxBatchInternalSec = 60;

    @VariableMgr.VarAttr(name = AUDIT_PLUGIN_MAX_SQL_LENGTH, flag = VariableMgr.GLOBAL)
    public static int auditPluginMaxSqlLength = 2097152;

    @VariableMgr.VarAttr(name = AUDIT_PLUGIN_MAX_INSERT_STMT_LENGTH, flag = VariableMgr.GLOBAL,
            description = {"专门用于限制 INSERT 语句的长度。如果该值大于 AUDIT_PLUGIN_MAX_SQL_LENGTH，"
                    + "则使用 AUDIT_PLUGIN_MAX_SQL_LENGTH 的值。"
                    + "如果 INSERT 语句超过该长度，将会被截断。",
                    "This is specifically used to limit the length of INSERT statements. "
                            + "If this value is greater than AUDIT_PLUGIN_MAX_SQL_LENGTH, "
                            + "it will use the value of AUDIT_PLUGIN_MAX_SQL_LENGTH. "
                            + "If an INSERT statement exceeds this length, it will be truncated."})
    public static int auditPluginMaxInsertStmtLength = Integer.MAX_VALUE;

    @VariableMgr.VarAttr(name = AUDIT_PLUGIN_LOAD_TIMEOUT, flag = VariableMgr.GLOBAL)
    public static int auditPluginLoadTimeoutS = 600;

    @VariableMgr.VarAttr(name = ENABLE_GET_ROW_COUNT_FROM_FILE_LIST, flag = VariableMgr.GLOBAL,
            description = {
                    "针对外表，是否允许根据文件列表估算表行数。获取文件列表可能是一个耗时的操作，"
                            + "如果不需要估算表行数或者对性能有影响，可以关闭该功能。",
                    "For external tables, whether to enable getting row count from file list. "
                            + "Getting file list may be a time-consuming operation. "
                            + "If you don't need to estimate the number of rows in the table "
                            + "or it affects performance, you can disable this feature."})
    public static boolean enable_get_row_count_from_file_list = true;

    @VariableMgr.VarAttr(name = READ_ONLY, flag = VariableMgr.GLOBAL,
            description = {"仅用于兼容 MySQL 生态，暂无实际意义",
                    "Only for compatibility with MySQL ecosystem, no practical meaning"})
    public static boolean read_only = true;

    @VariableMgr.VarAttr(name = SUPER_READ_ONLY, flag = VariableMgr.GLOBAL,
            description = {"仅用于兼容 MySQL 生态，暂无实际意义",
                    "Only for compatibility with MySQL ecosystem, no practical meaning"})
    public static boolean super_read_only = true;

    @VariableMgr.VarAttr(name = PARTITION_ANALYZE_BATCH_SIZE, flag = VariableMgr.GLOBAL,
            description = {
                "批量收集分区信息的分区数",
                "Number of partitions to collect in one batch."})
    public static int partitionAnalyzeBatchSize = 10;

    @VariableMgr.VarAttr(name = HUGE_PARTITION_LOWER_BOUND_ROWS, flag = VariableMgr.GLOBAL,
            description = {
                "行数超过该值的分区将跳过自动分区收集",
                "This defines the lower size bound for large partitions, which will skip auto partition analyze."})
    public static long hugePartitionLowerBoundRows = 100000000L;

    @VariableMgr.VarAttr(name = ENABLE_FETCH_ICEBERG_STATS, flag = VariableMgr.GLOBAL,
            description = {
                "当 HMS catalog 中的 Iceberg 表没有统计信息时，是否通过 Iceberg Api 获取统计信息",
                "Enable fetch stats for HMS Iceberg table when it's not analyzed."})
    public static boolean enableFetchIcebergStats = false;


    @VariableMgr.VarAttr(name = ENABLE_ANSI_QUERY_ORGANIZATION_BEHAVIOR, flag = VariableMgr.GLOBAL,
            description = {
                    "控制 query organization 的行为。当设置为 true 时使用 ANSI 的 query organization 行为，即作用于整个语句。"
                            + "当设置为 false 时，使用 Doris 历史版本的行为，"
                            + "即 order by 默认只作用于 set operation 的最后一个 operand。",
                    "Controls the behavior of query organization. When set to true, uses the ANSI query"
                            + " organization behavior, which applies to the entire statement. When set to false,"
                            + " uses the behavior of Doris's historical versions, where order by by default only"
                            + " applies to the last operand of the set operation."})
    public static boolean enable_ansi_query_organization_behavior = true;

    @VariableMgr.VarAttr(name = ENABLE_NEW_TYPE_COERCION_BEHAVIOR, flag = VariableMgr.GLOBAL,
            description = {
                    "控制隐式类型转换的行为，当设置为 true 时，使用新的行为。新行为更为合理。类型优先级从高到低为时间相关类型 > "
                            + "数值类型 > 复杂类型 / JSON 类型 / IP 类型 > 字符串类型 > VARIANT 类型。当两个或多个不同类型的表达式"
                            + "进行比较时，强制类型转换优先向高优先级类型转换。转换时尽可能保留精度，如："
                            + "当转换为时间相关类型时，当无法确定精度时，优先使用 6 位精度的 DATETIME 类型。"
                            + "当转换为数值类型时，当无法确定精度时，优先使用 DECIMAL 类型。",
                    "Controls the behavior of implicit type conversion. When set to true, the new behavior is used,"
                            + " which is more reasonable. The type priority, from highest to lowest, is: time-related"
                            + " types > numeric types > complex types / JSON types / IP types > string types"
                            + " > VARIANT types. When comparing two or more expressions of different types, "
                            + "type coercion preferentially converts values toward the type with higher priority. "
                            + "Precision is preserved as much as possible during conversion. For example, "
                            + "when converting to a time-related type and precision cannot be determined, "
                            + "the DATETIME type with 6-digit precision is preferred. When converting to"
                            + " a numeric type and precision cannot be determined, the DECIMAL type is preferred."})
    public static boolean enableNewTypeCoercionBehavior = true;

    @VariableMgr.VarAttr(name = ENABLE_NESTED_NAMESPACE, flag = VariableMgr.GLOBAL,
            description = {
                    "是否允许访问 `ns1.ns2` 这种类型的 database。当前仅适用于 External Catalog 中映射 Database 并访问。"
                            + "不支持创建。",
                    "Whether to allow accessing databases of the form `ns1.ns2`. "
                            + "Currently, this only applies to mapping databases in "
                            + "External Catalogs and accessing them. "
                            + "Creation is not supported."})
    public static boolean enableNestedNamespace = false;

    // Don't allow creating instance.
    private GlobalVariable() {
    }

    public static List<String> getPersistentGlobalVarNames() {
        List<String> varNames = Lists.newArrayList();
        for (Field field : GlobalVariable.class.getDeclaredFields()) {
            VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
            if (attr != null && (attr.flag() & VariableMgr.GLOBAL) != 0) {
                varNames.add(attr.name());
            }
        }
        return varNames;
    }
}
