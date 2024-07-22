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

import java.util.MissingFormatArgumentException;

// Error code used to indicate what error happened.
public enum ErrorCode {
    // Try our best to compatible with MySQL's
    ERR_HASHCHK(1000, new byte[]{'H', 'Y', '0', '0', '0'}, "hashchk"),
    ERR_CANT_CREATE_TABLE(1005, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't create table '%s' (errno: %d - %s)"),
    ERR_DB_CREATE_EXISTS(1007, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't create database '%s'; database exists"),
    ERR_DB_DROP_EXISTS(1008, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't drop database '%s'; database doesn't exist"),
    ERR_DBACCESS_DENIED_ERROR(1044, new byte[]{'4', '2', '0', '0', '0'}, "Access denied for user '%s' to "
            + "database '%s'"),
    ERR_ACCESS_DENIED_ERROR(1045, new byte[]{'2', '8', '0', '0', '0'}, "Access denied for user '%s' (using "
            + "password: %s)"),
    ERR_NO_DB_ERROR(1046, new byte[]{'3', 'D', '0', '0', '0'}, "No database selected"),
    ERR_UNKNOWN_COM_ERROR(1047, new byte[]{'0', '8', 'S', '0', '1'}, "Unknown command"),
    ERR_BAD_DB_ERROR(1049, new byte[]{'4', '2', '0', '0', '0'}, "Unknown database '%s'"),
    ERR_TABLE_EXISTS_ERROR(1050, new byte[]{'4', '2', 'S', '0', '1'}, "Table '%s' already exists"),
    ERR_BAD_TABLE_ERROR(1051, new byte[]{'4', '2', 'S', '0', '2'}, "Unknown table '%s'"),
    ERR_NON_UNIQ_ERROR(1052, new byte[]{'2', '3', '0', '0', '0'}, "Column '%s' in field list is ambiguous"),
    ERR_BAD_FIELD_ERROR(1054, new byte[]{'4', '2', 'S', '2', '2'}, "Unknown column '%s' in '%s'"),
    ERR_WRONG_VALUE_COUNT(1058, new byte[]{'2', '1', 'S', '0', '1'}, "Column count doesn't match value count"),
    ERR_DUP_FIELDNAME(1060, new byte[]{'4', '2', 'S', '2', '1'}, "Duplicate column name '%s'"),
    ERR_NONUNIQ_TABLE(1066, new byte[]{'4', '2', '0', '0', '0'}, "Not unique table/alias: '%s'"),
    ERR_NO_SUCH_THREAD(1094, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown thread id: %d"),
    ERR_KILL_DENIED_ERROR(1095, new byte[]{'H', 'Y', '0', '0', '0'}, "You are not owner of thread %d"),
    ERR_NO_TABLES_USED(1096, new byte[]{'H', 'Y', '0', '0', '0'}, "No tables used"),
    ERR_NO_SUCH_QUERY(1097, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown query id: %s"),
    ERR_WRONG_DB_NAME(1102, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect database name '%s'"),
    ERR_WRONG_TABLE_NAME(1103, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect table name '%s'. Table name regex is '%s'"),
    ERR_TOO_BIG_SELECT(1104, new byte[]{'4', '2', '0', '0', '0'}, "The SELECT would examine more than MAX_JOIN_SIZE "
            + "rows; check your WHERE and use SET SQL_BIG_SELECTS=1 or SET MAX_JOIN_SIZE=# if the SELECT is okay"),
    ERR_UNKNOWN_ERROR(1105, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown error"),
    ERR_UNKNOWN_TABLE(1109, new byte[]{'4', '2', 'S', '0', '2'}, "Unknown table '%s' in %s"),
    ERR_FIELD_SPECIFIED_TWICE(1110, new byte[]{'4', '2', '0', '0', '0'}, "Column '%s' specified twice"),
    ERR_INVALID_GROUP_FUNC_USE(1111, new byte[]{'H', 'Y', '0', '0', '0'}, "Invalid use of group function"),
    ERR_TABLE_MUST_HAVE_COLUMNS(1113, new byte[]{'4', '2', '0', '0', '0'}, "A table must have at least 1 column"),
    ERR_UNKNOWN_CHARACTER_SET(1115, new byte[]{'4', '2', '0', '0', '0'}, "Unknown character set: '%s'"),
    ERR_NONEXISTING_GRANT(1141, new byte[]{'4', '2', '0', '0', '0'}, "There is no such grant defined for user '%s' on"
            + " host '%s'"),
    ERR_TABLEACCESS_DENIED_ERROR(1142, new byte[]{'4', '2', '0', '0', '0'}, "%s command denied to user '%s'@'%s' for "
            + "table '%s'"),
    ERR_NOT_ALLOWED_COMMAND(1148, new byte[]{'4', '2', '0', '0', '0'}, "The used command is not allowed"
            + " with this MySQL version"),
    ERR_WRONG_COLUMN_NAME(1166, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect column name '%s'. Column regex is '%s'"),
    ERR_UNKNOWN_SYSTEM_VARIABLE(1193, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown system variable '%s',"
                    + "the similar variables are %s"),
    ERR_BAD_SLAVE(1200, new byte[]{'H', 'Y', '0', '0', '0'}, "The server is not configured as slave; fix in config "
            + "file or with CHANGE MASTER TO"),
    ERR_MASTER_INF(1201, new byte[]{'H', 'Y', '0', '0', '0'}, "Could not initialize master info structure; more error"
            + " messages can be found in the MariaDB error log"),
    ERR_SLAVE_THREAD(1202, new byte[]{'H', 'Y', '0', '0', '0'}, "Could not create slave thread; check system "
            + "resources"),
    ERR_TOO_MANY_USER_CONNECTIONS(1203, new byte[]{'4', '2', '0', '0', '0'}, "User %s already has more than "
            + "'max_user_connections' active connections"),
    ERR_USER_LIMIT_REACHED(1226, new byte[]{'4', '2', '0', '0', '0'}, "User '%s' has exceeded the '%s' resource "
            + "(current value: %d)"),
    ERR_SPECIFIC_ACCESS_DENIED_ERROR(1227, new byte[]{'4', '2', '0', '0', '0'}, "Access denied; you need (at least "
            + "one of) the (%s) privilege(s) for this operation"),
    ERR_DB_ACCESS_DENIED_ERROR(1225, new byte[]{'4', '2', '0', '0', '0'}, "Access denied; you need (at least "
            + "one of) the (%s) privilege(s) on database %s for this operation"),

    ERR_TABLE_ACCESS_DENIED_ERROR(1224, new byte[]{'4', '2', '0', '0', '0'}, "Access denied; you need (at least "
            + "one of) the (%s) privilege(s) on table %s for this operation"),

    ERR_SPECIFIC_ALL_ACCESS_DENIED_ERROR(1223, new byte[] {'4', '2', '0', '0', '0'}, "Access denied; you need all "
            + " %s privilege(s) for this operation"),

    ERR_RESOURCE_ACCESS_DENIED_ERROR(1222, new byte[]{'4', '2', '0', '0', '0'}, "Access denied; you need (at least "
            + "one of) the (%s) privilege(s) on resource %s for this operation"),

    ERR_LOCAL_VARIABLE(1228, new byte[]{'H', 'Y', '0', '0', '0'}, "Variable '%s' is a SESSION variable and can't be "
            + "used with SET GLOBAL"),
    ERR_GLOBAL_VARIABLE(1229, new byte[]{'H', 'Y', '0', '0', '0'}, "Variable '%s' is a GLOBAL variable and should be "
            + "set with SET GLOBAL"),
    ERR_NO_DEFAULT(1230, new byte[]{'4', '2', '0', '0', '0'}, "Variable '%s' doesn't have a default value"),
    ERR_WRONG_VALUE_FOR_VAR(1231, new byte[]{'4', '2', '0', '0', '0'}, "Variable '%s' can't be set to the value of "
            + "'%s'"),
    ERR_WRONG_TYPE_FOR_VAR(1232, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect argument type to variable '%s'"),
    ERR_VAR_CANT_BE_READ(1233, new byte[]{'H', 'Y', '0', '0', '0'}, "Variable '%s' can only be set, not read"),
    ERR_CANT_USE_OPTION_HERE(1234, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect usage/placement of '%s'"),
    ERR_NOT_SUPPORTED_YET(1235, new byte[]{'4', '2', '0', '0', '0'}, "This version of MariaDB doesn't yet support "
            + "'%s'"),
    ERR_MASTER_FATAL_ERROR_READING_BINLOG(1236, new byte[]{'H', 'Y', '0', '0', '0'}, "Got fatal error %d from master "
            + "when reading data from binary log: '%s'"),
    ERR_SLAVE_IGNORED_TABLE(1237, new byte[]{'H', 'Y', '0', '0', '0'}, "Slave SQL thread ignored the query because of"
            + " replicate-*-table rules"),
    ERR_INCORRECT_GLOBAL_LOCAL_VAR(1238, new byte[]{'H', 'Y', '0', '0', '0'}, "Variable '%s' is a %s variable"),
    ERR_WRONG_FK_DEF(1239, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect foreign key definition for '%s': %s"),
    ERR_KEY_REF_DO_NOT_MATCH_TABLE_REF(1240, new byte[]{'H', 'Y', '0', '0', '0'}, "Key reference and table reference "
            + "don't match"),
    ERR_OPERAND_COLUMNS(1241, new byte[]{'2', '1', '0', '0', '0'}, "Operand should contain %d column(s)"),
    ERR_SUBQUERY_NO_1_ROW(1242, new byte[]{'2', '1', '0', '0', '0'}, "Subquery returns more than 1 row"),
    ERR_UNKNOWN_STMT_HANDLER(1243, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown prepared statement handler (%.*s) "
            + "given to %s"),
    ERR_CORRUPT_HELP_DB(1244, new byte[]{'H', 'Y', '0', '0', '0'}, "Help database is corrupt or does not exist"),
    ERR_CYCLIC_REFERENCE(1245, new byte[]{'H', 'Y', '0', '0', '0'}, "Cyclic reference on subqueries"),
    ERR_AUTO_CONVERT(1246, new byte[]{'H', 'Y', '0', '0', '0'}, "Converting column '%s' from %s to %s"),
    ERR_ILLEGAL_REFERENCE(1247, new byte[]{'4', '2', 'S', '2', '2'}, "Reference '%s' not supported (%s)"),
    ERR_DERIVED_MUST_HAVE_ALIAS(1248, new byte[]{'4', '2', '0', '0', '0'}, "Every derived table must have its own "
            + "alias"),
    ERR_SELECT_REDUCED(1249, new byte[]{'0', '1', '0', '0', '0'}, "Select %u was reduced during optimization"),
    ERR_TABLENAME_NOT_ALLOWED_HERE(1250, new byte[]{'4', '2', '0', '0', '0'}, "Table '%s' from one of the SELECTs "
            + "cannot be used in %s"),
    ERR_NOT_SUPPORTED_AUTH_MODE(1251, new byte[]{'0', '8', '0', '0', '4'}, "Client does not support authentication "
            + "protocol requested by server; consider upgrading MariaDB client"),
    ERR_SPATIAL_CANT_HAVE_NULL(1252, new byte[]{'4', '2', '0', '0', '0'}, "All parts of a SPATIAL index must be NOT "
            + "NULL"),
    ERR_COLLATION_CHARSET_MISMATCH(1253, new byte[]{'4', '2', '0', '0', '0'}, "COLLATION '%s' is not valid for "
            + "CHARACTER SET '%s'"),
    ERR_SLAVE_WAS_RUNNING(1254, new byte[]{'H', 'Y', '0', '0', '0'}, "Slave is already running"),
    ERR_SLAVE_WAS_NOT_RUNNING(1255, new byte[]{'H', 'Y', '0', '0', '0'}, "Slave already has been stopped"),
    ERR_TOO_BIG_FOR_UNCOMPRESS(1256, new byte[]{'H', 'Y', '0', '0', '0'}, "Uncompressed data size too large; the "
            + "maximum size is %d (probably, length of uncompressed data was corrupted)"),
    ERR_ZLIB_Z_MEM_ERROR(1257, new byte[]{'H', 'Y', '0', '0', '0'}, "ZLIB: Not enough memory"),
    ERR_ZLIB_Z_BUF_ERROR(1258, new byte[]{'H', 'Y', '0', '0', '0'}, "ZLIB: Not enough room in the output buffer "
            + "(probably, length of uncompressed data was corrupted)"),
    ERR_ZLIB_Z_DATA_ERROR(1259, new byte[]{'H', 'Y', '0', '0', '0'}, "ZLIB: Input data corrupted"),
    ERR_CUT_VALUE_GROUP_CONCAT(1260, new byte[]{'H', 'Y', '0', '0', '0'}, "Row %u was cut by GROUP_CONCAT()"),
    ERR_WARN_TOO_FEW_RECORDS(1261, new byte[]{'0', '1', '0', '0', '0'}, "Row %d doesn't contain data for all columns"),
    ERR_WARN_TOO_MANY_RECORDS(1262, new byte[]{'0', '1', '0', '0', '0'}, "Row %d was truncated; it contained more "
            + "data than there were input columns"),
    ERR_WARN_NULL_TO_NOTNULL(1263, new byte[]{'2', '2', '0', '0', '4'}, "Column set to default value; NULL supplied "
            + "to NOT NULL column '%s' at row %d"),
    ERR_WARN_DATA_OUT_OF_RANGE(1264, new byte[]{'2', '2', '0', '0', '3'}, "Out of range value for column '%s' at row "
            + "%d"),
    WARN_DATA_TRUNCATED(1265, new byte[]{'0', '1', '0', '0', '0'}, "Data truncated for column '%s' at row %d"),
    ERR_WARN_USING_OTHER_HANDLER(1266, new byte[]{'H', 'Y', '0', '0', '0'}, "Using storage engine %s for table '%s'"),
    ERR_CANT_AGGREGATE_2COLLATIONS(1267, new byte[]{'H', 'Y', '0', '0', '0'}, "Illegal mix of collations (%s,%s) and "
            + "(%s,%s) for operation '%s'"),
    ERR_DROP_USER(1268, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot drop one or more of the requested users"),
    ERR_REVOKE_GRANTS(1269, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't revoke all privileges for one or more of the "
            + "requested users"),
    ERR_CANT_AGGREGATE_3COLLATIONS(1270, new byte[]{'H', 'Y', '0', '0', '0'}, "Illegal mix of collations (%s,%s), "
            + "(%s,%s), (%s,%s) for operation '%s'"),
    ERR_CANT_AGGREGATE_NCOLLATIONS(1271, new byte[]{'H', 'Y', '0', '0', '0'}, "Illegal mix of collations for "
            + "operation '%s'"),
    ERR_VARIABLE_IS_NOT_STRUCT(1272, new byte[]{'H', 'Y', '0', '0', '0'}, "Variable '%s' is not a variable component "
            + "(can't be used as XXXX.variable_name)"),
    ERR_UNKNOWN_COLLATION(1273, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown collation: '%s'"),
    ERR_SLAVE_IGNORED_SSL_PARAMS(1274, new byte[]{'H', 'Y', '0', '0', '0'}, "SSL parameters in CHANGE MASTER are "
            + "ignored because this MariaDB slave was compiled without SSL support; they can be used later if MariaDB "
            + "slave "
            + "with SSL is started"),
    ERR_SERVER_IS_IN_SECURE_AUTH_MODE(1275, new byte[]{'H', 'Y', '0', '0', '0'}, "Server is running in --secure-auth "
            + "mode, but '%s'@'%s' has a password in the old format; please change the password to the new format"),
    ERR_WARN_FIELD_RESOLVED(1276, new byte[]{'H', 'Y', '0', '0', '0'}, "Field or reference '%s%s%s%s%s' of SELECT #%d"
            + " was resolved in SELECT #%d"),
    ERR_BAD_SLAVE_UNTIL_COND(1277, new byte[]{'H', 'Y', '0', '0', '0'}, "Incorrect parameter or combination of "
            + "parameters for START SLAVE UNTIL"),
    ERR_MISSING_SKIP_SLAVE(1278, new byte[]{'H', 'Y', '0', '0', '0'}, "It is recommended to use --skip-slave-start "
            + "when doing step-by-step replication with START SLAVE UNTIL; otherwise, you will get problems if you get "
            + "an "
            + "unexpected slave's mysqld restart"),
    ERR_UNTIL_COND_IGNORED(1279, new byte[]{'H', 'Y', '0', '0', '0'}, "SQL thread is not to be started so UNTIL "
            + "options are ignored"),
    ERR_WRONG_NAME_FOR_INDEX(1280, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect index name '%s'"),
    ERR_WRONG_NAME_FOR_CATALOG(1281, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect catalog name '%s'"),
    ERR_WARN_QC_RESIZE(1282, new byte[]{'H', 'Y', '0', '0', '0'}, "Query cache failed to set size %d; new query "
            + "cache size is %d"),
    ERR_BAD_FT_COLUMN(1283, new byte[]{'H', 'Y', '0', '0', '0'}, "Column '%s' cannot be part of FULLTEXT index"),
    ERR_UNKNOWN_KEY_CACHE(1284, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown key cache '%s'"),
    ERR_WARN_HOSTNAME_WONT_WORK(1285, new byte[]{'H', 'Y', '0', '0', '0'}, "MariaDB is started in --skip-name-resolve"
            + " mode; you must restart it without this switch for this grant to work"),
    ERR_UNKNOWN_STORAGE_ENGINE(1286, new byte[]{'4', '2', '0', '0', '0'}, "Unknown storage engine '%s'"),
    ERR_WARN_DEPRECATED_SYNTAX(1287, new byte[]{'H', 'Y', '0', '0', '0'}, "'%s' is deprecated and will be removed in "
            + "a future release. Please use %s instead"),
    ERR_NON_UPDATABLE_TABLE(1288, new byte[]{'H', 'Y', '0', '0', '0'}, "The target table %s of the %s is not "
            + "updatable"),
    ERR_FEATURE_DISABLED(1289, new byte[]{'H', 'Y', '0', '0', '0'}, "The '%s' feature is disabled; you need MariaDB "
            + "built with '%s' to have it working"),
    ERR_OPTION_PREVENTS_STATEMENT(1290, new byte[]{'H', 'Y', '0', '0', '0'}, "The MariaDB server is running with the "
            + "%s option so it cannot execute this statement"),
    ERR_DUPLICATED_VALUE_IN_TYPE(1291, new byte[]{'H', 'Y', '0', '0', '0'}, "Column '%s' has duplicated value '%s' in"
            + " %s"),
    ERR_TRUNCATED_WRONG_VALUE(1292, new byte[]{'2', '2', '0', '0', '7'}, "Truncated incorrect %s value: '%s'"),
    ERR_TOO_MUCH_AUTO_TIMESTAMP_COLS(1293, new byte[]{'H', 'Y', '0', '0', '0'}, "Incorrect table definition; there "
            + "can be only one TIMESTAMP column with CURRENT_TIMESTAMP in DEFAULT or ON UPDATE clause"),
    ERR_INVALID_ON_UPDATE(1294, new byte[]{'H', 'Y', '0', '0', '0'}, "Invalid ON UPDATE clause for '%s' column"),
    ERR_UNSUPPORTED_PS(1295, new byte[]{'H', 'Y', '0', '0', '0'}, "This command is not supported in the prepared "
            + "statement protocol yet"),
    ERR_GET_ERRMSG(1296, new byte[]{'H', 'Y', '0', '0', '0'}, "Got error %d '%s' from %s"),
    ERR_GET_TEMPORARY_ERRMSG(1297, new byte[]{'H', 'Y', '0', '0', '0'}, "Got temporary error %d '%s' from %s"),
    ERR_UNKNOWN_TIME_ZONE(1298, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown or incorrect time zone: '%s'"),
    ERR_WARN_INVALID_TIMESTAMP(1299, new byte[]{'H', 'Y', '0', '0', '0'}, "Invalid TIMESTAMP value in column '%s' at "
            + "row %d"),
    ERR_INVALID_CHARACTER_STRING(1300, new byte[]{'H', 'Y', '0', '0', '0'}, "Invalid %s character string: '%s'"),
    ERR_WARN_ALLOWED_PACKET_OVERFLOWED(1301, new byte[]{'H', 'Y', '0', '0', '0'}, "Result of %s() was larger than "
            + "max_allowed_packet (%d) - truncated"),
    ERR_CONFLICTING_DECLARATIONS(1302, new byte[]{'H', 'Y', '0', '0', '0'}, "Conflicting declarations: '%s%s' and "
            + "'%s%s'"),
    ERR_SP_NO_RECURSIVE_CREATE(1303, new byte[]{'2', 'F', '0', '0', '3'}, "Can't create a %s from within another "
            + "stored routine"),
    ERR_SP_ALREADY_EXISTS(1304, new byte[]{'4', '2', '0', '0', '0'}, "%s %s already exists"),
    ERR_SP_DOES_NOT_EXIST(1305, new byte[]{'4', '2', '0', '0', '0'}, "%s %s does not exist"),
    ERR_SP_DROP_FAILED(1306, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to DROP %s %s"),
    ERR_SP_STORE_FAILED(1307, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to CREATE %s %s"),
    ERR_SP_LILABEL_MISMATCH(1308, new byte[]{'4', '2', '0', '0', '0'}, "%s with no matching label: %s"),
    ERR_SP_LABEL_REDEFINE(1309, new byte[]{'4', '2', '0', '0', '0'}, "Redefining label %s"),
    ERR_SP_LABEL_MISMATCH(1310, new byte[]{'4', '2', '0', '0', '0'}, "End-label %s without match"),
    ERR_SP_UNINIT_VAR(1311, new byte[]{'0', '1', '0', '0', '0'}, "Referring to uninitialized variable %s"),
    ERR_SP_BADSELECT(1312, new byte[]{'0', 'A', '0', '0', '0'}, "PROCEDURE %s can't return a result set in the given "
            + "context"),
    ERR_SP_BADRETURN(1313, new byte[]{'4', '2', '0', '0', '0'}, "RETURN is only allowed in a FUNCTION"),
    ERR_SP_BADSTATEMENT(1314, new byte[]{'0', 'A', '0', '0', '0'}, "%s is not allowed in stored procedures"),
    ERR_UPDATE_LOG_DEPRECATED_IGNORED(1315, new byte[]{'4', '2', '0', '0', '0'}, "The update log is deprecated and "
            + "replaced by the binary log; SET SQL_LOG_UPDATE has been ignored. This option will be removed in MariaDB "
            + "5.6."),
    ERR_UPDATE_LOG_DEPRECATED_TRANSLATED(1316, new byte[]{'4', '2', '0', '0', '0'}, "The update log is deprecated and"
            + " replaced by the binary log; SET SQL_LOG_UPDATE has been translated to SET SQL_LOG_BIN. This option will"
            + " be "
            + "removed in MariaDB 5.6."),
    ERR_QUERY_INTERRUPTED(1317, new byte[]{'7', '0', '1', '0', '0'}, "Query execution was interrupted"),
    ERR_SP_WRONG_NO_OF_ARGS(1318, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect number of arguments for %s %s; "
            + "expected %u, got %u"),
    ERR_SP_COND_MISMATCH(1319, new byte[]{'4', '2', '0', '0', '0'}, "Undefined CONDITION: %s"),
    ERR_SP_NORETURN(1320, new byte[]{'4', '2', '0', '0', '0'}, "No RETURN found in FUNCTION %s"),
    ERR_SP_NORETURNEND(1321, new byte[]{'2', 'F', '0', '0', '5'}, "FUNCTION %s ended without RETURN"),
    ERR_SP_BAD_CURSOR_QUERY(1322, new byte[]{'4', '2', '0', '0', '0'}, "Cursor statement must be a SELECT"),
    ERR_SP_BAD_CURSOR_SELECT(1323, new byte[]{'4', '2', '0', '0', '0'}, "Cursor SELECT must not have INTO"),
    ERR_SP_CURSOR_MISMATCH(1324, new byte[]{'4', '2', '0', '0', '0'}, "Undefined CURSOR: %s"),
    ERR_SP_CURSOR_ALREADY_OPEN(1325, new byte[]{'2', '4', '0', '0', '0'}, "Cursor is already open"),
    ERR_SP_CURSOR_NOT_OPEN(1326, new byte[]{'2', '4', '0', '0', '0'}, "Cursor is not open"),
    ERR_SP_UNDECLARED_VAR(1327, new byte[]{'4', '2', '0', '0', '0'}, "Undeclared variable: %s"),
    ERR_SP_WRONG_NO_OF_FETCH_ARGS(1328, new byte[]{'H', 'Y', '0', '0', '0'}, "Incorrect number of FETCH variables"),
    ERR_SP_FETCH_NO_DATA(1329, new byte[]{'0', '2', '0', '0', '0'}, "No data - zero rows fetched, selected, or "
            + "processed"),
    ERR_SP_DUP_PARAM(1330, new byte[]{'4', '2', '0', '0', '0'}, "Duplicate parameter: %s"),
    ERR_SP_DUP_VAR(1331, new byte[]{'4', '2', '0', '0', '0'}, "Duplicate variable: %s"),
    ERR_SP_DUP_COND(1332, new byte[]{'4', '2', '0', '0', '0'}, "Duplicate condition: %s"),
    ERR_SP_DUP_CURS(1333, new byte[]{'4', '2', '0', '0', '0'}, "Duplicate cursor: %s"),
    ERR_SP_CANT_ALTER(1334, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to ALTER %s %s"),
    ERR_SP_SUBSELECT_NYI(1335, new byte[]{'0', 'A', '0', '0', '0'}, "Subquery value not supported"),
    ERR_STMT_NOT_ALLOWED_IN_SF_OR_TRG(1336, new byte[]{'0', 'A', '0', '0', '0'}, "%s is not allowed in stored "
            + "function or trigger"),
    ERR_SP_VARCOND_AFTER_CURSHNDLR(1337, new byte[]{'4', '2', '0', '0', '0'}, "Variable or condition declaration "
            + "after cursor or handler declaration"),
    ERR_SP_CURSOR_AFTER_HANDLER(1338, new byte[]{'4', '2', '0', '0', '0'}, "Cursor declaration after handler "
            + "declaration"),
    ERR_SP_CASE_NOT_FOUND(1339, new byte[]{'2', '0', '0', '0', '0'}, "Case not found for CASE statement"),
    ERR_FPARSER_TOO_BIG_FILE(1340, new byte[]{'H', 'Y', '0', '0', '0'}, "Configuration file '%s' is too big"),
    ERR_FPARSER_BAD_HEADER(1341, new byte[]{'H', 'Y', '0', '0', '0'}, "Malformed file type header in file '%s'"),
    ERR_FPARSER_EOF_IN_COMMENT(1342, new byte[]{'H', 'Y', '0', '0', '0'}, "Unexpected end of file while parsing "
            + "comment '%s'"),
    ERR_FPARSER_ERROR_IN_PARAMETER(1343, new byte[]{'H', 'Y', '0', '0', '0'}, "Error while parsing parameter '%s' "
            + "(line: '%s')"),
    ERR_FPARSER_EOF_IN_UNKNOWN_PARAMETER(1344, new byte[]{'H', 'Y', '0', '0', '0'}, "Unexpected end of file while "
            + "skipping unknown parameter '%s'"),
    ERR_VIEW_NO_EXPLAIN(1345, new byte[]{'H', 'Y', '0', '0', '0'}, "EXPLAIN/SHOW can not be issued; lacking "
            + "privileges for underlying table"),
    ERR_FRM_UNKNOWN_TYPE(1346, new byte[]{'H', 'Y', '0', '0', '0'}, "File '%s' has unknown type '%s' in its header"),
    ERR_WRONG_OBJECT(1347, new byte[]{'H', 'Y', '0', '0', '0'}, "'%s.%s' is not %s. %s."),
    ERR_NONUPDATEABLE_COLUMN(1348, new byte[]{'H', 'Y', '0', '0', '0'}, "Column '%s' is not updatable"),
    ERR_VIEW_SELECT_DERIVED(1349, new byte[]{'H', 'Y', '0', '0', '0'}, "View's SELECT contains a subquery in the FROM"
            + " clause"),
    ERR_VIEW_SELECT_CLAUSE(1350, new byte[]{'H', 'Y', '0', '0', '0'}, "View's SELECT contains a '%s' clause"),
    ERR_VIEW_SELECT_VARIABLE(1351, new byte[]{'H', 'Y', '0', '0', '0'}, "View's SELECT contains a variable or "
            + "parameter"),
    ERR_VIEW_SELECT_TMPTABLE(1352, new byte[]{'H', 'Y', '0', '0', '0'}, "View's SELECT refers to a temporary table "
            + "'%s'"),
    ERR_VIEW_WRONG_LIST(1353, new byte[]{'H', 'Y', '0', '0', '0'}, "View's SELECT and view's field list have "
            + "different column counts"),
    ERR_WARN_VIEW_MERGE(1354, new byte[]{'H', 'Y', '0', '0', '0'}, "View merge algorithm can't be used here for now "
            + "(assumed undefined algorithm)"),
    ERR_WARN_VIEW_WITHOUT_KEY(1355, new byte[]{'H', 'Y', '0', '0', '0'}, "View being updated does not have complete "
            + "key of underlying table in it"),
    ERR_VIEW_INVALID(1356, new byte[]{'H', 'Y', '0', '0', '0'}, "View '%s.%s' references invalid table(s) or column"
            + "(s) or function(s) or definer/invoker of view lack rights to use them"),
    ERR_SP_NO_DROP_SP(1357, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't drop or alter a %s from within another stored"
            + " routine"),
    ERR_SP_GOTO_IN_HNDLR(1358, new byte[]{'H', 'Y', '0', '0', '0'}, "GOTO is not allowed in a stored procedure "
            + "handler"),
    ERR_TRG_ALREADY_EXISTS(1359, new byte[]{'H', 'Y', '0', '0', '0'}, "Trigger already exists"),
    ERR_TRG_DOES_NOT_EXIST(1360, new byte[]{'H', 'Y', '0', '0', '0'}, "Trigger does not exist"),
    ERR_TRG_ON_VIEW_OR_TEMP_TABLE(1361, new byte[]{'H', 'Y', '0', '0', '0'}, "Trigger's '%s' is view or temporary "
            + "table"),
    ERR_TRG_CANT_CHANGE_ROW(1362, new byte[]{'H', 'Y', '0', '0', '0'}, "Updating of %s row is not allowed in "
            + "%strigger"),
    ERR_TRG_NO_SUCH_ROW_IN_TRG(1363, new byte[]{'H', 'Y', '0', '0', '0'}, "There is no %s row in %s trigger"),
    ERR_NO_DEFAULT_FOR_FIELD(1364, new byte[]{'H', 'Y', '0', '0', '0'}, "Field '%s' doesn't have a default value"),
    ERR_DIVISION_BY_ZER(1365, new byte[]{'2', '2', '0', '1', '2'}, "Division by 0"),
    ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD(1366, new byte[]{'H', 'Y', '0', '0', '0'}, "Incorrect %s value: '%s' for "
            + "column '%s' at row %d"),
    ERR_ILLEGAL_VALUE_FOR_TYPE(1367, new byte[]{'2', '2', '0', '0', '7'}, "Illegal %s '%s' value found during parsing"),
    ERR_VIEW_NONUPD_CHECK(1368, new byte[]{'H', 'Y', '0', '0', '0'}, "CHECK OPTION on non-updatable view '%s.%s'"),
    ERR_VIEW_CHECK_FAILED(1369, new byte[]{'H', 'Y', '0', '0', '0'}, "CHECK OPTION failed '%s.%s'"),
    ERR_PROCACCESS_DENIED_ERROR(1370, new byte[]{'4', '2', '0', '0', '0'}, "%s command denied to user '%s'@'%s' for "
            + "routine '%s'"),
    ERR_RELAY_LOG_FAIL(1371, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed purging old relay logs: %s"),
    ERR_PASSWD_LENGTH(1372, new byte[]{'H', 'Y', '0', '0', '0'}, "Password hash should be a %d-digit hexadecimal "
            + "number"),
    ERR_UNKNOWN_TARGET_BINLOG(1373, new byte[]{'H', 'Y', '0', '0', '0'}, "Target log not found in binlog index"),
    ERR_IO_ERR_LOG_INDEX_READ(1374, new byte[]{'H', 'Y', '0', '0', '0'}, "I/O error reading log index file"),
    ERR_BINLOG_PURGE_PROHIBITED(1375, new byte[]{'H', 'Y', '0', '0', '0'}, "Server configuration does not permit "
            + "binlog purge"),
    ERR_FSEEK_FAIL(1376, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed on fseek()"),
    ERR_BINLOG_PURGE_FATAL_ERR(1377, new byte[]{'H', 'Y', '0', '0', '0'}, "Fatal error during log purge"),
    ERR_LOG_IN_USE(1378, new byte[]{'H', 'Y', '0', '0', '0'}, "A purgeable log is in use, will not purge"),
    ERR_LOG_PURGE_UNKNOWN_ERR(1379, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown error during log purge"),
    ERR_RELAY_LOG_INIT(1380, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed initializing relay log position: %s"),
    ERR_NO_BINARY_LOGGING(1381, new byte[]{'H', 'Y', '0', '0', '0'}, "You are not using binary logging"),
    ERR_RESERVED_SYNTAX(1382, new byte[]{'H', 'Y', '0', '0', '0'}, "The '%s' syntax is reserved for purposes internal"
            + " to the MariaDB server"),
    ERR_WSAS_FAILED(1383, new byte[]{'H', 'Y', '0', '0', '0'}, "WSAStartup Failed"),
    ERR_DIFF_GROUPS_PROC(1384, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't handle procedures with different groups "
            + "yet"),
    ERR_NO_GROUP_FOR_PROC(1385, new byte[]{'H', 'Y', '0', '0', '0'}, "Select must have a group with this procedure"),
    ERR_ORDER_WITH_PROC(1386, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't use ORDER clause with this procedure"),
    ERR_LOGGING_PROHIBIT_CHANGING_OF(1387, new byte[]{'H', 'Y', '0', '0', '0'}, "Binary logging and replication "
            + "forbid changing the global server %s"),
    ERR_NO_FILE_MAPPING(1388, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't map file: %s, errno: %d"),
    ERR_WRONG_MAGIC(1389, new byte[]{'H', 'Y', '0', '0', '0'}, "Wrong magic in %s"),
    ERR_PS_MANY_PARAM(1390, new byte[]{'H', 'Y', '0', '0', '0'}, "Prepared statement contains too many placeholders"),
    ERR_KEY_PART_0(1391, new byte[]{'H', 'Y', '0', '0', '0'}, "Key part '%s' length cannot be 0"),
    ERR_VIEW_CHECKSUM(1392, new byte[]{'H', 'Y', '0', '0', '0'}, "View text checksum failed"),
    ERR_VIEW_MULTIUPDATE(1393, new byte[]{'H', 'Y', '0', '0', '0'}, "Can not modify more than one base table through "
            + "a join view '%s.%s'"),
    ERR_VIEW_NO_INSERT_FIELD_LIST(1394, new byte[]{'H', 'Y', '0', '0', '0'}, "Can not insert into join view '%s.%s' "
            + "without fields list"),
    ERR_VIEW_DELETE_MERGE_VIEW(1395, new byte[]{'H', 'Y', '0', '0', '0'}, "Can not delete from join view '%s.%s'"),
    ERR_CANNOT_USER(1396, new byte[]{'H', 'Y', '0', '0', '0'}, "Operation %s failed for %s"),
    ERR_XAER_NOTA(1397, new byte[]{'X', 'A', 'E', '0', '4'}, "XAER_NOTA: Unknown XID"),
    ERR_XAER_INVAL(1398, new byte[]{'X', 'A', 'E', '0', '5'}, "XAER_INVAL: Invalid arguments (or unsupported command)"),
    ERR_XAER_RMFAIL(1399, new byte[]{'X', 'A', 'E', '0', '7'}, "XAER_RMFAIL: The command cannot be executed when "
            + "global transaction is in the %s state"),
    ERR_XAER_OUTSIDE(1400, new byte[]{'X', 'A', 'E', '0', '9'}, "XAER_OUTSIDE: Some work is done outside global "
            + "transaction"),
    ERR_XAER_RMERR(1401, new byte[]{'X', 'A', 'E', '0', '3'}, "XAER_RMERR: Fatal error occurred in the transaction "
            + "branch - check your data for consistency"),
    ERR_XA_RBROLLBACK(1402, new byte[]{'X', 'A', '1', '0', '0'}, "XA_RBROLLBACK: Transaction branch was rolled back"),
    ERR_NONEXISTING_PROC_GRANT(1403, new byte[]{'4', '2', '0', '0', '0'}, "There is no such grant defined for user "
            + "'%s' on host '%s' on routine '%s'"),
    ERR_PROC_AUTO_GRANT_FAIL(1404, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to grant EXECUTE and ALTER ROUTINE "
            + "privileges"),
    ERR_PROC_AUTO_REVOKE_FAIL(1405, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to revoke all privileges to dropped "
            + "routine"),
    ERR_DATA_TOO_LONG(1406, new byte[]{'2', '2', '0', '0', '1'}, "Data too long for column '%s' at row %d"),
    ERR_SP_BAD_SQLSTATE(1407, new byte[]{'4', '2', '0', '0', '0'}, "Bad SQLSTATE: '%s'"),
    ERR_STARTUP(1408, new byte[]{'H', 'Y', '0', '0', '0'}, "%s: ready for connections. Version: '%s' socket: '%s' "
            + "port: %d %s"),
    ERR_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR(1409, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't load value from file with "
            + "fixed size rows to variable"),
    ERR_CANT_CREATE_USER_WITH_GRANT(1410, new byte[]{'4', '2', '0', '0', '0'}, "You are not allowed to create a user "
            + "with GRANT"),
    ERR_WRONG_VALUE_FOR_TYPE(1411, new byte[]{'H', 'Y', '0', '0', '0'}, "Incorrect %s value: '%s' for function %s"),
    ERR_TABLE_DEF_CHANGED(1412, new byte[]{'H', 'Y', '0', '0', '0'}, "Table definition has changed, please retry "
            + "transaction"),
    ERR_SP_DUP_HANDLER(1413, new byte[]{'4', '2', '0', '0', '0'}, "Duplicate handler declared in the same block"),
    ERR_SP_NOT_VAR_ARG(1414, new byte[]{'4', '2', '0', '0', '0'}, "OUT or INOUT argument %d for routine %s is not a "
            + "variable or NEW pseudo-variable in BEFORE trigger"),
    ERR_SP_NO_RETSET(1415, new byte[]{'0', 'A', '0', '0', '0'}, "Not allowed to return a result set from a %s"),
    ERR_CANT_CREATE_GEOMETRY_OBJECT(1416, new byte[]{'2', '2', '0', '0', '3'}, "Cannot get geometry object from data "
            + "you send to the GEOMETRY field"),
    ERR_FAILED_ROUTINE_BREAK_BINLOG(1417, new byte[]{'H', 'Y', '0', '0', '0'}, "A routine failed and has neither NO "
            + "SQL nor READS SQL DATA in its declaration and binary logging is enabled; if non-transactional tables "
            + "were "
            + "updated, the binary log will miss their changes"),
    ERR_BINLOG_UNSAFE_ROUTINE(1418, new byte[]{'H', 'Y', '0', '0', '0'}, "This function has none of DETERMINISTIC, NO"
            + " SQL, or READS SQL DATA in its declaration and binary logging is enabled (you *might* want to use the "
            + "less safe"
            + " log_bin_trust_function_creators variable)"),
    ERR_BINLOG_CREATE_ROUTINE_NEED_SUPER(1419, new byte[]{'H', 'Y', '0', '0', '0'}, "You do not have the SUPER "
            + "privilege and binary logging is enabled (you *might* want to use the less safe "
            + "log_bin_trust_function_creators "
            + "variable)"),
    ERR_EXEC_STMT_WITH_OPEN_CURSOR(1420, new byte[]{'H', 'Y', '0', '0', '0'}, "You can't execute a prepared statement"
            + " which has an open cursor associated with it. Reset the statement to re-execute it."),
    ERR_STMT_HAS_NO_OPEN_CURSOR(1421, new byte[]{'H', 'Y', '0', '0', '0'}, "The statement (%d) has no open cursor."),
    ERR_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG(1422, new byte[]{'H', 'Y', '0', '0', '0'}, "Explicit or implicit commit is "
            + "not allowed in stored function or trigger."),
    ERR_NO_DEFAULT_FOR_VIEW_FIELD(1423, new byte[]{'H', 'Y', '0', '0', '0'}, "Field of view '%s.%s' underlying table "
            + "doesn't have a default value"),
    ERR_SP_NO_RECURSION(1424, new byte[]{'H', 'Y', '0', '0', '0'}, "Recursive stored functions and triggers are not "
            + "allowed."),
    ERR_TOO_BIG_SCALE(1425, new byte[]{'4', '2', '0', '0', '0'}, "Too big scale %d specified for column '%s'. Maximum"
            + " is %d."),
    ERR_TOO_BIG_PRECISION(1426, new byte[]{'4', '2', '0', '0', '0'}, "Too big precision %d specified for column '%s'."
            + " Maximum is %d."),
    ERR_M_BIGGER_THAN_D(1427, new byte[]{'4', '2', '0', '0', '0'}, "For float(M,D, double(M,D or decimal(M,D, M must "
            + "be >= D (column '%s')."),
    ERR_WRONG_LOCK_OF_SYSTEM_TABLE(1428, new byte[]{'H', 'Y', '0', '0', '0'}, "You can't combine write-locking of "
            + "system tables with other tables or lock types"),
    ERR_CONNECT_TO_FOREIGN_DATA_SOURCE(1429, new byte[]{'H', 'Y', '0', '0', '0'}, "Unable to connect to foreign data "
            + "source: %s"),
    ERR_QUERY_ON_FOREIGN_DATA_SOURCE(1430, new byte[]{'H', 'Y', '0', '0', '0'}, "There was a problem processing the "
            + "query on the foreign data source. Data source error: %s"),
    ERR_FOREIGN_DATA_SOURCE_DOESNT_EXIST(1431, new byte[]{'H', 'Y', '0', '0', '0'}, "The foreign data source you are "
            + "trying to reference does not exist. Data source error: %s"),
    ERR_FOREIGN_DATA_STRING_INVALID_CANT_CREATE(1432, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't create federated "
            + "table. The data source connection string '%s' is not in the correct format"),
    ERR_FOREIGN_DATA_STRING_INVALID(1433, new byte[]{'H', 'Y', '0', '0', '0'}, "The data source connection string "
            + "'%s' is not in the correct format"),
    ERR_CANT_CREATE_FEDERATED_TABLE(1434, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't create federated table. Foreign"
            + " data src error: %s"),
    ERR_TRG_IN_WRONG_SCHEMA(1435, new byte[]{'H', 'Y', '0', '0', '0'}, "Trigger in wrong schema"),
    ERR_STACK_OVERRUN_NEED_MORE(1436, new byte[]{'H', 'Y', '0', '0', '0'}, "Thread stack overrun: %d bytes used of a"
            + " %d byte stack, and %d bytes needed. Use 'mysqld --thread_stack=#' to specify a bigger stack."),
    ERR_TOO_LONG_BODY(1437, new byte[]{'4', '2', '0', '0', '0'}, "Routine body for '%s' is too long"),
    ERR_WARN_CANT_DROP_DEFAULT_KEYCACHE(1438, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot drop default keycache"),
    ERR_TOO_BIG_DISPLAYWIDTH(1439, new byte[]{'4', '2', '0', '0', '0'}, "Display width out of range for column '%s' "
            + "(max = %d)"),
    ERR_XAER_DUPID(1440, new byte[]{'X', 'A', 'E', '0', '8'}, "XAER_DUPID: The XID already exists"),
    ERR_DATETIME_FUNCTION_OVERFLOW(1441, new byte[]{'2', '2', '0', '0', '8'}, "Datetime function: %s field overflow"),
    ERR_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG(1442, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't update table '%s' in "
            + "stored function/trigger because it is already used by statement which invoked this stored "
            + "function/trigger."),
    ERR_VIEW_PREVENT_UPDATE(1443, new byte[]{'H', 'Y', '0', '0', '0'}, "The definition of table '%s' prevents "
            + "operation %s on table '%s'."),
    ERR_PS_NO_RECURSION(1444, new byte[]{'H', 'Y', '0', '0', '0'}, "The prepared statement contains a stored routine "
            + "call that refers to that same statement. It's not allowed to execute a prepared statement in such a "
            + "recursive "
            + "manner"),
    ERR_SP_CANT_SET_AUTOCOMMIT(1445, new byte[]{'H', 'Y', '0', '0', '0'}, "Not allowed to set autocommit from a "
            + "stored function or trigger"),
    ERR_MALFORMED_DEFINER(1446, new byte[]{'H', 'Y', '0', '0', '0'}, "Definer is not fully qualified"),
    ERR_VIEW_FRM_NO_USER(1447, new byte[]{'H', 'Y', '0', '0', '0'}, "View '%s'.'%s' has no definer information (old "
            + "table format). Current user is used as definer. Please recreate the view!"),
    ERR_VIEW_OTHER_USER(1448, new byte[]{'H', 'Y', '0', '0', '0'}, "You need the SUPER privilege for creation view "
            + "with '%s'@'%s' definer"),
    ERR_NO_SUCH_USER(1449, new byte[]{'H', 'Y', '0', '0', '0'}, "The user specified as a definer ('%s'@'%s') does not"
            + " exist"),
    ERR_FORBID_SCHEMA_CHANGE(1450, new byte[]{'H', 'Y', '0', '0', '0'}, "Changing schema from '%s' to '%s' is not "
            + "allowed."),
    ERR_ROW_IS_REFERENCED_2(1451, new byte[]{'2', '3', '0', '0', '0'}, "Cannot delete or update a parent row: a "
            + "foreign key constraint fails (%s)"),
    ERR_NO_REFERENCED_ROW_2(1452, new byte[]{'2', '3', '0', '0', '0'}, "Cannot add or update a child row: a foreign "
            + "key constraint fails (%s)"),
    ERR_SP_BAD_VAR_SHADOW(1453, new byte[]{'4', '2', '0', '0', '0'}, "Variable '%s' must be quoted with `...`, or "
            + "renamed"),
    ERR_TRG_NO_DEFINER(1454, new byte[]{'H', 'Y', '0', '0', '0'}, "No definer attribute for trigger '%s'.'%s'. The "
            + "trigger will be activated under the authorization of the invoker, which may have insufficient privileges"
            + ". "
            + "Please recreate the trigger."),
    ERR_OLD_FILE_FORMAT(1455, new byte[]{'H', 'Y', '0', '0', '0'}, "'%s' has an old format, you should re-create the "
            + "'%s' object(s)"),
    ERR_SP_RECURSION_LIMIT(1456, new byte[]{'H', 'Y', '0', '0', '0'}, "Recursive limit %d (as set by the "
            + "max_sp_recursion_depth variable) was exceeded for routine %s"),
    ERR_SP_PROC_TABLE_CORRUPT(1457, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to load routine %s. The table mysql"
            + ".proc is missing, corrupt, or contains bad data (internal code %d)"),
    ERR_SP_WRONG_NAME(1458, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect routine name '%s'"),
    ERR_TABLE_NEEDS_UPGRADE(1459, new byte[]{'H', 'Y', '0', '0', '0'}, "Table upgrade required. Please do \"REPAIR "
            + "TABLE `%s`\" or dump/reload to fix it!"),
    ERR_SP_NO_AGGREGATE(1460, new byte[]{'4', '2', '0', '0', '0'}, "AGGREGATE is not supported for stored functions"),
    ERR_MAX_PREPARED_STMT_COUNT_REACHED(1461, new byte[]{'4', '2', '0', '0', '0'}, "Can't create more than "
            + "max_prepared_stmt_count statements (current value: %d)"),
    ERR_VIEW_RECURSIVE(1462, new byte[]{'H', 'Y', '0', '0', '0'}, "`%s`.`%s` contains view recursion"),
    ERR_NON_GROUPING_FIELD_USED(1463, new byte[]{'4', '2', '0', '0', '0'}, "Non-grouping field '%s' is used in %s "
            + "clause"),
    ERR_TABLE_CANT_HANDLE_SPKEYS(1464, new byte[]{'H', 'Y', '0', '0', '0'}, "The used table type doesn't support "
            + "SPATIAL indexes"),
    ERR_NO_TRIGGERS_ON_SYSTEM_SCHEMA(1465, new byte[]{'H', 'Y', '0', '0', '0'}, "Triggers can not be created on "
            + "system tables"),
    ERR_REMOVED_SPACES(1466, new byte[]{'H', 'Y', '0', '0', '0'}, "Leading spaces are removed from name '%s'"),
    ERR_AUTOINC_READ_FAILED(1467, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to read auto-increment value from "
            + "storage engine"),
    ERR_USERNAME(1468, new byte[]{'H', 'Y', '0', '0', '0'}, "user name"),
    ERR_HOSTNAME(1469, new byte[]{'H', 'Y', '0', '0', '0'}, "host name"),
    ERR_WRONG_STRING_LENGTH(1470, new byte[]{'H', 'Y', '0', '0', '0'}, "String '%s' is too long for %s (should be no "
            + "longer than %d)"),
    ERR_NON_INSERTABLE_TABLE(1471, new byte[]{'H', 'Y', '0', '0', '0'}, "The target table %s of the %s is not "
            + "insertable-into"),
    ERR_ADMIN_WRONG_MRG_TABLE(1472, new byte[]{'H', 'Y', '0', '0', '0'}, "Table '%s' is differently defined or of "
            + "non-MyISAM type or doesn't exist"),
    ERR_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT(1473, new byte[]{'H', 'Y', '0', '0', '0'}, "Too high level of nesting "
            + "for select"),
    ERR_NAME_BECOMES_EMPTY(1474, new byte[]{'H', 'Y', '0', '0', '0'}, "Name '%s' has become ''"),
    ERR_AMBIGUOUS_FIELD_TERM(1475, new byte[]{'H', 'Y', '0', '0', '0'}, "First character of the FIELDS TERMINATED "
            + "string is ambiguous; please use non-optional and non-empty FIELDS ENCLOSED BY"),
    ERR_FOREIGN_SERVER_EXISTS(1476, new byte[]{'H', 'Y', '0', '0', '0'}, "The foreign server, %s, you are trying to "
            + "create already exists."),
    ERR_FOREIGN_SERVER_DOESNT_EXIST(1477, new byte[]{'H', 'Y', '0', '0', '0'}, "The foreign server name you are "
            + "trying to reference does not exist. Data source error: %s"),
    ERR_ILLEGAL_HA_CREATE_OPTION(1478, new byte[]{'H', 'Y', '0', '0', '0'}, "Table storage engine '%s' does not "
            + "support the create option '%s'"),
    ERR_PARTITION_REQUIRES_VALUES_ERROR(1479, new byte[]{'H', 'Y', '0', '0', '0'}, "Syntax error: %s PARTITIONING "
            + "requires definition of VALUES %s for each partition"),
    ERR_PARTITION_WRONG_VALUES_ERROR(1480, new byte[]{'H', 'Y', '0', '0', '0'}, "Only %s PARTITIONING can use VALUES "
            + "%s in partition definition"),
    ERR_PARTITION_MAXVALUE_ERROR(1481, new byte[]{'H', 'Y', '0', '0', '0'}, "MAXVALUE can only be used in last "
            + "partition definition"),
    ERR_PARTITION_SUBPARTITION_ERROR(1482, new byte[]{'H', 'Y', '0', '0', '0'}, "Subpartitions can only be hash "
            + "partitions and by key"),
    ERR_PARTITION_SUBPART_MIX_ERROR(1483, new byte[]{'H', 'Y', '0', '0', '0'}, "Must define subpartitions on all "
            + "partitions if on one partition"),
    ERR_PARTITION_WRONG_NO_PART_ERROR(1484, new byte[]{'H', 'Y', '0', '0', '0'}, "Wrong number of partitions defined,"
            + " mismatch with previous setting"),
    ERR_PARTITION_WRONG_NO_SUBPART_ERROR(1485, new byte[]{'H', 'Y', '0', '0', '0'}, "Wrong number of subpartitions "
            + "defined, mismatch with previous setting"),
    ERR_CONST_EXPR_IN_PARTITION_FUNC_ERROR(1486, new byte[]{'H', 'Y', '0', '0', '0'}, "Constant/Random expression in "
            + "(sub)partitioning function is not allowed"),
    ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR(1486, new byte[]{'H', 'Y', '0', '0', '0'}, "Constant, random or "
            + "timezone-dependent expressions in (sub)partitioning function are not allowed"),
    ERR_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR(1487, new byte[]{'H', 'Y', '0', '0', '0'}, "Expression in RANGE/LIST "
            + "VALUES must be constant"),
    ERR_FIELD_NOT_FOUND_PART_ERROR(1488, new byte[]{'H', 'Y', '0', '0', '0'}, "Field in list of fields for partition "
            + "function not found in table"),
    ERR_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR(1489, new byte[]{'H', 'Y', '0', '0', '0'}, "List of fields is only allowed "
            + "in KEY partitions"),
    ERR_INCONSISTENT_PARTITION_INFO_ERROR(1490, new byte[]{'H', 'Y', '0', '0', '0'}, "The partition info in the frm "
            + "file is not consistent with what can be written into the frm file"),
    ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR(1491, new byte[]{'H', 'Y', '0', '0', '0'}, "The %s function returns the "
            + "wrong type"),
    ERR_PARTITIONS_MUST_BE_DEFINED_ERROR(1492, new byte[]{'H', 'Y', '0', '0', '0'}, "For %s partitions each partition"
            + " must be defined"),
    ERR_RANGE_NOT_INCREASING_ERROR(1493, new byte[]{'H', 'Y', '0', '0', '0'}, "VALUES LESS THAN value must be "
            + "strictly increasing for each partition"),
    ERR_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR(1494, new byte[]{'H', 'Y', '0', '0', '0'}, "VALUES value must be of same"
            + " type as partition function"),
    ERR_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR(1495, new byte[]{'H', 'Y', '0', '0', '0'}, "Multiple definition of same"
            + " constant in list partitioning"),
    ERR_PARTITION_ENTRY_ERROR(1496, new byte[]{'H', 'Y', '0', '0', '0'}, "Partitioning can not be used stand-alone in"
            + " query"),
    ERR_MIX_HANDLER_ERROR(1497, new byte[]{'H', 'Y', '0', '0', '0'}, "The mix of handlers in the partitions is not "
            + "allowed in this version of MariaDB"),
    ERR_PARTITION_NOT_DEFINED_ERROR(1498, new byte[]{'H', 'Y', '0', '0', '0'}, "For the partitioned engine it is "
            + "necessary to define all %s"),
    ERR_TOO_MANY_PARTITIONS_ERROR(1499, new byte[]{'H', 'Y', '0', '0', '0'}, "Too many partitions (including "
            + "subpartitions) were defined"),
    ERR_SUBPARTITION_ERROR(1500, new byte[]{'H', 'Y', '0', '0', '0'}, "It is only possible to mix RANGE/LIST "
            + "partitioning with HASH/KEY partitioning for subpartitioning"),
    ERR_CANT_CREATE_HANDLER_FILE(1501, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to create specific handler file"),
    ERR_BLOB_FIELD_IN_PART_FUNC_ERROR(1502, new byte[]{'H', 'Y', '0', '0', '0'}, "A BLOB field is not allowed in "
            + "partition function"),
    ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF(1503, new byte[]{'H', 'Y', '0', '0', '0'}, "A %s must include all columns in"
            + " the table's partitioning function"),
    ERR_NO_PARTS_ERROR(1504, new byte[]{'H', 'Y', '0', '0', '0'}, "Number of %s = 0 is not an allowed value"),
    ERR_PARTITION_MGMT_ON_NONPARTITIONED(1505, new byte[]{'H', 'Y', '0', '0', '0'}, "Partition management on a not "
            + "partitioned table is not possible"),
    ERR_FOREIGN_KEY_ON_PARTITIONED(1506, new byte[]{'H', 'Y', '0', '0', '0'}, "Foreign key clause is not yet "
            + "supported in conjunction with partitioning"),
    ERR_DROP_PARTITION_NON_EXISTENT(1507, new byte[]{'H', 'Y', '0', '0', '0'}, "Error in list of partitions to %s"),
    ERR_DROP_LAST_PARTITION(1508, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot remove all partitions, use DROP TABLE "
            + "instead"),
    ERR_COALESCE_ONLY_ON_HASH_PARTITION(1509, new byte[]{'H', 'Y', '0', '0', '0'}, "COALESCE PARTITION can only be "
            + "used on HASH/KEY partitions"),
    ERR_REORG_HASH_ONLY_ON_SAME_N(1510, new byte[]{'H', 'Y', '0', '0', '0'}, "REORGANIZE PARTITION can only be used "
            + "to reorganize partitions not to change their numbers"),
    ERR_REORG_NO_PARAM_ERROR(1511, new byte[]{'H', 'Y', '0', '0', '0'}, "REORGANIZE PARTITION without parameters can "
            + "only be used on auto-partitioned tables using HASH PARTITIONs"),
    ERR_ONLY_ON_RANGE_LIST_PARTITION(1512, new byte[]{'H', 'Y', '0', '0', '0'}, "%s PARTITION can only be used on "
            + "RANGE/LIST partitions"),
    ERR_ADD_PARTITION_SUBPART_ERROR(1513, new byte[]{'H', 'Y', '0', '0', '0'}, "Trying to Add partition(s) with wrong"
            + " number of subpartitions"),
    ERR_ADD_PARTITION_NO_NEW_PARTITION(1514, new byte[]{'H', 'Y', '0', '0', '0'}, "At least one partition must be "
            + "added"),
    ERR_COALESCE_PARTITION_NO_PARTITION(1515, new byte[]{'H', 'Y', '0', '0', '0'}, "At least one partition must be "
            + "coalesced"),
    ERR_REORG_PARTITION_NOT_EXIST(1516, new byte[]{'H', 'Y', '0', '0', '0'}, "More partitions to reorganize than "
            + "there are partitions"),
    ERR_SAME_NAME_PARTITION(1517, new byte[]{'H', 'Y', '0', '0', '0'}, "Duplicate partition name %s"),
    ERR_NO_BINLOG_ERROR(1518, new byte[]{'H', 'Y', '0', '0', '0'}, "It is not allowed to shut off binlog on this "
            + "command"),
    ERR_CONSECUTIVE_REORG_PARTITIONS(1519, new byte[]{'H', 'Y', '0', '0', '0'}, "When reorganizing a set of "
            + "partitions they must be in consecutive order"),
    ERR_REORG_OUTSIDE_RANGE(1520, new byte[]{'H', 'Y', '0', '0', '0'}, "Reorganize of range partitions cannot change "
            + "total ranges except for last partition where it can extend the range"),
    ERR_PARTITION_FUNCTION_FAILURE(1521, new byte[]{'H', 'Y', '0', '0', '0'}, "Partition function not supported in "
            + "this version for this handler"),
    ERR_PART_STATE_ERROR(1522, new byte[]{'H', 'Y', '0', '0', '0'}, "Partition state cannot be defined from "
            + "CREATE/ALTER TABLE"),
    ERR_LIMITED_PART_RANGE(1523, new byte[]{'H', 'Y', '0', '0', '0'}, "The %s handler only supports 32 bit integers "
            + "in VALUES"),
    ERR_PLUGIN_IS_NOT_LOADED(1524, new byte[]{'H', 'Y', '0', '0', '0'}, "Plugin '%s' is not loaded"),
    ERR_WRONG_VALUE(1525, new byte[]{'H', 'Y', '0', '0', '0'}, "Incorrect %s value: '%s'"),
    ERR_NO_PARTITION_FOR_GIVEN_VALUE(1526, new byte[]{'H', 'Y', '0', '0', '0'}, "Table has no partition for value %s"),
    ERR_FILEGROUP_OPTION_ONLY_ONCE(1527, new byte[]{'H', 'Y', '0', '0', '0'}, "It is not allowed to specify %s more "
            + "than once"),
    ERR_CREATE_FILEGROUP_FAILED(1528, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to create %s"),
    ERR_DROP_FILEGROUP_FAILED(1529, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to drop %s"),
    ERR_TABLESPACE_AUTO_EXTEND_ERROR(1530, new byte[]{'H', 'Y', '0', '0', '0'}, "The handler doesn't support "
            + "autoextend of tablespaces"),
    ERR_WRONG_SIZE_NUMBER(1531, new byte[]{'H', 'Y', '0', '0', '0'}, "A size parameter was incorrectly specified, "
            + "either number or on the form 10M"),
    ERR_SIZE_OVERFLOW_ERROR(1532, new byte[]{'H', 'Y', '0', '0', '0'}, "The size number was correct but we don't "
            + "allow the digit part to be more than 2 billion"),
    ERR_ALTER_FILEGROUP_FAILED(1533, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to alter: %s"),
    ERR_BINLOG_ROW_LOGGING_FAILED(1534, new byte[]{'H', 'Y', '0', '0', '0'}, "Writing one row to the row-based binary"
            + " log failed"),
    ERR_BINLOG_ROW_WRONG_TABLE_DEF(1535, new byte[]{'H', 'Y', '0', '0', '0'}, "Table definition on master and slave "
            + "does not match: %s"),
    ERR_BINLOG_ROW_RBR_TO_SBR(1536, new byte[]{'H', 'Y', '0', '0', '0'}, "Slave running with --log-slave-updates must"
            + " use row-based binary logging to be able to replicate row-based binary log events"),
    ERR_EVENT_ALREADY_EXISTS(1537, new byte[]{'H', 'Y', '0', '0', '0'}, "Event '%s' already exists"),
    ERR_EVENT_STORE_FAILED(1538, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to store event %s. Error code %d from "
            + "storage engine."),
    ERR_EVENT_DOES_NOT_EXIST(1539, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown event '%s'"),
    ERR_EVENT_CANT_ALTER(1540, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to alter event '%s'"),
    ERR_EVENT_DROP_FAILED(1541, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to drop %s"),
    ERR_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG(1542, new byte[]{'H', 'Y', '0', '0', '0'}, "INTERVAL is either not "
            + "positive or too big"),
    ERR_EVENT_ENDS_BEFORE_STARTS(1543, new byte[]{'H', 'Y', '0', '0', '0'}, "ENDS is either invalid or before STARTS"),
    ERR_EVENT_EXEC_TIME_IN_THE_PAST(1544, new byte[]{'H', 'Y', '0', '0', '0'}, "Event execution time is in the past. "
            + "Event has been disabled"),
    ERR_EVENT_OPEN_TABLE_FAILED(1545, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to open mysql.event"),
    ERR_EVENT_NEITHER_M_EXPR_NOR_M_AT(1546, new byte[]{'H', 'Y', '0', '0', '0'}, "No datetime expression provided"),
    ERR_COL_COUNT_DOESNT_MATCH_CORRUPTED(1547, new byte[]{'H', 'Y', '0', '0', '0'}, "Column count of mysql.%s is "
            + "wrong. Expected %d, found %d. The table is probably corrupted"),
    ERR_CANNOT_LOAD_FROM_TABLE(1548, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot load from mysql.%s. The table is "
            + "probably corrupted"),
    ERR_EVENT_CANNOT_DELETE(1549, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to delete the event from mysql.event"),
    ERR_EVENT_COMPILE_ERROR(1550, new byte[]{'H', 'Y', '0', '0', '0'}, "Error during compilation of event's body"),
    ERR_EVENT_SAME_NAME(1551, new byte[]{'H', 'Y', '0', '0', '0'}, "Same old and new event name"),
    ERR_EVENT_DATA_TOO_LONG(1552, new byte[]{'H', 'Y', '0', '0', '0'}, "Data for column '%s' too long"),
    ERR_DROP_INDEX_FK(1553, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot drop index '%s': needed in a foreign key "
            + "constraint"),
    ERR_WARN_DEPRECATED_SYNTAX_WITH_VER(1554, new byte[]{'H', 'Y', '0', '0', '0'}, "The syntax '%s' is deprecated and"
            + " will be removed in MariaDB %s. Please use %s instead"),
    ERR_CANT_WRITE_LOCK_LOG_TABLE(1555, new byte[]{'H', 'Y', '0', '0', '0'}, "You can't write-lock a log table. Only "
            + "read access is possible"),
    ERR_CANT_LOCK_LOG_TABLE(1556, new byte[]{'H', 'Y', '0', '0', '0'}, "You can't use locks with log tables."),
    ERR_FOREIGN_DUPLICATE_KEY(1557, new byte[]{'2', '3', '0', '0', '0'}, "Upholding foreign key constraints for table"
            + " '%s', entry '%s', key %d would lead to a duplicate entry"),
    ERR_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE(1558, new byte[]{'H', 'Y', '0', '0', '0'}, "Column count of mysql.%s is "
            + "wrong. Expected %d, found %d. Created with MariaDB %d, now running %d. Please use mysql_upgrade to fix "
            + "this "
            + "error."),
    ERR_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR(1559, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot switch out of the "
            + "row-based binary log format when the session has open temporary tables"),
    ERR_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_FORMAT(1560, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot change the "
            + "binary logging format inside a stored function or trigger"),
    ERR_UNUSED_13(1561, new byte[]{}, "You should never see it"),
    ERR_PARTITION_NO_TEMPORARY(1562, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot create temporary table with "
            + "partitions"),
    ERR_PARTITION_CONST_DOMAIN_ERROR(1563, new byte[]{'H', 'Y', '0', '0', '0'}, "Partition constant is out of "
            + "partition function domain"),
    ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED(1564, new byte[]{'H', 'Y', '0', '0', '0'}, "This partition function is not "
            + "allowed"),
    ERR_DDL_LOG_ERROR(1565, new byte[]{'H', 'Y', '0', '0', '0'}, "Error in DDL log"),
    ERR_NULL_IN_VALUES_LESS_THAN(1566, new byte[]{'H', 'Y', '0', '0', '0'}, "Not allowed to use NULL value in VALUES "
            + "LESS THAN"),
    ERR_WRONG_PARTITION_NAME(1567, new byte[]{'H', 'Y', '0', '0', '0'}, "Incorrect partition name"),
    ERR_CANT_CHANGE_TX_ISOLATION(1568, new byte[]{'2', '5', '0', '0', '1'}, "Transaction isolation level can't be "
            + "changed while a transaction is in progress"),
    ERR_DUP_ENTRY_AUTOINCREMENT_CASE(1569, new byte[]{'H', 'Y', '0', '0', '0'}, "ALTER TABLE causes auto_increment "
            + "resequencing, resulting in duplicate entry '%s' for key '%s'"),
    ERR_EVENT_MODIFY_QUEUE_ERROR(1570, new byte[]{'H', 'Y', '0', '0', '0'}, "Internal scheduler error %d"),
    ERR_EVENT_SET_VAR_ERROR(1571, new byte[]{'H', 'Y', '0', '0', '0'}, "Error during starting/stopping of the "
            + "scheduler. Error code %u"),
    ERR_PARTITION_MERGE_ERROR(1572, new byte[]{'H', 'Y', '0', '0', '0'}, "Engine cannot be used in partitioned tables"),
    ERR_CANT_ACTIVATE_LOG(1573, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot activate '%s' log"),
    ERR_RBR_NOT_AVAILABLE(1574, new byte[]{'H', 'Y', '0', '0', '0'}, "The server was not built with row-based "
            + "replication"),
    ERR_BASE64_DECODE_ERROR(1575, new byte[]{'H', 'Y', '0', '0', '0'}, "Decoding of base64 string failed"),
    ERR_EVENT_RECURSION_FORBIDDEN(1576, new byte[]{'H', 'Y', '0', '0', '0'}, "Recursion of EVENT DDL statements is "
            + "forbidden when body is present"),
    ERR_EVENTS_DB_ERROR(1577, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot proceed because system tables used by "
            + "Event Scheduler were found damaged at server start"),
    ERR_ONLY_INTEGERS_ALLOWED(1578, new byte[]{'H', 'Y', '0', '0', '0'}, "Only integers allowed as number here"),
    ERR_UNSUPORTED_LOG_ENGINE(1579, new byte[]{'H', 'Y', '0', '0', '0'}, "This storage engine cannot be used for log "
            + "tables"),
    ERR_BAD_LOG_STATEMENT(1580, new byte[]{'H', 'Y', '0', '0', '0'}, "You cannot '%s' a log table if logging is "
            + "enabled"),
    ERR_CANT_RENAME_LOG_TABLE(1581, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot rename '%s'. When logging enabled, "
            + "rename to/from log table must rename two tables: the log table to an archive table and another table "
            + "back to "
            + "'%s'"),
    ERR_WRONG_PARAMCOUNT_TO_NATIVE_FCT(1582, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect parameter count in the "
            + "call to native function '%s'"),
    ERR_WRONG_PARAMETERS_TO_NATIVE_FCT(1583, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect parameters in the call "
            + "to native function '%s'"),
    ERR_WRONG_PARAMETERS_TO_STORED_FCT(1584, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect parameters in the call "
            + "to stored function '%s'"),
    ERR_NATIVE_FCT_NAME_COLLISION(1585, new byte[]{'H', 'Y', '0', '0', '0'}, "This function '%s' has the same name as"
            + " a native function"),
    ERR_DUP_ENTRY_WITH_KEY_NAME(1586, new byte[]{'2', '3', '0', '0', '0'}, "Duplicate entry '%s' for key '%s'"),
    ERR_BINLOG_PURGE_EMFILE(1587, new byte[]{'H', 'Y', '0', '0', '0'}, "Too many files opened, please execute the "
            + "command again"),
    ERR_EVENT_CANNOT_CREATE_IN_THE_PAST(1588, new byte[]{'H', 'Y', '0', '0', '0'}, "Event execution time is in the "
            + "past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation."),
    ERR_EVENT_CANNOT_ALTER_IN_THE_PAST(1589, new byte[]{'H', 'Y', '0', '0', '0'}, "Event execution time is in the "
            + "past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation."),
    ERR_SLAVE_INCIDENT(1590, new byte[]{'H', 'Y', '0', '0', '0'}, "The incident %s occurred on the master. Message: %s"),
    ERR_NO_PARTITION_FOR_GIVEN_VALUE_SILENT(1591, new byte[]{'H', 'Y', '0', '0', '0'}, "Table has no partition for "
            + "some existing values"),
    ERR_BINLOG_UNSAFE_STATEMENT(1592, new byte[]{'H', 'Y', '0', '0', '0'}, "Unsafe statement written to the binary "
            + "log using statement format since BINLOG_FORMAT = STATEMENT. %s"),
    ERR_SLAVE_FATAL_ERROR(1593, new byte[]{'H', 'Y', '0', '0', '0'}, "Fatal error: %s"),
    ERR_SLAVE_RELAY_LOG_READ_FAILURE(1594, new byte[]{'H', 'Y', '0', '0', '0'}, "Relay log read failure: %s"),
    ERR_SLAVE_RELAY_LOG_WRITE_FAILURE(1595, new byte[]{'H', 'Y', '0', '0', '0'}, "Relay log write failure: %s"),
    ERR_SLAVE_CREATE_EVENT_FAILURE(1596, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to create %s"),
    ERR_SLAVE_MASTER_COM_FAILURE(1597, new byte[]{'H', 'Y', '0', '0', '0'}, "Master command %s failed: %s"),
    ERR_BINLOG_LOGGING_IMPOSSIBLE(1598, new byte[]{'H', 'Y', '0', '0', '0'}, "Binary logging not possible. Message: "
            + "%s"),
    ERR_VIEW_NO_CREATION_CTX(1599, new byte[]{'H', 'Y', '0', '0', '0'}, "View `%s`.`%s` has no creation context"),
    ERR_VIEW_INVALID_CREATION_CTX(1600, new byte[]{'H', 'Y', '0', '0', '0'}, "Creation context of view `%s`.`%s' is "
            + "invalid"),
    ERR_SR_INVALID_CREATION_CTX(1601, new byte[]{'H', 'Y', '0', '0', '0'}, "Creation context of stored routine `%s`"
            + ".`%s` is invalid"),
    ERR_TRG_CORRUPTED_FILE(1602, new byte[]{'H', 'Y', '0', '0', '0'}, "Corrupted TRG file for table `%s`.`%s`"),
    ERR_TRG_NO_CREATION_CTX(1603, new byte[]{'H', 'Y', '0', '0', '0'}, "Triggers for table `%s`.`%s` have no creation"
            + " context"),
    ERR_TRG_INVALID_CREATION_CTX(1604, new byte[]{'H', 'Y', '0', '0', '0'}, "Trigger creation context of table `%s`"
            + ".`%s` is invalid"),
    ERR_EVENT_INVALID_CREATION_CTX(1605, new byte[]{'H', 'Y', '0', '0', '0'}, "Creation context of event `%s`.`%s` is"
            + " invalid"),
    ERR_TRG_CANT_OPEN_TABLE(1606, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot open table for trigger `%s`.`%s`"),
    ERR_CANT_CREATE_SROUTINE(1607, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot create stored routine `%s`. Check "
            + "warnings"),
    ERR_UNUSED_11(1608, new byte[]{}, "You should never see it"),
    ERR_NO_FORMAT_DESCRIPTION_EVENT_BEFORE_BINLOG_STATEMENT(1609, new byte[]{'H', 'Y', '0', '0', '0'}, "The BINLOG "
            + "statement of type `%s` was not preceded by a format description BINLOG statement."),
    ERR_SLAVE_CORRUPT_EVENT(1610, new byte[]{'H', 'Y', '0', '0', '0'}, "Corrupted replication event was detected"),
    ERR_LOAD_DATA_INVALID_COLUMN(1611, new byte[]{'H', 'Y', '0', '0', '0'}, "Invalid column reference (%s) in LOAD "
            + "DATA"),
    ERR_LOG_PURGE_NO_FILE(1612, new byte[]{'H', 'Y', '0', '0', '0'}, "Being purged log %s was not found"),
    ERR_XA_RBTIMEOUT(1613, new byte[]{'X', 'A', '1', '0', '6'}, "XA_RBTIMEOUT: Transaction branch was rolled back: "
            + "took too long"),
    ERR_XA_RBDEADLOCK(1614, new byte[]{'X', 'A', '1', '0', '2'}, "XA_RBDEADLOCK: Transaction branch was rolled back: "
            + "deadlock was detected"),
    ERR_NEED_REPREPARE(1615, new byte[]{'H', 'Y', '0', '0', '0'}, "Prepared statement needs to be re-prepared"),
    ERR_DELAYED_NOT_SUPPORTED(1616, new byte[]{'H', 'Y', '0', '0', '0'}, "DELAYED option not supported for table '%s'"),
    WARN_NO_MASTER_INF(1617, new byte[]{'H', 'Y', '0', '0', '0'}, "The master info structure does not exist"),
    WARN_OPTION_IGNORED(1618, new byte[]{'H', 'Y', '0', '0', '0'}, "<%s> option ignored"),
    WARN_PLUGIN_DELETE_BUILTIN(1619, new byte[]{'H', 'Y', '0', '0', '0'}, "Built-in plugins cannot be deleted"),
    WARN_PLUGIN_BUSY(1620, new byte[]{'H', 'Y', '0', '0', '0'}, "Plugin is busy and will be uninstalled on shutdown"),
    ERR_VARIABLE_IS_READONLY(1621, new byte[]{'H', 'Y', '0', '0', '0'}, "Variable '%s' is a read only variable"),
    ERR_WARN_ENGINE_TRANSACTION_ROLLBACK(1622, new byte[]{'H', 'Y', '0', '0', '0'}, "Storage engine %s does not "
            + "support rollback for this statement. Transaction rolled back and must be restarted"),
    ERR_SLAVE_HEARTBEAT_FAILURE(1623, new byte[]{'H', 'Y', '0', '0', '0'}, "Unexpected master's heartbeat data: %s"),
    ERR_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE(1624, new byte[]{'H', 'Y', '0', '0', '0'}, "The requested value for the "
            + "heartbeat period is either negative or exceeds the maximum allowed (%s seconds)."),
    ERR_UNUSED_14(1625, new byte[]{}, "You should never see it"),
    ERR_CONFLICT_FN_PARSE_ERROR(1626, new byte[]{'H', 'Y', '0', '0', '0'}, "Error in parsing conflict function. "
            + "Message: %s"),
    ERR_EXCEPTIONS_WRITE_ERROR(1627, new byte[]{'H', 'Y', '0', '0', '0'}, "Write to exceptions table failed. Message:"
            + " %s"),
    ERR_TOO_LONG_TABLE_COMMENT(1628, new byte[]{'H', 'Y', '0', '0', '0'}, "Comment for table '%s' is too long (max = "
            + "%d)"),
    ERR_TOO_LONG_FIELD_COMMENT(1629, new byte[]{'H', 'Y', '0', '0', '0'}, "Comment for field '%s' is too long (max = "
            + "%d)"),
    ERR_FUNC_INEXISTENT_NAME_COLLISION(1630, new byte[]{'4', '2', '0', '0', '0'}, "FUNCTION %s does not exist. Check "
            + "the 'Function Name Parsing and Resolution' section in the Reference Manual"),
    ERR_DATABASE_NAME(1631, new byte[]{'H', 'Y', '0', '0', '0'}, "Database"),
    ERR_TABLE_NAME(1632, new byte[]{'H', 'Y', '0', '0', '0'}, "Table"),
    ERR_PARTITION_NAME(1633, new byte[]{'H', 'Y', '0', '0', '0'}, "Partition"),
    ERR_SUBPARTITION_NAME(1634, new byte[]{'H', 'Y', '0', '0', '0'}, "Subpartition"),
    ERR_TEMPORARY_NAME(1635, new byte[]{'H', 'Y', '0', '0', '0'}, "Temporary"),
    ERR_RENAMED_NAME(1636, new byte[]{'H', 'Y', '0', '0', '0'}, "Renamed"),
    ERR_TOO_MANY_CONCURRENT_TRXS(1637, new byte[]{'H', 'Y', '0', '0', '0'}, "Too many active concurrent transactions"),
    WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED(1638, new byte[]{'H', 'Y', '0', '0', '0'}, "Non-ASCII separator "
            + "arguments are not fully supported"),
    ERR_DEBUG_SYNC_TIMEOUT(1639, new byte[]{'H', 'Y', '0', '0', '0'}, "debug sync point wait timed out"),
    ERR_DEBUG_SYNC_HIT_LIMIT(1640, new byte[]{'H', 'Y', '0', '0', '0'}, "debug sync point hit limit reached"),
    ERR_DUP_SIGNAL_SET(1641, new byte[]{'4', '2', '0', '0', '0'}, "Duplicate condition information item '%s'"),
    ERR_SIGNAL_WARN(1642, new byte[]{'0', '1', '0', '0', '0'}, "Unhandled user-defined warning condition"),
    ERR_SIGNAL_NOT_FOUND(1643, new byte[]{'0', '2', '0', '0', '0'}, "Unhandled user-defined not found condition"),
    ERR_SIGNAL_EXCEPTION(1644, new byte[]{'H', 'Y', '0', '0', '0'}, "Unhandled user-defined exception condition"),
    ERR_RESIGNAL_WITHOUT_ACTIVE_HANDLER(1645, new byte[]{'0', 'K', '0', '0', '0'}, "RESIGNAL when handler not active"),
    ERR_SIGNAL_BAD_CONDITION_TYPE(1646, new byte[]{'H', 'Y', '0', '0', '0'}, "SIGNAL/RESIGNAL can only use a "
            + "CONDITION defined with SQLSTATE"),
    WARN_COND_ITEM_TRUNCATED(1647, new byte[]{'H', 'Y', '0', '0', '0'}, "Data truncated for condition item '%s'"),
    ERR_COND_ITEM_TOO_LONG(1648, new byte[]{'H', 'Y', '0', '0', '0'}, "Data too long for condition item '%s'"),
    ERR_UNKNOWN_LOCALE(1649, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown locale: '%s'"),
    ERR_SLAVE_IGNORE_SERVER_IDS(1650, new byte[]{'H', 'Y', '0', '0', '0'}, "The requested server id %d clashes with "
            + "the slave startup option --replicate-same-server-id"),
    ERR_QUERY_CACHE_DISABLED(1651, new byte[]{'H', 'Y', '0', '0', '0'}, "Query cache is disabled; restart the server "
            + "with query_cache_type=1 to enable it"),
    ERR_SAME_NAME_PARTITION_FIELD(1652, new byte[]{'H', 'Y', '0', '0', '0'}, "Duplicate partition field name '%s'"),
    ERR_PARTITION_COLUMN_LIST_ERROR(1653, new byte[]{'H', 'Y', '0', '0', '0'}, "Inconsistency in usage of column "
            + "lists for partitioning"),
    ERR_WRONG_TYPE_COLUMN_VALUE_ERROR(1654, new byte[]{'H', 'Y', '0', '0', '0'}, "Partition column values of "
            + "incorrect type"),
    ERR_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR(1655, new byte[]{'H', 'Y', '0', '0', '0'}, "Too many fields in '%s'"),
    ERR_MAXVALUE_IN_VALUES_IN(1656, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot use MAXVALUE as value in VALUES IN"),
    ERR_TOO_MANY_VALUES_ERROR(1657, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot have more than one value for this "
            + "type of %s partitioning"),
    ERR_ROW_SINGLE_PARTITION_FIELD_ERROR(1658, new byte[]{'H', 'Y', '0', '0', '0'}, "Row expressions in VALUES IN "
            + "only allowed for multi-field column partitioning"),
    ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD(1659, new byte[]{'H', 'Y', '0', '0', '0'}, "Field '%s' is of a not "
            + "allowed type for this type of partitioning"),
    ERR_PARTITION_FIELDS_TOO_LONG(1660, new byte[]{'H', 'Y', '0', '0', '0'}, "The total length of the partitioning "
            + "fields is too large"),
    ERR_BINLOG_ROW_ENGINE_AND_STMT_ENGINE(1661, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot execute statement: "
            + "impossible to write to binary log since both row-incapable engines and statement-incapable engines are "
            + "involved"
            + "."),
    ERR_BINLOG_ROW_MODE_AND_STMT_ENGINE(1662, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot execute statement: "
            + "impossible to write to binary log since BINLOG_FORMAT = ROW and at least one table uses a storage engine"
            + " "
            + "limited to statement-based logging."),
    ERR_BINLOG_UNSAFE_AND_STMT_ENGINE(1663, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot execute statement: "
            + "impossible to write to binary log since statement is unsafe, storage engine is limited to "
            + "statement-based "
            + "logging, and BINLOG_FORMAT = MIXED. %s"),
    ERR_BINLOG_ROW_INJECTION_AND_STMT_ENGINE(1664, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot execute statement: "
            + "impossible to write to binary log since statement is in row format and at least one table uses a storage"
            + " engine"
            + " limited to statement-based logging."),
    ERR_BINLOG_STMT_MODE_AND_ROW_ENGINE(1665, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot execute statement: "
            + "impossible to write to binary log since BINLOG_FORMAT = STATEMENT and at least one table uses a storage "
            + "engine "
            + "limited to row-based logging.%s"),
    ERR_BINLOG_ROW_INJECTION_AND_STMT_MODE(1666, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot execute statement: "
            + "impossible to write to binary log since statement is in row format and BINLOG_FORMAT = STATEMENT."),
    ERR_BINLOG_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE(1667, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot execute "
            + "statement: impossible to write to binary log since more than one engine is involved and at least one "
            + "engine is "
            + "self-logging."),
    ERR_BINLOG_UNSAFE_LIMIT(1668, new byte[]{'H', 'Y', '0', '0', '0'}, "The statement is unsafe because it uses a "
            + "LIMIT clause. This is unsafe because the set of rows included cannot be predicted."),
    ERR_BINLOG_UNSAFE_INSERT_DELAYED(1669, new byte[]{'H', 'Y', '0', '0', '0'}, "The statement is unsafe because it "
            + "uses INSERT DELAYED. This is unsafe because the times when rows are inserted cannot be predicted."),
    ERR_BINLOG_UNSAFE_SYSTEM_TABLE(1670, new byte[]{'H', 'Y', '0', '0', '0'}, "The statement is unsafe because it "
            + "uses the general log, slow query log, or performance_schema table(s). This is unsafe because system "
            + "tables may "
            + "differ on slaves."),
    ERR_BINLOG_UNSAFE_AUTOINC_COLUMNS(1671, new byte[]{'H', 'Y', '0', '0', '0'}, "Statement is unsafe because it "
            + "invokes a trigger or a stored function that inserts into an AUTO_INCREMENT column. Inserted values "
            + "cannot be "
            + "logged correctly."),
    ERR_BINLOG_UNSAFE_UDF(1672, new byte[]{'H', 'Y', '0', '0', '0'}, "Statement is unsafe because it uses a UDF which"
            + " may not return the same value on the slave."),
    ERR_BINLOG_UNSAFE_SYSTEM_VARIABLE(1673, new byte[]{'H', 'Y', '0', '0', '0'}, "Statement is unsafe because it uses"
            + " a system variable that may have a different value on the slave."),
    ERR_BINLOG_UNSAFE_SYSTEM_FUNCTION(1674, new byte[]{'H', 'Y', '0', '0', '0'}, "Statement is unsafe because it uses"
            + " a system function that may return a different value on the slave."),
    ERR_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS(1675, new byte[]{'H', 'Y', '0', '0', '0'}, "Statement is unsafe because it"
            + " accesses a non-transactional table after accessing a transactional table within the same transaction."),
    ERR_MESSAGE_AND_STATEMENT(1676, new byte[]{'H', 'Y', '0', '0', '0'}, "%s Statement: %s"),
    ERR_SLAVE_CONVERSION_FAILED(1677, new byte[]{'H', 'Y', '0', '0', '0'}, "Column %d of table '%s.%s' cannot be "
            + "converted from type '%s' to type '%s'"),
    ERR_SLAVE_CANT_CREATE_CONVERSION(1678, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't create conversion table for "
            + "table '%s.%s'"),
    ERR_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_FORMAT(1679, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot modify "
            + "@@session.binlog_format inside a transaction"),
    ERR_PATH_LENGTH(1680, new byte[]{'H', 'Y', '0', '0', '0'}, "The path specified for %s is too long."),
    ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT(1681, new byte[]{'H', 'Y', '0', '0', '0'}, "'%s' is deprecated and will"
            + " be removed in a future release."),
    ERR_WRONG_NATIVE_TABLE_STRUCTURE(1682, new byte[]{'H', 'Y', '0', '0', '0'}, "Native table '%s'.'%s' has the wrong"
            + " structure"),
    ERR_WRONG_PERFSCHEMA_USAGE(1683, new byte[]{'H', 'Y', '0', '0', '0'}, "Invalid performance_schema usage."),
    ERR_WARN_I_S_SKIPPED_TABLE(1684, new byte[]{'H', 'Y', '0', '0', '0'}, "Table '%s'.'%s' was skipped since its "
            + "definition is being modified by concurrent DDL statement"),
    ERR_INSIDE_TRANSACTION_PREVENTS_SWITCH_BINLOG_DIRECT(1685, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot modify "
            + "@@session.binlog_direct_non_transactional_updates inside a transaction"),
    ERR_STORED_FUNCTION_PREVENTS_SWITCH_BINLOG_DIRECT(1686, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot change the "
            + "binlog direct flag inside a stored function or trigger"),
    ERR_SPATIAL_MUST_HAVE_GEOM_COL(1687, new byte[]{'4', '2', '0', '0', '0'}, "A SPATIAL index may only contain a "
            + "geometrical type column"),
    ERR_TOO_LONG_INDEX_COMMENT(1688, new byte[]{'H', 'Y', '0', '0', '0'}, "Comment for index '%s' is too long (max = "
            + "%d)"),
    ERR_LOCK_ABORTED(1689, new byte[]{'H', 'Y', '0', '0', '0'}, "Wait on a lock was aborted due to a pending "
            + "exclusive lock"),
    ERR_DATA_OUT_OF_RANGE(1690, new byte[]{'2', '2', '0', '0', '3'}, "%s value is out of range in '%s'"),
    ERR_WRONG_SPVAR_TYPE_IN_LIMIT(1691, new byte[]{'H', 'Y', '0', '0', '0'}, "A variable of a non-integer based type "
            + "in LIMIT clause"),
    ERR_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE(1692, new byte[]{'H', 'Y', '0', '0', '0'}, "Mixing "
            + "self-logging and non-self-logging engines in a statement is unsafe."),
    ERR_BINLOG_UNSAFE_MIXED_STATEMENT(1693, new byte[]{'H', 'Y', '0', '0', '0'}, "Statement accesses nontransactional"
            + " table as well as transactional or temporary table, and writes to any of them."),
    ERR_INSIDE_TRANSACTION_PREVENTS_SWITCH_SQL_LOG_BIN(1694, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot modify "
            + "@@session.sql_log_bin inside a transaction"),
    ERR_STORED_FUNCTION_PREVENTS_SWITCH_SQL_LOG_BIN(1695, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot change the "
            + "sql_log_bin inside a stored function or trigger"),
    ERR_FAILED_READ_FROM_PAR_FILE(1696, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed to read from the .par file"),
    ERR_VALUES_IS_NOT_INT_TYPE_ERROR(1697, new byte[]{'H', 'Y', '0', '0', '0'}, "VALUES value for partition '%s' must"
            + " have type INT"),
    ERR_ACCESS_DENIED_NO_PASSWORD_ERROR(1698, new byte[]{'2', '8', '0', '0', '0'}, "Access denied for user '%s'@'%s'"),
    ERR_SET_PASSWORD_AUTH_PLUGIN(1699, new byte[]{'H', 'Y', '0', '0', '0'}, "SET PASSWORD has no significance for "
            + "users authenticating via plugins"),
    ERR_GRANT_PLUGIN_USER_EXISTS(1700, new byte[]{'H', 'Y', '0', '0', '0'}, "GRANT with IDENTIFIED WITH is illegal "
            + "because the user %-.*s already exists"),
    ERR_TRUNCATE_ILLEGAL_FK(1701, new byte[]{'4', '2', '0', '0', '0'}, "Cannot truncate a table referenced in a "
            + "foreign key constraint (%s)"),
    ERR_PLUGIN_IS_PERMANENT(1702, new byte[]{'H', 'Y', '0', '0', '0'}, "Plugin '%s' is force_plus_permanent and can "
            + "not be unloaded"),
    ERR_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN(1703, new byte[]{'H', 'Y', '0', '0', '0'}, "The requested value for "
            + "the heartbeat period is less than 1 millisecond. The value is reset to 0, meaning that heartbeating will"
            + " "
            + "effectively be disabled."),
    ERR_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX(1704, new byte[]{'H', 'Y', '0', '0', '0'}, "The requested value for "
            + "the heartbeat period exceeds the value of slave_net_timeout seconds. A sensible value for the period "
            + "should be "
            + "less than the timeout."),
    ERR_STMT_CACHE_FULL(1705, new byte[]{'H', 'Y', '0', '0', '0'}, "Multi-row statements required more than "
            + "'max_binlog_stmt_cache_size' bytes of storage; increase this mysqld variable and try again"),
    ERR_MULTI_UPDATE_KEY_CONFLICT(1706, new byte[]{'H', 'Y', '0', '0', '0'}, "Primary key/partition key update is not"
            + " allowed since the table is updated both as '%s' and '%s'."),
    ERR_TABLE_NEEDS_REBUILD(1707, new byte[]{'H', 'Y', '0', '0', '0'}, "Table rebuild required. Please do \"ALTER "
            + "TABLE `%s` FORCE\" or dump/reload to fix it!"),
    WARN_OPTION_BELOW_LIMIT(1708, new byte[]{'H', 'Y', '0', '0', '0'}, "The value of '%s' should be no less than the "
            + "value of '%s'"),
    ERR_INDEX_COLUMN_TOO_LONG(1709, new byte[]{'H', 'Y', '0', '0', '0'}, "Index column size too large. The maximum "
            + "column size is %d bytes."),
    ERR_ERROR_IN_TRIGGER_BODY(1710, new byte[]{'H', 'Y', '0', '0', '0'}, "Trigger '%s' has an error in its body: '%s'"),
    ERR_ERROR_IN_UNKNOWN_TRIGGER_BODY(1711, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown trigger has an error in its"
            + " body: '%s'"),
    ERR_INDEX_CORRUPT(1712, new byte[]{'H', 'Y', '0', '0', '0'}, "Index %s is corrupted"),
    ERR_UNDO_RECORD_TOO_BIG(1713, new byte[]{'H', 'Y', '0', '0', '0'}, "Undo log record is too big."),
    ERR_BINLOG_UNSAFE_INSERT_IGNORE_SELECT(1714, new byte[]{'H', 'Y', '0', '0', '0'}, "INSERT IGNORE... SELECT is "
            + "unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are "
            + "ignored. "
            + "This order cannot be predicted and may differ on master and the slave."),
    ERR_BINLOG_UNSAFE_INSERT_SELECT_UPDATE(1715, new byte[]{'H', 'Y', '0', '0', '0'}, "INSERT... SELECT... ON "
            + "DUPLICATE KEY UPDATE is unsafe because the order in which rows are retrieved by the SELECT determines "
            + "which (if"
            + " any) rows are updated. This order cannot be predicted and may differ on master and the slave."),
    ERR_BINLOG_UNSAFE_REPLACE_SELECT(1716, new byte[]{'H', 'Y', '0', '0', '0'}, "REPLACE... SELECT is unsafe because "
            + "the order in which rows are retrieved by the SELECT determines which (if any) rows are replaced. This "
            + "order "
            + "cannot be predicted and may differ on master and the slave."),
    ERR_BINLOG_UNSAFE_CREATE_IGNORE_SELECT(1717, new byte[]{'H', 'Y', '0', '0', '0'}, "CREATE... IGNORE SELECT is "
            + "unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are "
            + "ignored. "
            + "This order cannot be predicted and may differ on master and the slave."),
    ERR_BINLOG_UNSAFE_CREATE_REPLACE_SELECT(1718, new byte[]{'H', 'Y', '0', '0', '0'}, "CREATE... REPLACE SELECT is "
            + "unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are "
            + "replaced."
            + " This order cannot be predicted and may differ on master and the slave."),
    ERR_BINLOG_UNSAFE_UPDATE_IGNORE(1719, new byte[]{'H', 'Y', '0', '0', '0'}, "UPDATE IGNORE is unsafe because the "
            + "order in which rows are updated determines which (if any) rows are ignored. This order cannot be "
            + "predicted and "
            + "may differ on master and the slave."),
    ERR_UNUSED_15(1720, new byte[]{}, "You should never see it"),
    ERR_UNUSED_16(1721, new byte[]{}, "You should never see it"),
    ERR_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT(1722, new byte[]{'H', 'Y', '0', '0', '0'}, "Statements writing to a table "
            + "with an auto-increment column after selecting from another table are unsafe because the order in which "
            + "rows are"
            + " retrieved determines what (if any) rows will be written. This order cannot be predicted and may differ "
            + "on "
            + "master and the slave."),
    ERR_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC(1723, new byte[]{'H', 'Y', '0', '0', '0'}, "CREATE TABLE... SELECT... on "
            + "a table with an auto-increment column is unsafe because the order in which rows are retrieved by the "
            + "SELECT "
            + "determines which (if any) rows are inserted. This order cannot be predicted and may differ on master and"
            + " the "
            + "slave."),
    ERR_BINLOG_UNSAFE_INSERT_TWO_KEYS(1724, new byte[]{'H', 'Y', '0', '0', '0'}, "INSERT... ON DUPLICATE KEY UPDATE "
            + "on a table with more than one UNIQUE KEY is unsafe"),
    ERR_TABLE_IN_FK_CHECK(1725, new byte[]{'H', 'Y', '0', '0', '0'}, "Table is being used in foreign key check."),
    ERR_UNSUPPORTED_ENGINE(1726, new byte[]{'H', 'Y', '0', '0', '0'}, "Storage engine '%s' does not support system "
            + "tables. [%s.%s]"),
    ERR_BINLOG_UNSAFE_AUTOINC_NOT_FIRST(1727, new byte[]{'H', 'Y', '0', '0', '0'}, "INSERT into autoincrement field "
            + "which is not the first part in the composed primary key is unsafe."),
    ERR_CANNOT_LOAD_FROM_TABLE_V2(1728, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot load from %s.%s. The table is "
            + "probably corrupted"),
    ERR_MASTER_DELAY_VALUE_OUT_OF_RANGE(1729, new byte[]{'H', 'Y', '0', '0', '0'}, "The requested value %s for the "
            + "master delay exceeds the maximum %u"),
    ERR_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT(1730, new byte[]{'H', 'Y', '0', '0', '0'}, "Only "
            + "Format_description_log_event and row events are allowed in BINLOG statements (but %s was provided"),
    ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION(1731, new byte[]{'H', 'Y', '0', '0', '0'}, "Non matching attribute '%s' "
            + "between partition and table"),
    ERR_PARTITION_EXCHANGE_PART_TABLE(1732, new byte[]{'H', 'Y', '0', '0', '0'}, "Table to exchange with partition is"
            + " partitioned: '%s'"),
    ERR_PARTITION_EXCHANGE_TEMP_TABLE(1733, new byte[]{'H', 'Y', '0', '0', '0'}, "Table to exchange with partition is"
            + " temporary: '%s'"),
    ERR_PARTITION_INSTEAD_OF_SUBPARTITION(1734, new byte[]{'H', 'Y', '0', '0', '0'}, "Subpartitioned table, use "
            + "subpartition instead of partition"),
    ERR_UNKNOWN_PARTITION(1735, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown partition '%s' in table '%s'"),
    ERR_TABLES_DIFFERENT_METADATA(1736, new byte[]{'H', 'Y', '0', '0', '0'}, "Tables have different definitions"),
    ERR_ROW_DOES_NOT_MATCH_PARTITION(1737, new byte[]{'H', 'Y', '0', '0', '0'}, "Found a row that does not match the "
            + "partition"),
    ERR_BINLOG_CACHE_SIZE_GREATER_THAN_MAX(1738, new byte[]{'H', 'Y', '0', '0', '0'}, "Option binlog_cache_size (%d)"
            + " is greater than max_binlog_cache_size (%d); setting binlog_cache_size equal to max_binlog_cache_size."),
    ERR_WARN_INDEX_NOT_APPLICABLE(1739, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot use %s access on index '%s' due "
            + "to type or collation conversion on field '%s'"),
    ERR_PARTITION_EXCHANGE_FOREIGN_KEY(1740, new byte[]{'H', 'Y', '0', '0', '0'}, "Table to exchange with partition "
            + "has foreign key references: '%s'"),
    ERR_NO_SUCH_KEY_VALUE(1741, new byte[]{'H', 'Y', '0', '0', '0'}, "Key value '%s' was not found in table '%s.%s'"),
    ERR_RPL_INFO_DATA_TOO_LONG(1742, new byte[]{'H', 'Y', '0', '0', '0'}, "Data for column '%s' too long"),
    ERR_NETWORK_READ_EVENT_CHECKSUM_FAILURE(1743, new byte[]{'H', 'Y', '0', '0', '0'}, "Replication event checksum "
            + "verification failed while reading from network."),
    ERR_BINLOG_READ_EVENT_CHECKSUM_FAILURE(1744, new byte[]{'H', 'Y', '0', '0', '0'}, "Replication event checksum "
            + "verification failed while reading from a log file."),
    ERR_BINLOG_STMT_CACHE_SIZE_GREATER_THAN_MAX(1745, new byte[]{'H', 'Y', '0', '0', '0'}, "Option "
            + "binlog_stmt_cache_size (%d) is greater than max_binlog_stmt_cache_size (%d); setting "
            + "binlog_stmt_cache_size "
            + "equal to max_binlog_stmt_cache_size."),
    ERR_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT(1746, new byte[]{'H', 'Y', '0', '0', '0'}, "Can't update table '%s' "
            + "while '%s' is being created."),
    ERR_PARTITION_CLAUSE_ON_NONPARTITIONED(1747, new byte[]{'H', 'Y', '0', '0', '0'}, "PARTITION () clause on non "
            + "partitioned table"),
    ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET(1748, new byte[]{'H', 'Y', '0', '0', '0'}, "Found a row not matching "
            + "the given partition set"),
    ERR_NO_SUCH_PARTITION(1749, new byte[]{'H', 'Y', '0', '0', '0'}, "partition '%s' doesn't exist"),
    ERR_CHANGE_RPL_INFO_REPOSITORY_FAILURE(1750, new byte[]{'H', 'Y', '0', '0', '0'}, "Failure while changing the "
            + "type of replication repository: %s."),
    ERR_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE(1751, new byte[]{'H', 'Y', '0', '0', '0'}, "The "
            + "creation of some temporary tables could not be rolled back."),
    ERR_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE(1752, new byte[]{'H', 'Y', '0', '0', '0'}, "Some "
            + "temporary tables were dropped, but these operations could not be rolled back."),
    ERR_MTS_FEATURE_IS_NOT_SUPPORTED(1753, new byte[]{'H', 'Y', '0', '0', '0'}, "%s is not supported in "
            + "multi-threaded slave mode. %s"),
    ERR_MTS_UPDATED_DBS_GREATER_MAX(1754, new byte[]{'H', 'Y', '0', '0', '0'}, "The number of modified databases "
            + "exceeds the maximum %d; the database names will not be included in the replication event metadata."),
    ERR_GTID_UNSAFE_CREATE_DROP_TEMPORARY_TABLE_IN_TRANSACTION(1787, new byte[]{'H', 'Y', '0', '0', '0'}, "When "
            + "@@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1, the statements CREATE TEMPORARY TABLE and DROP TEMPORARY TABLE "
            + "can be "
            + "executed in a non-transactional context only, and require that AUTOCOMMIT = 1."),
    ERR_GTID_MODE_CAN_ONLY_CHANGE_ONE_STEP_AT_A_TIME(1788, new byte[]{'H', 'Y', '0', '0', '0'}, "The value of "
            + "@@GLOBAL.GTID_MODE can only change one step at a time: OFF <-> UPGRADE_STEP_1 <-> UPGRADE_STEP_2 <-> ON."
            + " Also "
            + "note that this value must be stepped up or down simultaneously on all servers; see the Manual for "
            + "instructions"
            + "."),
    ERR_MTS_RECOVERY_FAILURE(1803, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot recover after SLAVE errored out in "
            + "parallel execution mode. Additional error messages can be found in the MySQL error log."),
    ERR_MTS_RESET_WORKERS(1804, new byte[]{'H', 'Y', '0', '0', '0'}, "Cannot clean up worker info tables. Additional "
            + "error messages can be found in the MySQL error log."),
    ERR_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2(1805, new byte[]{'H', 'Y', '0', '0', '0'}, "Column count of %s.%s is "
            + "wrong. Expected %d, found %d. The table is probably corrupted"),
    ERR_SLAVE_SILENT_RETRY_TRANSACTION(1806, new byte[]{'H', 'Y', '0', '0', '0'}, "Slave must silently retry current "
            + "transaction"),
    ERR_DISCARD_FK_CHECKS_RUNNING(1807, new byte[]{'H', 'Y', '0', '0', '0'}, "There is a foreign key check running on"
            + " table '%s'. Cannot discard the table."),
    ERR_TABLE_SCHEMA_MISMATCH(1808, new byte[]{'H', 'Y', '0', '0', '0'}, "Schema mismatch (%s"),
    ERR_TABLE_IN_SYSTEM_TABLESPACE(1809, new byte[]{'H', 'Y', '0', '0', '0'}, "Table '%s' in system tablespace"),
    ERR_IO_READ_ERROR(1810, new byte[]{'H', 'Y', '0', '0', '0'}, "IO Read error: (%d, %s) %s"),
    ERR_IO_WRITE_ERROR(1811, new byte[]{'H', 'Y', '0', '0', '0'}, "IO Write error: (%d, %s) %s"),
    ERR_TABLESPACE_MISSING(1812, new byte[]{'H', 'Y', '0', '0', '0'}, "Tablespace is missing for table '%s'"),
    ERR_TABLESPACE_EXISTS(1813, new byte[]{'H', 'Y', '0', '0', '0'}, "Tablespace for table '%s' exists. Please "
            + "DISCARD the tablespace before IMPORT."),
    ERR_TABLESPACE_DISCARDED(1814, new byte[]{'H', 'Y', '0', '0', '0'}, "Tablespace has been discarded for table '%s'"),
    ERR_INTERNAL_ERROR(1815, new byte[]{'H', 'Y', '0', '0', '0'}, "Internal error: %s"),


    ERR_MUST_CHANGE_PASSWORD_LOGIN(1862, new byte[]{'H', 'Y', '0', '0', '0'}, "Your password has expired. To log in "
            + "you must change it using a client that supports expired passwords."),
    ERR_CREDENTIALS_CONTRADICT_TO_HISTORY(3638, new byte[] {'H', 'Y', '0', '0', '0'},
            "Cannot use these credentials for '%s'@'%s' because they contradict the password history policy"),
    ERR_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK(3955, new byte[] {'H', 'Y', '0', '0', '0'},
            "Access denied for user '%s'@'%s'. Account is blocked for %s second(s) (%s second(s) remaining) due to %s consecutive failed logins."),
    // Following is Palo's error code, which start from 5000
    ERR_NOT_OLAP_TABLE(5000, new byte[] {'H', 'Y', '0', '0', '0'}, "Table '%s' is not a OLAP table"),
    ERR_WRONG_PROC_PATH(5001, new byte[] {'H', 'Y', '0', '0', '0'}, "Proc path '%s' doesn't exist"),
    ERR_COL_NOT_MENTIONED(5002, new byte[] {'H', 'Y', '0', '0', '0'},
            "'%s' must be explicitly mentioned in column permutation"),
    ERR_OLAP_KEY_MUST_BEFORE_VALUE(5003, new byte[] {'H', 'Y', '0', '0', '0'},
            "Key column must before value column"),
    ERR_TABLE_MUST_HAVE_KEYS(5004, new byte[] {'H', 'Y', '0', '0', '0'},
            "Table must have at least 1 key column"),
    ERR_UNKNOWN_CLUSTER_ID(5005, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown cluster id '%s'"),
    ERR_UNKNOWN_PLAN_HINT(5006, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown plan hint '%s'"),
    ERR_PLAN_HINT_CONFILT(5007, new byte[] {'H', 'Y', '0', '0', '0'}, "Conflict plan hint '%s'"),
    ERR_INSERT_HINT_NOT_SUPPORT(5008, new byte[] {'H', 'Y', '0', '0', '0'},
            "INSERT hints are only supported for partitioned table"),
    ERR_PARTITION_CLAUSE_NO_ALLOWED(5009, new byte[] {'H', 'Y', '0', '0', '0'},
            "PARTITION clause is not valid for INSERT into unpartitioned table"),
    ERR_COL_NUMBER_NOT_MATCH(5010, new byte[] {'H', 'Y', '0', '0', '0'},
            "Number of columns don't equal number of SELECT statement's select list"),
    ERR_UNRESOLVED_TABLE_REF(5011, new byte[] {'H', 'Y', '0', '0', '0'},
            "Unresolved table reference '%s'"),
    ERR_BAD_NUMBER(5012, new byte[] {'H', 'Y', '0', '0', '0'}, "'%s' is not a number"),
    ERR_BAD_TIMEUNIT(5013, new byte[] {'H', 'Y', '0', '0', '0'}, "Unsupported time unit '%s'"),
    ERR_BAD_TABLE_STATE(5014, new byte[] {'H', 'Y', '0', '0', '0'}, "Table state is not NORMAL: '%s'"),
    ERR_BAD_PARTITION_STATE(5015, new byte[] {'H', 'Y', '0', '0', '0'}, "Partition state is not NORMAL: '%s':'%s'"),
    ERR_PARTITION_HAS_LOADING_JOBS(5016, new byte[] {'H', 'Y', '0', '0', '0'}, "Partition has loading jobs: '%s'"),
    ERR_NOT_KEY_COLUMN(5017, new byte[] {'H', 'Y', '0', '0', '0'}, "Column is not a key column: '%s'"),
    ERR_INVALID_VALUE(5018, new byte[] {'H', 'Y', '0', '0', '0'}, "Value '%s' of '%s' is invalid, '%s'"),
    ERR_REPLICA_NOT_CATCH_UP_WITH_VERSION(5019, new byte[] {'H', 'Y', '0', '0', '0'},
            "Replica does not catch up with version: '%s':'%s'"),
    ERR_BACKEND_OFFLINE(5021, new byte[] {'H', 'Y', '0', '0', '0'}, "Backend is offline: '%s'"),
    ERR_BAD_PARTS_IN_UNPARTITION_TABLE(5022, new byte[] {'H', 'Y', '0', '0', '0'},
            "Number of partitions in unpartitioned table is not 1"),
    ERR_NO_ALTER_OPERATION(5023, new byte[] {'H', 'Y', '0', '0', '0'},
            "No operation in alter statement"),
    ERR_EXECUTE_TIMEOUT(5024, new byte[]{'H', 'Y', '0', '0', '0'}, "Execute timeout"),
    ERR_FAILED_WHEN_INSERT(5025, new byte[]{'H', 'Y', '0', '0', '0'}, "Failed when INSERT execute"),
    ERR_CLUSTER_NO_EXISTS(5028, new byte[]{'H', 'Y', '0', '0', '0'}, "Unknown cluster '%s'"),
    ERR_CLUSTER_NO_AUTHORITY(5030, new byte[]{'H', 'Y', '0', '0', '0'},
            "User '%s' has no permissions '%s' cluster"),
    ERR_CLUSTER_NO_PARAMETER(5031, new byte[]{'H', 'Y', '0', '0', '0'}, "No parameter or parameter is incorrect"),
    ERR_CLUSTER_NO_INSTANCE_NUM(5032, new byte[]{'H', 'Y', '0', '0', '0'}, "No assign properties's instance_num"),
    ERR_CLUSTER_HAS_EXIST(5034, new byte[]{'H', 'Y', '0', '0', '0'}, "Cluster '%s' has exist"),
    ERR_CLUSTER_INSTANCE_NUM_WRONG(5035, new byte[]{'H', 'Y', '0', '0', '0'}, "Cluster '%s' has exist"),
    ERR_CLUSTER_BE_NOT_ENOUGH(5036, new byte[]{'H', 'Y', '0', '0', '0'}, "Be is not enough"),
    ERR_CLUSTER_DELETE_DB_EXIST(5037, new byte[]{'H', 'Y', '0', '0', '0'},
            "All datbases in cluster must be dropped before dropping cluster"),
    ERR_CLUSTER_NO_CLUSTER_NAME(5038, new byte[]{'H', 'Y', '0', '0', '0'}, "There is no cluster name"),
    ERR_CLUSTER_UNKNOWN_ERROR(5040, new byte[]{'4', '2', '0', '0', '0'}, "Unknown cluster '%s'"),
    ERR_CLUSTER_NAME_NULL(5041, new byte[]{'4', '2', '0', '0', '0'}, "No cluster name"),
    ERR_CLUSTER_NO_PERMISSIONS(5042, new byte[]{'4', '2', '0', '0', '0'}, "No permissions"),
    ERR_CLUSTER_CREATE_ISTANCE_NUM_ERROR(5043, new byte[]{'4', '2', '0', '0', '0'},
            "Instance num can't be less than or equal 0"),
    ERR_CLUSTER_SRC_CLUSTER_NOT_EXIST(5046, new byte[]{'4', '2', '0', '0', '0'}, "Src cluster '%s' does not exist"),
    ERR_CLUSTER_DEST_CLUSTER_NOT_EXIST(5047, new byte[]{'4', '2', '0', '0', '0'},
            "Dest cluster '%s' does not exist"),
    ERR_CLUSTER_SRC_DB_NOT_EXIST(5048, new byte[]{'4', '2', '0', '0', '0'}, "Src db '%s' no exist"),
    ERR_CLUSTER_NO_SELECT_CLUSTER(5050, new byte[]{'4', '2', '0', '0', '0'}, "Please enter cluster"),
    ERR_CLUSTER_MIGRATION_NO_LINK(5051, new byte[]{'4', '2', '0', '0', '0'}, "Db %s must be linked to %s at first"),
    ERR_CLUSTER_BACKEND_ERROR(5052, new byte[]{'4', '2', '0', '0', '0'},
            "Cluster has internal error, wrong backend info"),
    ERR_CLUSTER_MIGRATION_NO_EXIT(5053, new byte[]{'4', '2', '0', '0', '0'},
            "There is't migration from '%s' to '%s'"),
    ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE(5054, new byte[]{'4', '2', '0', '0', '0'},
            "Db %s is linked or being migrated"),
    ERR_CLUSTER_MIGRATE_SAME_CLUSTER(5055, new byte[]{'4', '2', '0', '0', '0'},
            "Migrate or link cant't be in same cluster"),
    ERR_CLUSTER_DELETE_DB_ERR(5056, new byte[]{'4', '2', '0', '0', '0'},
            "Can't delete db '%s', linked or migrating"),
    ERR_CLUSTER_RENAME_DB_ERR(5056, new byte[]{'4', '2', '0', '0', '0'},
            "Can't rename db '%s', linked or migrating"),
    ERR_CLUSTER_MIGRATE_BE_NOT_ENOUGH(5056, new byte[]{'4', '2', '0', '0', '0'},
            "Be in the cluster '%s' is not enough"),
    ERR_CLUSTER_ALTER_BE_NO_CHANGE(5056, new byte[]{'4', '2', '0', '0', '0'},
            "There is %d instance already"),
    ERR_CLUSTER_ALTER_BE_IN_DECOMMISSION(5059, new byte[]{'4', '2', '0', '0', '0'},
            "Cluster '%s' has backends in decommission"),
    ERR_WRONG_NAME_FORMAT(5063, new byte[]{'4', '2', '0', '0', '0'},
            "Incorrect %s name '%s'"),
    ERR_COMMON_ERROR(5064, new byte[]{'4', '2', '0', '0', '0'},
            "%s"),
    ERR_COLOCATE_FEATURE_DISABLED(5063, new byte[]{'4', '2', '0', '0', '0'},
            "Colocate feature is disabled by Admin"),
    ERR_COLOCATE_TABLE_NOT_EXIST(5063, new byte[]{'4', '2', '0', '0', '0'},
            "Colocate table '%s' does not exist"),
    ERR_COLOCATE_TABLE_MUST_BE_OLAP_TABLE(5063, new byte[]{'4', '2', '0', '0', '0'},
            "Colocate table '%s' must be OLAP table"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_REPLICATION_ALLOCATION(5063, new byte[]{'4', '2', '0', '0', '0'},
            "Colocate tables must have same replication allocation: { %s } should be { %s }"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_BUCKET_NUM(5063, new byte[]{'4', '2', '0', '0', '0'},
            "Colocate tables must have same bucket num: %s should be %s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_SIZE(5063, new byte[]{'4', '2', '0', '0', '0'},
            "Colocate tables distribution columns size must be same: %s should be %s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_TYPE(5063, new byte[]{'4', '2', '0', '0', '0'},
            "Colocate tables distribution columns must have the same data type: %s should be %s"),
    ERR_COLOCATE_NOT_COLOCATE_TABLE(5064, new byte[]{'4', '2', '0', '0', '0'},
            "Table %s is not a colocated table"),
    ERR_INVALID_OPERATION(5065, new byte[]{'4', '2', '0', '0', '0'}, "Operation %s is invalid"),
    ERR_DYNAMIC_PARTITION_MUST_HAS_SAME_BUCKET_NUM_WITH_COLOCATE_TABLE(5063, new byte[]{'4', '2', '0', '0', '0'},
        "Dynamic partition buckets must equal the distribution buckets if creating a colocate table: %s"),
    ERROR_DYNAMIC_PARTITION_TIME_UNIT(5065, new byte[]{'4', '2', '0', '0', '0'},
            "Unsupported time unit %s. Expect HOUR/DAY/WEEK/MONTH/YEAR."),
    ERROR_DYNAMIC_PARTITION_START_ZERO(5066, new byte[]{'4', '2', '0', '0', '0'},
            "Dynamic partition start must less than 0"),
    ERROR_DYNAMIC_PARTITION_START_FORMAT(5066, new byte[]{'4', '2', '0', '0', '0'},
            "Invalid dynamic partition start %s"),
    ERROR_DYNAMIC_PARTITION_END_ZERO(5066, new byte[]{'4', '2', '0', '0', '0'},
            "Dynamic partition end must greater than 0"),
    ERROR_DYNAMIC_PARTITION_END_FORMAT(5066, new byte[]{'4', '2', '0', '0', '0'},
            "Invalid dynamic partition end %s"),
    ERROR_DYNAMIC_PARTITION_END_EMPTY(5066, new byte[]{'4', '2', '0', '0', '0'},
            "Dynamic partition end is empty"),
    ERROR_DYNAMIC_PARTITION_BUCKETS_ZERO(5067, new byte[]{'4', '2', '0', '0', '0'},
            "Dynamic partition buckets must greater than 0"),
    ERROR_DYNAMIC_PARTITION_BUCKETS_FORMAT(5067, new byte[]{'4', '2', '0', '0', '0'},
            "Invalid dynamic partition buckets %s"),
    ERROR_DYNAMIC_PARTITION_BUCKETS_EMPTY(5066, new byte[]{'4', '2', '0', '0', '0'},
            "Dynamic partition buckets is empty"),
    ERROR_DYNAMIC_PARTITION_ENABLE(5068, new byte[]{'4', '2', '0', '0', '0'},
            "Invalid dynamic partition enable: %s. Expected true or false"),
    ERROR_DYNAMIC_PARTITION_PREFIX(5069, new byte[]{'4', '2', '0', '0', '0'},
            "Invalid dynamic partition prefix: %s."),
    ERR_OPERATION_DISABLED(5070, new byte[]{'4', '2', '0', '0', '0'},
            "Operation %s is disabled. %s"),
    ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_ZERO(5071, new byte[]{'4', '2', '0', '0', '0'},
            "Dynamic partition replication num must greater than 0"),
    ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_FORMAT(5072, new byte[]{'4', '2', '0', '0', '0'},
            "Invalid dynamic partition replication num: %s."),
    ERROR_CREATE_TABLE_LIKE_EMPTY(5073, new byte[]{'4', '2', '0', '0', '0'},
            "Origin create table stmt is empty"),
    ERROR_DYNAMIC_PARTITION_CREATE_HISTORY_PARTITION(5074, new byte[]{'4', '2', '0', '0', '0'},
            "Invalid dynamic partition create_history_partition: %s. Expected true or false"),
    ERROR_DYNAMIC_PARTITION_HISTORY_PARTITION_NUM_ZERO(5075, new byte[]{'4', '2', '0', '0', '0'},
            "Dynamic history partition num must greater than 0"),
    ERROR_DYNAMIC_PARTITION_RESERVED_HISTORY_PERIODS_EMPTY(5076, new byte[]{'4', '2', '0', '0', '0'},
            "Dynamic reserved history periods is empty."),
    ERROR_DYNAMIC_PARTITION_RESERVED_HISTORY_PERIODS_INVALID(5077, new byte[]{'4', '2', '0', '0', '0'},
            "Invalid \" %s \" value %s. It must be like \"[yyyy-MM-dd,yyyy-MM-dd],[...,...]\" while time_unit is "
                    + "DAY/WEEK/MONTH or \"[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]\" while time_unit is HOUR."),
    ERROR_DYNAMIC_PARTITION_RESERVED_HISTORY_PERIODS_START_ENDS_LENGTH_NOT_EQUAL(5078, new byte[]{'4', '2', '0', '0',
            '0'},
            "RESERVED_HISTORY_PERIODS must have pairs of date value. The input %s is not valid."),
    ERROR_DYNAMIC_PARTITION_RESERVED_HISTORY_PERIODS_START_LARGER_THAN_ENDS(5079, new byte[]{'4', '2', '0', '0', '0'},
            "The first date is larger than the second date, [%s,%s] is invalid."),
    ERROR_LDAP_CONFIGURATION_ERR(5080, new byte[]{'4', '2', '0', '0', '0'},
            "LDAP configuration is incorrect or LDAP admin password is not set."),
    ERROR_LDAP_USER_NOT_UNIQUE_ERR(5081, new byte[]{'4', '2', '0', '0', '0'},
            "%s is not unique in LDAP server."),
    ERR_ILLEGAL_COLUMN_REFERENCE_ERROR(5082, new byte[]{'4', '2', '0', '0', '1'},
            "Illegal column/field reference '%s' of semi-/anti-join"),
    ERR_EMPTY_PARTITION_IN_TABLE(5083, new byte[]{'4', '2', '0', '0', '0'},
            "data cannot be inserted into table with empty partition. "
                    + "Use `SHOW PARTITIONS FROM %s` to see the currently partitions of this table. "),
    ERROR_SQL_AND_LIMITATIONS_SET_IN_ONE_RULE(5084, new byte[]{'4', '2', '0', '0', '0'},
            "sql/sqlHash and partition_num/tablet_num/cardinality cannot be set in one rule."),
    ERR_WRONG_CATALOG_NAME(5085, new byte[]{'4', '2', '0', '0', '0'}, "Incorrect catalog name '%s'"),
    ERR_UNKNOWN_CATALOG(5086, new byte[]{'4', '2', '0', '0', '0'}, "Unknown catalog '%s'"),
    ERR_CATALOG_ACCESS_DENIED(5087, new byte[]{'4', '2', '0', '0', '0'},
            "Access denied for user '%s' to catalog '%s'"),
    ERR_NONSUPPORT_HMS_TABLE(5088, new byte[]{'4', '2', '0', '0', '0'},
            "Nonsupport hive metastore table named '%s' in database '%s' with catalog '%s'."),
    ERR_TABLE_NAME_LENGTH_LIMIT(5089, new byte[]{'4', '2', '0', '0', '0'}, "Table name length exceeds limit, "
     + "the length of table name '%s' is %d which is greater than the configuration 'table_name_length_limit' (%d)."),

    ERR_NONSUPPORT_TIME_TRAVEL_TABLE(5090, new byte[]{'4', '2', '0', '0', '0'}, "Only iceberg/hudi external"
     + " table supports time travel in current version"),

    ERR_NONSSL_HANDSHAKE_RESPONSE(5091, new byte[] {'4', '2', '0', '0'},
            "SSL mode on but received non-ssl handshake response from client."),

    ERR_MORE_THAN_ONE_AUTO_INCREMENT_COLUMN(5092, new byte[]{'4', '2', '0', '0', '0'},
            "there can be at most one auto increment column in OlapTable."),

    ERR_AUTO_INCREMENT_COLUMN_NULLABLE(5093, new byte[]{'4', '2', '0', '0', '0'},
            "the auto increment column should be NOT NULL."),

    ERR_AUTO_INCREMENT_COLUMN_WITH_DEFAULT_VALUE(5094, new byte[]{'4', '2', '0', '0', '0'},
            "the auto increment column can't have default value."),

    ERR_AUTO_INCREMENT_COLUMN_NOT_BIGINT_TYPE(5095, new byte[]{'4', '2', '0', '0', '0'},
            "the auto increment must be BIGINT type."),

    ERR_AUTO_INCREMENT_COLUMN_IN_AGGREGATE_TABLE(5096, new byte[]{'4', '2', '0', '0', '0'},
            "the auto increment is only supported in duplicate table and unique table."),

    ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT(5097, new byte[]{'4', '2', '0', '0', '0'},
            "There can only be one stmt that returns the result and it is at the end."),

    ERR_CLOUD_CLUSTER_ERROR(5098, new byte[]{'4', '2', '0', '0', '0'},
            "Cluster %s not exist, use SQL 'SHOW CLUSTERS' to get a valid cluster"),

    ERR_NO_CLUSTER_ERROR(5099, new byte[]{'4', '2', '0', '0', '0'}, "No cluster selected"),

    ERR_NOT_CLOUD_MODE(6000, new byte[]{'4', '2', '0', '0', '0'},
            "Command only support in cloud mode.");

    // This is error code
    private final int code;
    // This sql state is compatible with ANSI SQL
    private final byte[] sqlState;
    // Error message format
    private final String errorMsg;

    ErrorCode(int code, byte[] sqlState, String errorMsg) {
        this.code = code;
        this.sqlState = sqlState;
        this.errorMsg = errorMsg;
    }

    public int getCode() {
        return code;
    }

    public byte[] getSqlState() {
        return sqlState;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public String formatErrorMsg(Object... args) {
        try {
            return String.format(errorMsg, args);
        } catch (MissingFormatArgumentException e) {
            return errorMsg;
        }
    }
}
