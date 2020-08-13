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
    ERR_CANT_CREATE_TABLE(1005, new byte[] { 'H', 'Y', '0', '0', '0' }, "Can't create table '%s' (errno: %s)"),
    ERR_DB_CREATE_EXISTS(1007, new byte[] {'H', 'Y', '0', '0', '0'}, "Can't create database '%s'; database exists"),
    ERR_DB_DROP_EXISTS(1008, new byte[] { 'H', 'Y', '0', '0', '0' },
            "Can't drop database '%s'; database doesn't exist"),
    ERR_DB_ACCESS_DENIED(1044, new byte[] {'4', '2', '0', '0', '0'}, "Access denied for user '%s' to database '%s'"),
    ERR_ACCESS_DENIED_ERROR(1045, new byte[] {'2', '8', '0', '0', '0'},
            "Access denied for user '%s' (using password: %s)"),
    ERR_NO_DB_ERROR(1046, new byte[] {'3', 'D', '0', '0', '0'}, "No database selected"),
    ERR_UNKNOWN_COM_ERROR(1047, new byte[] {'0', '8', 'S', '0', '1'}, "Unknown command"),
    ERR_BAD_DB_ERROR(1049, new byte[] {'4', '2', '0', '0', '0'}, "Unknown database '%s'"),
    ERR_TABLE_EXISTS_ERROR(1050, new byte[] {'4', '2', 'S', '0', '1'}, "Table '%s' already exists"),
    ERR_BAD_TABLE_ERROR(1051, new byte[] {'4', '2', 'S', '0', '2'}, "Unknown table '%s'"),
    ERR_NON_UNIQ_ERROR(1052, new byte[] {'2', '3', '0', '0', '0'}, "Column '%s' in is ambiguous"),
    ERR_ILLEGAL_COLUMN_REFERENCE_ERROR(1053, new byte[] {'2', '3', '0', '0', '1'},
            "Illegal column/field reference '%s' of semi-/anti-join"),
    ERR_BAD_FIELD_ERROR(1054, new byte[] {'4', '2', 'S', '2', '2'}, "Unknown column '%s' in '%s'"),
    ERR_WRONG_VALUE_COUNT(1058, new byte[] {'2', '1', 'S', '0', '1'}, "Column count doesn't match value count"),
    ERR_DUP_FIELDNAME(1060, new byte[] {'4', '2', 'S', '2', '1'}, "Duplicate column name '%s'"),
    ERR_NONUNIQ_TABLE(1066, new byte[] {'4', '2', '0', '0', '0'}, "Not unique table/alias: '%s'"),
    ERR_NO_SUCH_THREAD(1094, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown thread id: %d"),
    ERR_KILL_DENIED_ERROR(1095, new byte[] {'H', 'Y', '0', '0', '0'}, "You are not owner of thread %d"),
    ERR_NO_TABLES_USED(1096, new byte[] {'H', 'Y', '0', '0', '0'}, "No tables used"),
    ERR_WRONG_DB_NAME(1102, new byte[] {'4', '2', '0', '0', '0'}, "Incorrect database name '%s'"),
    ERR_WRONG_TABLE_NAME(1104, new byte[] {'4', '2', '0', '0', '0'}, "Incorrect table name '%s'"),
    ERR_UNKNOWN_ERROR(1105, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown error"),
    ERR_FIELD_SPECIFIED_TWICE(1110, new byte[] {'4', '2', '0', '0', '0'}, "Column '%s' specified twice"),
    ERR_INVALID_GROUP_FUNC_USE(1111, new byte[] {'H', 'Y', '0', '0', '0'}, "Invalid use of group function"),
    ERR_TABLE_MUST_HAVE_COLUMNS(1113, new byte[] {'4', '2', '0', '0', '0'}, "A table must have at least 1 column"),
    ERR_UNKNOWN_CHARACTER_SET(1115, new byte[] {'4', '2', '0', '0', '0'}, "Unknown character set: '%s'"),
    ERR_IP_NOT_ALLOWED(1130, new byte[] { '4', '2', '0', '0', '0' },
            "Host %s is not allowed to connect to this MySQL server"),
    ERR_PASSWORD_NOT_ALLOWED(1132, new byte[] {'4', '2', '0', '0', '0'},
            "You must have privileges to "
                    + "update tables in the mysql database to be able to change passwords for others"),
    ERR_NONEXISTING_GRANT(1141, new byte[] { '4', '2', '0', '0', '0' },
            "There is no such grant defined for user '%s' on host '%s'"),
    ERR_TABLEACCESS_DENIED_ERROR(1142, new byte[] { '4', '2', '0', '0', '0' },
            "%s command denied to user '%s'@'%s' for table '%s'"),
    ERR_WRONG_COLUMN_NAME(1166, new byte[] {'4', '2', '0', '0', '0'}, "Incorrect column name '%s'"),
    ERR_UNKNOWN_SYSTEM_VARIABLE(1193, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown system variable '%s'"),
    ERR_TOO_MANY_USER_CONNECTIONS(1203, new byte[] {'4', '2', '0', '0', '0'},
            "User %s already has more than 'max_user_connections' active connections"),
    ERR_NO_PERMISSION_TO_CREATE_USER(1211, new byte[] {'4', '2', '0', '0', '0'},
            "'%s' is not allowed to create new users"),
    ERR_SPECIFIC_ACCESS_DENIED_ERROR(1227, new byte[] {'4', '2', '0', '0', '0'},
            "Access denied; you need (at least one of) the %s privilege(s) for this operation"),
    ERR_LOCAL_VARIABLE(1228, new byte[] {'H', 'Y', '0', '0', '0'},
            "Variable '%s' is a SESSION variable and can't be used with SET GLOBAL"),
    ERR_GLOBAL_VARIABLE(1229, new byte[] {'H', 'Y', '0', '0', '0'},
            "Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL"),
    ERR_NO_DEFAULT(1230, new byte[] {'4', '2', '0', '0', '0'}, "Variable '%s' doesn't have a default value"),
    ERR_WRONG_VALUE_FOR_VAR(1231, new byte[] {'4', '2', '0', '0', '0'},
            "Variable '%s' can't be set to the value of '%s'"),
    ERR_WRONG_TYPE_FOR_VAR(1232, new byte[] {'4', '2', '0', '0', '0'}, "Incorrect argument type to variable '%s'"),
    ERR_DERIVED_MUST_HAVE_ALIAS(1248, new byte[] {'4', '2', '0', '0', '0'},
            "Every derived table must have its own alias"),
    ERR_NOT_SUPPORTED_AUTH_MODE(1251, new byte[] {'0', '8', '0', '0', '4'},
            "Client does not support authentication protocol requested by server; consider upgrading MySQL client"),
    ERR_UNKNOWN_STORAGE_ENGINE(1286, new byte[] {'4', '2', '0', '0', '0'}, "Unknown storage engine '%s'"),
    ERR_UNKNOWN_TIME_ZONE(1298, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown or incorrect time zone: '%s'"),
    ERR_WRONG_OBJECT(1347, new byte[] {'H', 'Y', '0', '0', '0'}, "'%s'.'%s' is not '%s'"),
    ERR_VIEW_WRONG_LIST(1353, new byte[] {'H', 'Y', '0', '0', '0'},
            "View's SELECT and view's field list have different column counts"),
    ERR_NO_DEFAULT_FOR_FIELD(1364, new byte[] {'H', 'Y', '0', '0', '0'}, 
            "Field '%s' is not null but doesn't have a default value"),
    ERR_PASSWD_LENGTH(1372, new byte[] {'H', 'Y', '0', '0', '0'},
            "Password hash should be a %d-digit hexadecimal number"),
    ERR_CANNOT_USER(1396, new byte[] {'H', 'Y', '0', '0', '0'}, "Operation %s failed for %s"),
    ERR_NON_INSERTABLE_TABLE(1471, new byte[] {'H', 'Y', '0', '0', '0'},
            "The target table %s of the %s is not insertable-into"),
    ERR_DROP_PARTITION_NON_EXISTENT(1507, new byte[] { 'H', 'Y', '0', '0', '0' },
            "Error in list of partitions to %s"),
    ERR_DROP_LAST_PARTITION(1508, new byte[] { 'H', 'Y', '0', '0', '0' },
            "Cannot remove all partitions, use DROP TABLE instead"),
    ERR_SAME_NAME_PARTITION(1517, new byte[] { 'H', 'Y', '0', '0', '0' }, "Duplicate partition name %s"),
    ERR_WRONG_PARTITION_NAME(1567, new byte[] {'H', 'Y', '0', '0', '0'}, "Incorrect partition name '%s'"),
    ERR_VARIABLE_IS_READONLY(1621, new byte[] {'H', 'Y', '0', '0', '0'}, "Variable '%s' is a read only variable"),
    ERR_UNKNOWN_PARTITION(1735, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown partition '%s' in table '%s'"),
    ERR_PARTITION_CLAUSE_ON_NONPARTITIONED(1747, new byte[] {'H', 'Y', '0', '0', '0'},
            "PARTITION () clause on non partitioned table"),
    ERR_EMPTY_PARTITION_IN_TABLE(1748, new byte[] {'H', 'Y', '0', '0', '0'},
            "data cannot be inserted into table with emtpy partition. [%s]"),
    ERR_NO_SUCH_PARTITION(1749, new byte[] {'H', 'Y', '0', '0', '0'}, "partition '%s' doesn't exist"),
    // Following is Palo's error code, which start from 5000
    ERR_NOT_OLAP_TABLE(5000, new byte[] {'H', 'Y', '0', '0', '0'}, "Table '%s' is not a OLAP table"),
    ERR_WRONG_PROC_PATH(5001, new byte[] {'H', 'Y', '0', '0', '0'}, "Proc path '%s' doesn't exist"),
    ERR_COL_NOT_MENTIONED(5002, new byte[] {'H', 'Y', '0', '0', '0'},
            "'%s' must be explicitly mentioned in column permutation"),
    ERR_OLAP_KEY_MUST_BEFORE_VALUE(5003, new byte[] { 'H', 'Y', '0', '0', '0' },
            "Key column must before value column"),
    ERR_TABLE_MUST_HAVE_KEYS(5004, new byte[] { 'H', 'Y', '0', '0', '0' },
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
    ERR_BAD_TIMEUNIT(5013, new byte[] { 'H', 'Y', '0', '0', '0' }, "Unsupported time unit '%s'"),
    ERR_BAD_TABLE_STATE(5014, new byte[] { 'H', 'Y', '0', '0', '0' }, "Table state is not NORMAL: '%s'"),
    ERR_BAD_PARTITION_STATE(5015, new byte[] { 'H', 'Y', '0', '0', '0' }, "Partition state is not NORMAL: '%s':'%s'"),
    ERR_PARTITION_HAS_LOADING_JOBS(5016, new byte[] { 'H', 'Y', '0', '0', '0' }, "Partition has loading jobs: '%s'"),
    ERR_NOT_KEY_COLUMN(5017, new byte[] { 'H', 'Y', '0', '0', '0' }, "Column is not a key column: '%s'"),
    ERR_INVALID_VALUE(5018, new byte[] { 'H', 'Y', '0', '0', '0' }, "Invalid value format: '%s'"),
    ERR_REPLICA_NOT_CATCH_UP_WITH_VERSION(5019, new byte[] { 'H', 'Y', '0', '0', '0' },
            "Replica does not catch up with version: '%s':'%s'"),
    ERR_BACKEND_OFFLINE(5021, new byte[] { 'H', 'Y', '0', '0', '0' }, "Backend is offline: '%s'"),
    ERR_BAD_PARTS_IN_UNPARTITION_TABLE(5022, new byte[] { 'H', 'Y', '0', '0', '0' },
            "Number of partitions in unpartitioned table is not 1"),
    ERR_NO_ALTER_OPERATION(5023, new byte[] { 'H', 'Y', '0', '0', '0' },
            "No operation in alter statement"),
    ERR_EXECUTE_TIMEOUT(5024, new byte[] { 'H', 'Y', '0', '0', '0' }, "Execute timeout"),
    ERR_FAILED_WHEN_INSERT(5025, new byte[] { 'H', 'Y', '0', '0', '0' }, "Failed when INSERT execute"),
    ERR_UNSUPPORTED_TYPE_IN_CTAS(5026, new byte[] { 'H', 'Y', '0', '0', '0' },
            "Unsupported type '%s' in create table as select statement"),
    ERR_MISSING_PARAM(5027, new byte[] { 'H', 'Y', '0', '0', '0' }, "Missing param: %s "),
    ERR_CLUSTER_NO_EXISTS(5028, new byte[] { 'H', 'Y', '0', '0', '0' }, "Unknown cluster '%s'"),
    ERR_CLUSTER_NO_AUTHORITY(5030, new byte[] { 'H', 'Y', '0', '0', '0' },
            "User '%s' has no permissions '%s' cluster"),
    ERR_CLUSTER_NO_PARAMETER(5031, new byte[] { 'H', 'Y', '0', '0', '0' }, "No parameter or parameter is incorrect"),
    ERR_CLUSTER_NO_INSTANCE_NUM(5032, new byte[] { 'H', 'Y', '0', '0', '0' }, "No assign properties's instance_num"),
    ERR_CLUSTER_HAS_EXIST(5034, new byte[] { 'H', 'Y', '0', '0', '0' }, "Cluster '%s' has exist"),
    ERR_CLUSTER_INSTANCE_NUM_WRONG(5035, new byte[] { 'H', 'Y', '0', '0', '0' }, "Cluster '%s' has exist"),
    ERR_CLUSTER_BE_NOT_ENOUGH(5036, new byte[] { 'H', 'Y', '0', '0', '0' }, "Be is not enough"),
    ERR_CLUSTER_DELETE_DB_EXIST(5037, new byte[] { 'H', 'Y', '0', '0', '0' }, 
            "All datbases in cluster must be dropped before dropping cluster"),
    ERR_CLUSTER_DELETE_BE_ID_ERROR(5037, new byte[] { 'H', 'Y', '0', '0', '0' }, "There is no be's id in the System"),
    ERR_CLUSTER_NO_CLUSTER_NAME(5038, new byte[] { 'H', 'Y', '0', '0', '0' }, "There is no cluster name"),
    ERR_CLUSTER_UNKNOWN_ERROR(5040, new byte[] {'4', '2', '0', '0', '0'}, "Unknown cluster '%s'"),
    ERR_CLUSTER_NAME_NULL(5041, new byte[] {'4', '2', '0', '0', '0'}, "No cluster name"),
    ERR_CLUSTER_NO_PERMISSIONS(5042, new byte[] {'4', '2', '0', '0', '0'}, "No permissions"),
    ERR_CLUSTER_CREATE_ISTANCE_NUM_ERROR(5043, new byte[] {'4', '2', '0', '0', '0'}, 
            "Instance num can't be less than or equal 0"),
    ERR_CLUSTER_SRC_CLUSTER_NOT_EXIST(5046, new byte[] { '4', '2', '0', '0', '0' }, "Src cluster '%s' does not exist"),
    ERR_CLUSTER_DEST_CLUSTER_NOT_EXIST(5047, new byte[] { '4', '2', '0', '0', '0' },
            "Dest cluster '%s' does not exist"),
    ERR_CLUSTER_SRC_DB_NOT_EXIST(5048, new byte[] {'4', '2', '0', '0', '0'}, "Src db '%s' no exist"),
    ERR_CLUSTER_DES_DB_NO_EXIT(5049, new byte[] { '4', '2', '0', '0', '0' }, "Dest db '%s' no exist"),
    ERR_CLUSTER_NO_SELECT_CLUSTER(5050, new byte[] {'4', '2', '0', '0', '0'}, "Please enter cluster"),
    ERR_CLUSTER_MIGRATION_NO_LINK(5051, new byte[] {'4', '2', '0', '0', '0'}, "Db %s must be linked to %s at first"),
    ERR_CLUSTER_BACKEND_ERROR(5052, new byte[] {'4', '2', '0', '0', '0'}, 
            "Cluster has internal error, wrong backend info"),
    ERR_CLUSTER_MIGRATION_NO_EXIT(5053, new byte[] {'4', '2', '0', '0', '0'}, 
            "There is't migration from '%s' to '%s'"),
    ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE(5054, new byte[] {'4', '2', '0', '0', '0'}, 
            "Db %s is linked or being migrated"),
    ERR_CLUSTER_MIGRATE_SAME_CLUSTER(5055, new byte[] {'4', '2', '0', '0', '0'}, 
            "Migrate or link cant't be in same cluster"),
    ERR_CLUSTER_DELETE_DB_ERR(5056, new byte[] {'4', '2', '0', '0', '0'}, 
            "Can't delete db '%s', linked or migrating"),
    ERR_CLUSTER_RENAME_DB_ERR(5056, new byte[] {'4', '2', '0', '0', '0'}, 
            "Can't rename db '%s', linked or migrating"),
    ERR_CLUSTER_MIGRATE_BE_NOT_ENOUGH(5056, new byte[] {'4', '2', '0', '0', '0'}, 
            "Be in the cluster '%s' is not enough"),
    ERR_CLUSTER_ALTER_BE_NO_CHANGE(5056, new byte[] {'4', '2', '0', '0', '0'}, 
            "There is %d instance already"),
    ERR_CLUSTER_ALTER_BE_IN_DECOMMISSION(5059, new byte[] {'4', '2', '0', '0', '0'}, 
            "Cluster '%s' has backends in decommission"),
    ERR_WRONG_CLUSTER_NAME(5062, new byte[] { '4', '2', '0', '0', '0' },
            "Incorrect cluster name '%s'(name 'default_cluster' is a reserved name)"),
    ERR_WRONG_NAME_FORMAT(5063, new byte[] { '4', '2', '0', '0', '0' },
            "Incorrect %s name '%s'"),
    ERR_COMMON_ERROR(5064, new byte[] { '4', '2', '0', '0', '0' },
            "%s"),
    ERR_COLOCATE_FEATURE_DISABLED(5063, new byte[] { '4', '2', '0', '0', '0' },
            "Colocate feature is disabled by Admin"),
    ERR_COLOCATE_TABLE_NOT_EXIST(5063, new byte[] { '4', '2', '0', '0', '0' },
            "Colocate table '%s' does not exist"),
    ERR_COLOCATE_TABLE_MUST_BE_OLAP_TABLE(5063, new byte[] { '4', '2', '0', '0', '0' },
            "Colocate table '%s' must be OLAP table"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_REPLICATION_NUM(5063, new byte[] { '4', '2', '0', '0', '0' },
            "Colocate tables must have same replication num: %s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_BUCKET_NUM(5063, new byte[] { '4', '2', '0', '0', '0' },
            "Colocate tables must have same bucket num: %s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_SIZE(5063, new byte[] { '4', '2', '0', '0', '0' },
            "Colocate tables distribution columns size must be same : %s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_TYPE(5063, new byte[] { '4', '2', '0', '0', '0' },
            "Colocate tables distribution columns must have the same data type: %s should be %s"), 
    ERR_COLOCATE_NOT_COLOCATE_TABLE(5064, new byte[] { '4', '2', '0', '0', '0' },
            "Table %s is not a colocated table"),
    ERR_INVALID_OPERATION(5065, new byte[] { '4', '2', '0', '0', '0' }, "Operation %s is invalid"),
    ERROR_DYNAMIC_PARTITION_TIME_UNIT(5065, new byte[] {'4', '2', '0', '0', '0'},
            "Unsupported time unit %s. Expect DAY/WEEK/MONTH."),
    ERROR_DYNAMIC_PARTITION_START_ZERO(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition start must less than 0"),
    ERROR_DYNAMIC_PARTITION_START_FORMAT(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition start %s"),
    ERROR_DYNAMIC_PARTITION_END_ZERO(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition end must greater than 0"),
    ERROR_DYNAMIC_PARTITION_END_FORMAT(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition end %s"),
    ERROR_DYNAMIC_PARTITION_END_EMPTY(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition end is empty"),
    ERROR_DYNAMIC_PARTITION_BUCKETS_ZERO(5067, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition buckets must greater than 0"),
    ERROR_DYNAMIC_PARTITION_BUCKETS_FORMAT(5067, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition buckets %s"),
    ERROR_DYNAMIC_PARTITION_BUCKETS_EMPTY(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition buckets is empty"),
    ERROR_DYNAMIC_PARTITION_ENABLE(5068, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition enable: %s. Expected true or false"),
    ERROR_DYNAMIC_PARTITION_PREFIX(5069, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition prefix: %s."),
    ERR_OPERATION_DISABLED(5070, new byte[] {'4', '2', '0', '0', '0'},
            "Operation %s is disabled. %s"),
    ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_ZERO(5071, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition replication num must greater than 0"),
    ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_FORMAT(5072, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition replication num: %s.");

    ErrorCode(int code, byte[] sqlState, String errorMsg) {
        this.code = code;
        this.sqlState = sqlState;
        this.errorMsg = errorMsg;
    }

    // This is error code
    private int code;
    // This sql state is compatible with ANSI SQL
    private byte[] sqlState;
    // Error message format
    private String errorMsg;

    public int getCode() {
        return code;
    }

    public byte[] getSqlState() {
        return sqlState;
    }

    public String formatErrorMsg(Object... args) {
        try {
            return String.format(errorMsg, args);
        } catch (MissingFormatArgumentException e) {
            return errorMsg;
        }
    }
}
