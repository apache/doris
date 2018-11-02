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

namespace cpp doris
namespace java org.apache.doris.thrift

include "Partitions.thrift"
include "Types.thrift"
include "Status.thrift"

struct TDdlResult {
  // required in V1
  1: optional Status.TStatus status
}

enum TCommonDdlType {
    CREATE_DATABASE
    DROP_DATABASE
    CREATE_TABLE
    DROP_TABLE
    LOAD 
}

// Parameters of CREATE DATABASE command
struct TCreateDbParams {
    // database name to create
    1: required string database_name
}

// Parameters of DROP DATABASE command
struct TDropDbParams {
    // database name to drop
    1: required string database_name
}

// database_name + table_name
struct TTableName {
    1: required string db_name
    2: required string table_name
}

// supported aggregation type
enum TAggType {
    AGG_SUM
    AGG_MIN
    AGG_MAX
    AGG_REPLACE
}

// column defination
//struct TColumn {
//    // column name
//    1: required string column_name
//
//    // column type
//    2: required Types.TColumnType column_type
//
//    // aggregation type, if not set, this column is a KEY column, otherwise is a value column
//    3: optional TAggType agg_type
//    
//    // default value
//    4: optional string default_value 
//}

enum THashType {
    CRC32
}

// random partition info
struct TRandomPartitionDesc {
}

// hash partition info
struct THashPartitionDesc {
    // column to compute hash value
    1: required list<string> column_list

    // hash buckets
    2: required i32 hash_buckets

    // type to compute hash value. if not set, use CRC32
    3: optional THashType hash_type
}

// value used to represents one column value in one range value
struct TValue {
    1: optional string value

    // if this sign is set and is true, this value is stand for MAX value
    2: optional bool max_sign
}

// one range value
struct TRangeValue {
    1: required list<TValue> value_list
}

// range partition defination
struct TRangePartitionDesc {
    // column used to compute range
    1: required list<string> column_list

    // range value for range, if not set, all in one range
    2: optional list<TRangeValue> range_value
}

// partition info
struct TPartitionDesc {
    // partition type
    1: required Partitions.TPartitionType type
    // hash buckets
    2: required i32 partition_num

    // hash partition information
    3: optional THashPartitionDesc hash_partition

    // range partition information
    4: optional TRangePartitionDesc range_partition

    // random partition infomation
    5: optional TRandomPartitionDesc random_partition
}

// Parameters of CREATE TABLE command
struct TCreateTableParams {
    // table name to create
    1: required TTableName table_name

    // column defination.
    // 2: required list<TColumn> columns

    // engine type, if not set, use the default type.
    3: optional string engine_name

    // if set and true, no error when there is already table with same name
    4: optional bool if_not_exists

    // partition info, if not set, use the default partition type which meta define.
    5: optional TPartitionDesc partition_desc

    // used to set row format, maybe columnar or row format
    6: optional string row_format_type

    // other properties
    7: optional map<string, string> properties
}

// Parameters of DROP TABLE command
struct TDropTableParams {
    // table name to drop
    1: required TTableName table_name

    // If true, no error is raised if the target db does not exist
    2: optional bool if_exists
}

// Parameters to CREATE ROLLUP
struct TCreateRollupParams {
    // table name which create rollup
    1: required TTableName table_name

    // column names ROLLUP contains
    2: required list<string> column_names

    // rollup name, if not set, meta will assign a default value
    3: optional string rollup_name

    // partition info, if not set, use the base table's 
    4: optional TPartitionDesc partition_desc
}

// Parameters to DROP ROLLUP
struct TDropRollupParams {
    // table name which create rollup
    1: required TTableName table_name

    // rollup name to drop
    2: required string rollup_name
}

// Parameters for SCHEMA CHANGE
// struct TShcemaChangeParams {
//     // table name need to schema change
//     1: required TTableName table_name
// 
//     // column definations for this table
//     2: required list<TColumn> column_defs
// 
//     // rollup schema, map is 'rollup_name' -> 'list of column_name'
//     3: required map<string, list<string>> rollup_defs
// }

// Parameters to create function
struct TCreateFunctionParams {
    // database name which function to create is in
    1: required string db_name

    // function name to create
    2: required string function_name

    // function argument type
    3: required list<Types.TColumnType> argument_type

    // function return type
    4: required Types.TColumnType return_type

    // function dynamic library path
    5: required string so_file_path

    // other properties
    6: optional map<string, string> properties
}

// Parameters to drop function
struct TDropFunctionParams {
    // database name which function to drop is in
    1: required string db_name

    // function name to drop
    2: required string function_name
}

// enum TSetType {
//     SESSION
//     GLOBAL
// }
// 
// // Parameters to SET opration
// struct TSetParams {
//     // set type, GLOBAL\SESSION
//     1: required TSetType type
// 
//     // set pairs, one name and one Expr
//     // 2: required map<string, TExpr> set_content
// }

struct TUserSpecification {
    1: required string user_name
    2: optional string host_name
}

// Parameters to create user
struct TCreateUserParams {
    1: required TUserSpecification user_spec

    // user's password
    2: optional string password 
}

// Parameters to drop user
struct TDropUserParams {
    // user name to drop
    1: required string user_spec
}

// Parameters to SET PASSWORD
struct TSetPasswordParams {
    1: required TUserSpecification user_spec

    // password will changed to after this opration
    3: required string password
}

enum TPrivType {
    PRIVILEGE_READ_ONLY
    PRIVILEGE_READ_WRITE
}

// Parameters to GRANT
struct TGrantParams {
    1: required TUserSpecification user_spec

    // database to grant
    3: required string db_name

    // privileges to grant
    4: required list<TPrivType> priv_types
}

// Data info 
struct TDataSpecification {
    // database name which table belongs to
    1: required TTableName table_name

    // all file pathes need to load
    3: required list<string> file_path

    // column names in file
    4: optional list<string> columns

    // column separator
    5: optional string column_separator

    // line separator
    6: optional string line_separator

    // if true, value will be multiply with -1
    7: optional bool is_negative
}

struct TLabelName {
    // database name which load_label belongs to 
    1: required string db_name

    // load label which to be canceled.
    2: required string load_label
}

// Parameters to LOAD file
struct TLoadParams {
    // label belong to this load job, used when cancel load, show load
    1: required TLabelName load_label

    // data profiles used to load in this job
    2: required list<TDataSpecification> data_profiles
    
    // task info
    3: optional map<string, string> properties
}

// Parameters to CANCEL LOAD file
struct TCancelLoadParams {
    1: required TLabelName load_label
}

enum TPaloInternalServiceVersion {
    V1
}

struct TMasterDdlRequest {
    1: required TPaloInternalServiceVersion protocol_version
    2: required TCommonDdlType ddl_type
    3: optional TCreateDbParams create_db_params
    4: optional TDropDbParams drop_db_params
    // 5: optional TCreateTableParams create_table_params
    6: optional TDropTableParams drop_table_params
    7: optional TLoadParams load_params
    8: optional TCancelLoadParams cancel_load_params
    9: optional TCreateUserParams create_user_params
    10: optional TDropUserParams drop_user_params
    11: optional TCreateRollupParams create_rollup_params
    12: optional TDropRollupParams drop_rollup_params
    13: optional TCreateFunctionParams create_function_params
    14: optional TDropFunctionParams drop_function_params
}

struct TMasterDdlResponse {
    1: required TPaloInternalServiceVersion protocol_version
    2: required TCommonDdlType ddl_type
    3: optional Status.TStatus status
}
