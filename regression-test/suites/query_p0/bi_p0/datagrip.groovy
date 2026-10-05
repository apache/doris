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

suite("datagrip") {
    sql """SET net_write_timeout=600"""
    sql """select table_name, table_type, table_comment, engine, table_collation, create_options from information_schema.tables where table_schema = '__internal_schema'"""
    sql """select ordinal_position, column_name, column_type, column_default, generation_expression, table_name, column_comment, is_nullable, extra, collation_name from information_schema.columns where table_schema = '__internal_schema' order by table_name, ordinal_position"""
    sql """select table_name, auto_increment from information_schema.tables where table_schema = '__internal_schema'"""
    sql """select table_name, auto_increment from information_schema.tables where table_schema = '__internal_schema' and auto_increment is not null"""
    sql """select table_name, index_name, index_comment, index_type, non_unique, column_name, sub_part, collation, null expression  from information_schema.statistics  where table_schema = '__internal_schema' and     index_schema = '__internal_schema'  order by index_schema, table_name, index_name, index_type, seq_in_index"""
    sql """select   c.constraint_name,   c.constraint_schema,   c.table_name,   c.constraint_type,   false enforced from information_schema.table_constraints c   where c.table_schema = '__internal_schema'"""
    sql """select constraint_name, table_name, column_name, referenced_table_schema, referenced_table_name, referenced_column_name from information_schema.key_column_usage where table_schema = '__internal_schema' and referenced_column_name is not null order by   table_name , constraint_name , ordinal_position"""
    sql """select table_name, partition_name, subpartition_name, partition_ordinal_position,  subpartition_ordinal_position, partition_method, subpartition_method, partition_expression,  subpartition_expression, partition_description, partition_comment/*, tablespace_name*/     from information_schema.partitions where partition_name is not null and table_schema = 'demo'"""
    sql """select     trigger_name,     event_object_table,     event_manipulation,     action_timing,     definer   from information_schema.triggers   where trigger_schema = 'demo'"""
    sql """select event_name, event_comment, definer, event_type = 'RECURRING' recurring, interval_value, interval_field, cast(coalesce(starts, execute_at) as char) starts, cast(ends as char) ends, status, on_completion = 'PRESERVE' preserve, last_executed from information_schema.events where event_schema = 'demo'"""
    sql """select   routine_name,   routine_type,   routine_definition,   routine_comment,   dtd_identifier,   definer,   is_deterministic = 'YES' is_deterministic,   cast(sql_data_access as char(1)) sql_data_access,   cast(security_type as char(1)) security_type from information_schema.routines where routine_schema = 'demo'"""
    sql """select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc     from mysql.procs_priv where Db = 'demo';"""
    sql """select grantee, table_name, column_name, privilege_type, is_grantable from information_schema.column_privileges where table_schema = 'demo' union all select grantee, table_name, '' column_name, privilege_type, is_grantable from information_schema.table_privileges where table_schema = 'demo' order by table_name, grantee, privilege_type"""
    sql """select specific_name,   ordinal_position,   parameter_name,   parameter_mode,   dtd_identifier from information_schema.parameters where specific_schema = 'demo' and ordinal_position > 0 order by specific_name, ordinal_position"""
    sql """select table_name, view_definition, definer from information_schema.views where table_schema = 'demo'"""
    sql """select table_name, table_type, table_comment, engine, table_collation, create_options   from information_schema.tables   where table_schema = 'information_schema'"""
    sql """select table_name, auto_increment   from information_schema.tables   where table_schema = 'mysql' and auto_increment is not null"""
    sql """select table_name, partition_name, subpartition_name, partition_ordinal_position,  subpartition_ordinal_position, partition_method, subpartition_method, partition_expression,  subpartition_expression, partition_description, partition_comment/*, tablespace_name*/     from information_schema.partitions where partition_name is not null and table_schema = 'mysql'"""
    sql """select trigger_name, event_object_table, event_manipulation, action_timing, definer from information_schema.triggers where trigger_schema = 'mysql'"""
    sql """select   event_name,   event_comment,   definer,   event_type = 'RECURRING' recurring,   interval_value,   interval_field,   cast(coalesce(starts, execute_at) as char) starts,   cast(ends as char) ends,   status,   on_completion = 'PRESERVE' preserve,   last_executed from information_schema.events where event_schema = 'mysql'"""
    sql """select   routine_name,   routine_type,   routine_definition,   routine_comment,   dtd_identifier,   definer,   is_deterministic = 'YES' is_deterministic,   cast(sql_data_access as char(1)) sql_data_access,   cast(security_type as char(1)) security_type from information_schema.routines where routine_schema = 'mysql'"""
    sql """select Host, User, Routine_name, Proc_priv, Routine_type = 'PROCEDURE' as is_proc     from mysql.procs_priv where Db = 'mysql';"""
    sql """select grantee, table_name, column_name, privilege_type, is_grantable from information_schema.column_privileges where table_schema = 'mysql' union all select grantee, table_name, '' column_name, privilege_type, is_grantable from information_schema.table_privileges where table_schema = 'mysql' order by table_name, grantee, privilege_type"""
}