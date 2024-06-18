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
suite("test_current_timestamp_as_column_default_value") {
    sql "DROP TABLE IF EXISTS test_default4"
    sql """create table test_default4(a int, b int) distributed by hash(a) properties('replication_num'="1");"""

    sql "DROP TABLE IF EXISTS test_default10"
    test {
        sql """create table test_default10(a int, b varchar(100) default current_timestamp) 
        distributed by hash(a) properties('replication_num'="1");"""
        exception "Types other than DATETIME and DATETIMEV2 cannot use current_timestamp as the default value"
    }

    test{
        sql """alter table test_default4 add column dt varchar(100) default current_timestamp"""
        exception "Types other than DATETIME and DATETIMEV2 cannot use current_timestamp as the default value"
    }

    test {
        sql """create table test_default10(a int, b varchar(100) default current_timestamp) 
        distributed by hash(a) properties('replication_num'="1");"""
        exception "Types other than DATETIME and DATETIMEV2 cannot use current_timestamp as the default value"
    }

    test{
        sql "alter table test_default4 add column dt varchar(100) default current_timestamp"
        exception "Types other than DATETIME and DATETIMEV2 cannot use current_timestamp as the default value"
    }
}
