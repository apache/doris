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

suite("test_temp_partition") {
    sql """
        drop table if exists t_temp_partition;
    """
    
    sql """
        create table t_temp_partition (
            id bigint(20) not null ,
            name varchar(64) not null ,
            age varchar(64) not null ,
            address varchar(300) not null,
            create_time datetime not null
        ) engine = olap
            duplicate key (`id`)
        partition by range (`create_time`) ()
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        properties (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
        ); 
    """

    sql """
        alter table t_temp_partition add temporary partition tp1 values[("2022-11-01 00:00:00"), ("2022-11-17 00:00:00")); 
    """

    sql """
        insert into t_temp_partition temporary partition(tp1) values (0, "a", "10", "asddfa","2022-11-11 00:00:00"); 
    """

    qt_select """
        select * from t_temp_partition temporary partition(tp1) 
    """

    sql """
        drop table if exists t_temp_partition;
    """
}
