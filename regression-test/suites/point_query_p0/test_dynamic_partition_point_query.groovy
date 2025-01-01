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

import java.time.LocalDate
import java.time.format.DateTimeFormatter

suite("test_dynamic_partition_point_query") {
    sql "drop table if exists dy_par_pq"
    sql """
        CREATE TABLE IF NOT EXISTS dy_par_pq ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int NOT NULL )
        UNIQUE KEY(k1)
        PARTITION BY RANGE(k1) ( )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="10",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="DAY",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1",
            "replication_allocation" = "tag.location.default: 1",
            "store_row_column" = "true")
        """
    def result  = sql "show tables like 'dy_par_pq'"
    logger.info("${result}")
    assertEquals(result.size(), 1)
    result = sql_return_maparray "show partitions from dy_par_pq"
    assertEquals(result.get(0).Buckets.toInteger(), 10)
    def currentDate = LocalDate.now()
    def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    def currenteDay = currentDate.format(formatter)
    sql "insert into dy_par_pq values ('${currenteDay}', 'a', 1);"
    def previous1Day = currentDate.minusDays(1).format(formatter)
    sql "insert into dy_par_pq values ('${previous1Day}', 'b', 2);"
    def previous2Day = currentDate.minusDays(2).format(formatter)
    sql "insert into dy_par_pq values ('${previous2Day}', 'c', 3);"
    def previous3Day = currentDate.minusDays(3).format(formatter)
    sql "insert into dy_par_pq values ('${previous3Day}', 'd', 4);"
    def next1Day = currentDate.plusDays(1).format(formatter)
    sql "insert into dy_par_pq values ('${next1Day}', 'e', 5);"
    def next2Day = currentDate.plusDays(2).format(formatter)
    sql "insert into dy_par_pq values ('${next2Day}', 'f', 6);"
    def next3Day = currentDate.plusDays(3).format(formatter)
    sql "insert into dy_par_pq values ('${next3Day}', 'g', 7);"

    result = sql """
        select 
            * 
        from 
            dy_par_pq 
        where 
            k1 in (
            '${currenteDay}', '${previous1Day}', '${previous2Day}', '${previous3Day}',
            '${next1Day}', '${next2Day}', '${next3Day}') ;
    """
    assertEquals(result.size(), 7)
    explain {
        sql """
            select 
                * 
            from 
                dy_par_pq 
            where 
                k1 in (
                '${currenteDay}', '${previous1Day}', '${previous2Day}', '${previous3Day}',
                '${next1Day}', '${next2Day}', '${next3Day}') ;
        """
        contains "SHORT-CIRCUIT"
    }

    def previous4Day = currentDate.minusDays(4).format(formatter)
    def next4Day = currentDate.plusDays(4).format(formatter)
    result = sql """
        select 
            * 
        from 
            dy_par_pq 
        where 
            k1 in (
            '${currenteDay}', '${previous4Day}', '${next4Day}') ;
    """
    assertEquals(result.size(), 1)

    result = sql """
        select 
            * 
        from 
            dy_par_pq 
        where 
            k1 = '${currenteDay}' ;
    """
    assertEquals(result.size(), 1)

    result = sql """
        select 
            * 
        from 
            dy_par_pq 
        where 
            k1 = '${next4Day}' ;
    """
    assertEquals(result.size(), 0)
    
    sql "drop table dy_par_pq"

}
