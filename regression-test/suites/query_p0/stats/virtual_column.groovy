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
suite("virtual_column") {
    sql """
    drop table if exists virtual_column_table;
    create table virtual_column_table (
        k1 int null,
        v1 int null
    ) distributed by hash(k1) buckets 1 properties('replication_num' = '1');

    insert into virtual_column_table values (1,1),(2,2),(3,3);
    alter table virtual_column_table modify column k1 set stats ('row_count'='20', 'ndv'='25', 'min_value'='0', 'max_value'='25');
    alter table virtual_column_table modify column v1 set stats ('row_count'='20', 'ndv'='25', 'min_value'='0', 'max_value'='25');

    """
    explain {
        /*
        PhysicalResultSink[103] ( outputExprs=[k1#0] )
            +--PhysicalProject[99]@2 ( stats=18, projects=[k1#0] )
                +--PhysicalFilter[95]@1 ( stats=18, predicates=AND[((cast(k1 as BIGINT) + cast(v1 as BIGINT))#2 >= 0),(abs((cast(k1 as BIGINT) + cast(v1 as BIGINT))#2) > 5)] )
                    +--PhysicalOlapScan[virtual_column_table]@0 ( stats=20, operativeSlots=[k1#0, v1#1, (cast(k1 as BIGINT) + cast(v1 as BIGINT))#2], virtualColumns=[(cast(k1#0 as BIGINT) + cast(v1#1 as BIGINT)) AS `(cast(k1 as BIGINT) + cast(v1 as BIGINT))`#2] ) 
        */
        sql "physical plan select k1 from virtual_column_table where k1 + v1 >= 0 and abs(k1+v1) > 5;"
        contains "stats=18"
    }
}