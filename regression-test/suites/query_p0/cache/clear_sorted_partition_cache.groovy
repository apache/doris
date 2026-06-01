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

suite("clear_sorted_partition_cache") {
    multi_sql """
        set enable_sql_cache=false;
        set enable_binary_search_filtering_partitions=true;
        drop table if exists t_part_cache;
        create table t_part_cache(
            id int,
            dt int
        )
        partition by range(dt) (
            partition p20250101 values [(20250101), (20250102))
        )
        distributed by hash(id)
        properties("replication_num" = "1");
        alter table t_part_cache add temporary partition tp20250101 values [(20250101), (20250102));
        insert into t_part_cache temporary partition (tp20250101) select 1, 20250101;
        """
    // wait cache valid
    sleep(10000)
    // query the original empty partition to populate the sorted partition cache
    test {
        sql """
        select *
        from t_part_cache
        where dt = '20250101'
        """
        rowNum 0
    }
    sql "alter table t_part_cache replace partition p20250101 WITH TEMPORARY PARTITION tp20250101;"
    test {
        sql """
        select *
        from t_part_cache
        where dt = '20250101'
        """
        rowNum 1
    }
}
