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

suite("test_ctas_auto_partition") {
    sql "drop table if exists test_partition__dbt_tmp3"
    test {
        sql """
            create table `test_partition__dbt_tmp3`
            AUTO PARTITION BY RANGE (date_trunc(`today`, 'day')) ()
            PROPERTIES ("replication_num" = "1" )
            as
            select nullable(current_date()) as today;
        """
        exception "AUTO RANGE PARTITION doesn't support NULL column"
    }

    test {
        sql """
            create table `test_partition__dbt_tmp3`
            AUTO PARTITION BY RANGE (date_trunc(`today`, 'day')) ()
            PROPERTIES ("replication_num" = "1" )
            as
            select cast(null as date) as today;
        """
        exception "AUTO RANGE PARTITION doesn't support NULL column"
    }

    test {
        sql """
            create table `test_partition__dbt_tmp3`
            AUTO PARTITION BY RANGE (date_trunc(`today`, 'day')) ()
            PROPERTIES ("replication_num" = "1" )
            as
            select 1 as today;
        """
        exception "partition expr date_trunc is illegal!"
    }

    test{
        sql """
            create table `test_partition__dbt_tmp3`
            AUTO PARTITION BY RANGE (date_trunc(`today`, 'day'), date_trunc(`today1`, 'day')) ()
            PROPERTIES ("replication_num" = "1" )
            as
            select 1 as today;
        """
        exception "auto create partition only support one slotRef in function expr"
    }

    test{
        sql """
            create table `test_partition__dbt_tmp3`
            AUTO PARTITION BY RANGE (date_trunc(`today`, 'day')) ()
            PROPERTIES ("replication_num" = "1" )
            as
            select current_date() as today, current_date() + 1 as today;
        """
        exception "Duplicate column name 'today'"
    }

    test{
        sql """
            create table `test_partition__dbt_tmp3`
            AUTO PARTITION BY RANGE (date_trunc(`today`, 'day')) ()
            PROPERTIES ("replication_num" = "1" )
            as
            select current_date() as today2, current_date() + 1 as today1;
        """
        exception "partition key today is not exists"
    }

    sql """
        create table `test_partition__dbt_tmp3`
        AUTO PARTITION BY RANGE (date_trunc(`today`, 'day')) ()
        PROPERTIES ("replication_num" = "1" )
        as
        select 1 as id, 'cyril' as name, 18 as age, 100 as score, current_date() as today;
    """
    assertEquals((sql "select count() from test_partition__dbt_tmp3")[0][0], 1)
}