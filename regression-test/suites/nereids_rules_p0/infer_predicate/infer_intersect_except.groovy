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

suite("infer_intersect_except") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql """SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"""
    sql 'set runtime_filter_mode=off'
    sql 'set enable_fold_constant_by_be=true'
    sql 'set debug_skip_fold_constant=false'



    sql "drop table if exists infer_intersect_except1"
    sql "drop table if exists infer_intersect_except2"
    sql "drop table if exists infer_intersect_except3"

    sql """
    CREATE TABLE `infer_intersect_except1` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE `infer_intersect_except2` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE `infer_intersect_except3` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    insert into infer_intersect_except1 values(1,'d2',3,5),(0,'d2',3,5),(-3,'d2',2,2),(-2,'d2',2,2);
    """

    sql """
    insert into infer_intersect_except2 values(1,'d2',2,2),(-3,'d2',2,2),(0,'d2',3,5);
    """

    sql """
    insert into infer_intersect_except3 values(1,'d2',2,2),(-2,'d2',2,2),(0,'d2',3,5);
    """
    qt_except """
    explain shape plan 
    select a,b from infer_intersect_except1 where a>0 except select a,b from infer_intersect_except2 where b>'ab' except select a,b from infer_intersect_except2 where a<10;
    """

    qt_except_to_empty """
    explain shape plan 
    select a,b from infer_intersect_except1 where a>0 except select a,b from infer_intersect_except2 where b>'ab' except select a,b from infer_intersect_except3 where a<0;
    """

    qt_except_not_infer_1_greater_than_0 """
    explain shape plan 
    select a,b from infer_intersect_except1 where a>0 except select 1,'abc' from infer_intersect_except2 where b>'ab' except select a,b from infer_intersect_except2 where a<0;
    """

    qt_except_number_and_string """
    explain shape plan 
    select a,2 from infer_intersect_except1 where a>0 except select 1,'abc' from infer_intersect_except2 where b>'ab' except select a,b from infer_intersect_except3 where a<0;
    """

    qt_intersect """
    explain shape plan
    select a,b from infer_intersect_except1 where a>0 intersect select a,b from infer_intersect_except2 where b>'ab';
    """
    qt_intersect_empty """
    explain shape plan
    select a,b from infer_intersect_except1 where a>0 intersect select a,b from infer_intersect_except2 where a<0;
    """

    qt_intersect_expr """
    explain shape plan
    select a+1,b from infer_intersect_except1 where a>0 intersect select a+1,b from infer_intersect_except2 where a+1<0;
    """

    qt_except_and_intersect """
    explain shape plan
    select a,b from infer_intersect_except1 where a>0 except select 1,'abc' from infer_intersect_except2 where b>'ab' intersect select a,b from infer_intersect_except3 where a<10;
    """

    qt_except_and_intersect_except_predicate_to_right """
    explain shape plan
    select a,b from infer_intersect_except1 where a>0 except select a,'abc' from infer_intersect_except2 where b>'ab' intersect select a,b from infer_intersect_except3 where a<10;
    """
    qt_intersect_and_except """
    explain shape plan
    select a,b from infer_intersect_except1 where a>0 intersect select 1,'abc' from infer_intersect_except2 where b>'ab' except select a,b from infer_intersect_except3 where a<10;
    """

    qt_function_intersect """
    explain shape plan
    select abs(a) from infer_intersect_except1 t1 where abs(a)<3 intersect select abs(a) from infer_intersect_except2 t2  """
    qt_function_except """
    explain shape plan
    select abs(a) from infer_intersect_except1 t1 where abs(a)<3 except select abs(a) from infer_intersect_except2 t2    """

    qt_except_res """
    (select a,b from infer_intersect_except1 where a>0) except (select a,b from infer_intersect_except2 where b>'ab') except (select a,b from infer_intersect_except2 where a<10) order by 1,2;
    """

    qt_except_to_empty_res """
    (select a,b from infer_intersect_except1 where a>0) except (select a,b from infer_intersect_except2 where b>'ab') except (select a,b from infer_intersect_except3 where a<0)  order by 1,2;
    """

    qt_except_not_infer_1_greater_than_0_res """
    (select a,b from infer_intersect_except1 where a>0) except (select 1,'abc' from infer_intersect_except2 where b>'ab') except (select a,b from infer_intersect_except2 where a<0) order by 1,2;
    """

    qt_except_number_and_string_res """
    (select a,2 from infer_intersect_except1 where a>0) except (select 1,'abc' from infer_intersect_except2 where b>'ab') except (select a,b from infer_intersect_except3 where a<0) order by 1,2;
    """

    qt_intersect_res """
    (select a,b from infer_intersect_except1 where a>0) intersect (select a,b from infer_intersect_except2 where b>'ab') order by 1,2;
    """
    qt_intersect_empty_res """
    (select a,b from infer_intersect_except1 where a>0) intersect (select a,b from infer_intersect_except2 where a<0) order by 1,2;
    """

    qt_intersect_expr_res """
    (select a+1,b from infer_intersect_except1 where a>0) intersect (select a+1,b from infer_intersect_except2 where a+1<0 ) order by 1,2;
    """

    qt_except_and_intersect_res """
    (select a,b from infer_intersect_except1 where a>0) except (select 1,'abc' from infer_intersect_except2 where b>'ab') intersect (select a,b from infer_intersect_except3 where a<10) order by 1,2;
    """

    qt_except_and_intersect_except_predicate_to_right_res """
    (select a,b from infer_intersect_except1 where a>0) except (select a,'abc' from infer_intersect_except2 where b>'ab') intersect (select a,b from infer_intersect_except3 where a<10)  order by 1,2;
    """
    qt_intersect_and_except_res """
    (select a,b from infer_intersect_except1 where a>0) intersect (select 1,'abc' from infer_intersect_except2 where b>'ab') except (select a,b from infer_intersect_except3 where a<10)  order by 1,2;
    """

    qt_function_intersect_res """
    (select abs(a) from infer_intersect_except1 t1 where abs(a)<3) intersect (select abs(a) from infer_intersect_except2 t2)  order by 1  """
    qt_function_except_res """
    (select abs(a) from infer_intersect_except1 t1 where abs(a)<3) except (select abs(a) from infer_intersect_except2 t2)  order by 1  """

    sql "drop table if exists infer_intersect_except4"
    sql "create table infer_intersect_except4(d_int int, d_char100 char(100), d_smallint smallint, d_tinyint tinyint, d_char10 char(10),d_datetimev2 datetimev2, d_datev2 datev2) properties('replication_num'='1');"
    sql """insert into infer_intersect_except4 values(1,'01234567890123456789', 3,3,'0123456789','2020-01-09 10:00:00.99','2020-01-09'),(14,'01234567890123456789', 33,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11')
            ,(14,'01234567890123456789', 33,23,'2024-01-04','2020-01-11 10:00:00.99','2020-01-11'),
            (14,'01234567890123456789', 33,23,'2024-01-03 10:00:00','2020-01-11 10:00:00.99','2020-01-11');"""

    test {
        sql """
        select d_datetimev2 from infer_intersect_except4 where d_datetimev2>'2020-01-01'  intersect select d_int from infer_intersect_except4 where d_int<10;
        """
        exception("can not cast from origin type DATETIMEV2(0) to target type=UNSUPPORTED")
    }
    qt_different_type_date_string """
    explain shape plan
    select d_datetimev2 from infer_intersect_except4 where d_datetimev2>'2020-01-01'  intersect select d_char100 from infer_intersect_except4 where d_char100<'abc';
    """
    qt_different_type_int_string """
    explain shape plan
    select d_int from infer_intersect_except4 where d_int>2  intersect select d_char100 from infer_intersect_except4 where d_char100<'abc';
    """
    qt_different_type_date_string_res """
    (select d_datetimev2 from infer_intersect_except4 where d_datetimev2>'2020-01-01')  intersect (select d_char100 from infer_intersect_except4 where d_char100<'abc') order by 1;
    """
    qt_different_type_int_string_res """
    (select d_int from infer_intersect_except4 where d_int>2 ) intersect (select d_char100 from infer_intersect_except4 where d_char100<'abc') order by 1;
    """
}