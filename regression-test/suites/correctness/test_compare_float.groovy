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

suite("test_compare_float") {
    
    sql """
        DROP TABLE IF EXISTS sort_float;
    """


    sql """
    CREATE TABLE IF NOT EXISTS sort_float (
              `id` INT(11),
              `d` double
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """


    sql """
        insert into sort_float values(1,'123'),(2,'-123'),(3,'114514'),(4,'+inf'),(5,'-inf'),(6,'-0'),(7,'+0'),(8,'Nan'),(9,'Nan');
    """

    qt_sql """
        select * from sort_float order by id;
    """

    qt_sql1 """
        select * from sort_float order by d;
    """

    qt_sql2 """
        select d, d < cast('Nan' as double) from sort_float order by d;
    """

    qt_sql3 """
        select d, d <= cast('Nan' as double) from sort_float order by d;
    """

    qt_sql4 """
        select d, d > cast('Nan' as double) from sort_float order by d;
    """

    qt_sql5 """
        select d, d >= cast('Nan' as double) from sort_float order by d;
    """


    qt_sql6 """
        select d, d = cast('Nan' as double) from sort_float order by d;
    """

    qt_sql7 """
        select d, d != cast('Nan' as double) from sort_float order by d;
    """


    qt_sql8 """
        select max_by(id,d), min_by(id,d) from sort_float;
    """
}
