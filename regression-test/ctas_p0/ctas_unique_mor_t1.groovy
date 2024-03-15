
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

suite("ctas_unique_mor_t1") {
    def tbName = "ctas_unique_mor_t1"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
        CREATE TABLE ${tbName}
        (
            `id` LARGEINT NOT NULL COMMENT "id",
            `decimal_col` DECIMAL(10, 2) COMMENT "DECIMAL列",
            `date_col` date NOT NULL COMMENT "date列",
            `nullable_col` VARCHAR(20) COMMENT "nullable列",
            `varchar_col` VARCHAR COMMENT "VARCHAR 默认值列",
            `test` BIGINT DEFAULT "0" COMMENT "test列"
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        
        """
    sql """
        INSERT INTO ${tbName} (`id`, `decimal_col`, `date_col`, `nullable_col`, `varchar_col`, `test`)
        VALUES
        (1, 12.34, '2023-09-12', 'Nullable1', '1', 100),
        (2, 45.67, '2023-09-13', 'Nullable2', '2', 200),
        (3, 78.90, '2023-09-14', 'Nullable3', '3', 300),
        (4, 98.76, '2023-09-15', 'Nullable4', '4', 400),
        (5, 54.32, '2023-09-16', 'Nullable5', '5', 500),
        (6, 21.43, '2023-09-17', 'Nullable6', '6', 600),
        (7, 65.98, '2023-09-18', 'Nullable7', '7', 700),
        (8, 87.61, '2023-09-19', 'Nullable8', '8', 800),
        (9, 34.56, '2023-09-20', 'Nullable9', '9', 900),
        (10, 76.89, '2023-09-21', 'Nullable10', '1', 1000);
    
    """

    qt_sql1 """ 
        create table ctas_unique_mor_t2
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ) 
        as select * from ${tbName};
             
     """

     qt_sql2 """ 
        desc ctas_unique_mor_t2 all;
             
     """

    qt_sql3 """ 
        create table ctas_unique_mor_t3
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ) 
        as select id, decimal_col from ${tbName}; 
    """

    qt_sql4 """ 
        desc ctas_unique_mor_t3 all;
             
     """


    qt_sql5 """ 
        create table ctas_unique_mor_t4
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ) 
        as select id, decimal_col, nullable_col, varchar_col, test from ${tbName}; 
    """

    qt_sql6 """ 
        desc ctas_unique_mor_t4 all;
             
     """

    qt_sql7 """ 
        create table ctas_unique_mor_t5
        DISTRIBUTED BY HASH(cast_id) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        as select cast(varchar_col as varchar(250)) as cast_varchar, cast(id as int
        ) as cast_id from ${tbName};  
    """

    qt_sql8 """ 
        desc ctas_unique_mor_t5 all;
             
     """

    sql "DROP TABLE ${tbName}"
}
