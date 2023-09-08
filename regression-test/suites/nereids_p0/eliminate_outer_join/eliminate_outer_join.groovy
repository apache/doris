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

suite("eliminate_outer_join") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules='ELIMINATE_NOT_NULL'"
    sql "set disable_join_reorder=true"

    String database = context.config.getDbNameByFile(context.file)
    sql "drop database if exists ${database}"
    sql "create database ${database}"
    sql "use ${database}"

    sql '''
    drop table if exists partsupp;
    '''

    sql '''
    CREATE TABLE partsupp (
        ps_partkey          int NOT NULL,
        ps_suppkey     int NOT NULL,
        ps_availqty    int NOT NULL,
        ps_supplycost  decimal(15, 2)  NOT NULL,
        ps_comment     VARCHAR(199) NOT NULL
    )ENGINE=OLAP
    DUPLICATE KEY(`ps_partkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
    PROPERTIES (
        "replication_num" = "1",
        "colocate_with" = "part_partsupp"
    );
    '''

    sql '''
    drop table if exists supplier
    '''
    
    sql '''
    CREATE TABLE supplier (
        s_suppkey       int NOT NULL,
        s_name        VARCHAR(25) NOT NULL,
        s_address     VARCHAR(40) NOT NULL,
        s_nationkey   int NOT NULL,
        s_phone       VARCHAR(15) NOT NULL,
        s_acctbal     decimal(15, 2) NOT NULL,
        s_comment     VARCHAR(101) NOT NULL
    )ENGINE=OLAP
    DUPLICATE KEY(`s_suppkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
    PROPERTIES (
        "replication_num" = "1"
    );
    '''

    sql '''
    drop table if exists nation;
    '''

    sql '''
    CREATE TABLE `nation` (
    `n_nationkey` int(11) NOT NULL,
    `n_name`      varchar(25) NOT NULL,
    `n_regionkey` int(11) NOT NULL,
    `n_comment`   varchar(152) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`N_NATIONKEY`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );
    '''
    
    sql '''
    drop table if exists region;
    '''
    
    sql '''
    CREATE TABLE region  (
        r_regionkey      int NOT NULL,
        r_name       VARCHAR(25) NOT NULL,
        r_comment    VARCHAR(152)
    )ENGINE=OLAP
    DUPLICATE KEY(`r_regionkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    );
    '''
    
    qt_1 '''
    --eliminate outer joins in cascading
    explain shape plan
    select * 
    from region
        left join nation on r_regionkey=n_regionkey -->inner
        left join supplier on n_nationkey=s_suppkey -->inner
        left join partsupp on ps_suppkey=s_suppkey -->inner
    where ps_suppkey > 1
    '''

    qt_2 '''
    -- full join ps => right join ps, other outer joins are not eliminated
    explain shape plan
    select * 
    from region
        full join nation on r_regionkey=n_regionkey  -->full
        full join supplier on n_nationkey=s_suppkey -->full
        full join partsupp on ps_suppkey=s_suppkey -->right
    where ps_suppkey > 1;
    '''

    qt_3 '''
    explain shape plan
    select * 
    from region
        full join nation on r_regionkey=n_regionkey -- full
        left join supplier on n_nationkey=s_suppkey -- left
        full join partsupp on ps_suppkey=s_suppkey  -- right
    where ps_suppkey > 1;
    '''

    qt_4 '''
    explain shape plan
    select * 
    from region
        full join nation on r_regionkey=n_regionkey -- left
    where r_name = "";
    '''

    qt_5 '''
    explain shape plan
    select * 
    from region
        full join nation on r_regionkey=n_regionkey -- left
        left join supplier on n_nationkey=s_suppkey -- left
    where r_name = "";
    '''

    qt_6 '''
    explain shape plan
    select * 
    from region
        full join nation on r_regionkey=n_regionkey -- left
        left join supplier on n_nationkey=s_suppkey -- left
        full join partsupp on ps_suppkey=s_suppkey  -- left
    where r_name = "";
    '''

    qt_7'''
    explain shape plan
        select * 
        from region
            join nation on r_regionkey=n_regionkey      -- inner
            left join supplier on n_nationkey=s_suppkey  -- left
            full join partsupp on ps_suppkey=s_suppkey;  -- full
    '''

    qt_8'''
    explain shape plan
        select * 
        from region
            join nation on r_regionkey=n_regionkey      --inner
            left join supplier on n_nationkey=s_suppkey --left
            full join partsupp on ps_suppkey=s_suppkey  --left
        where r_name = "";
    '''

    qt_9 '''
    explain shape plan
        select * 
        from region
            join nation on r_regionkey=n_regionkey      --inner
            right join supplier on n_nationkey=s_suppkey --inner
            full join partsupp on ps_suppkey=s_suppkey  --left
        where r_name = "";
        '''
}
