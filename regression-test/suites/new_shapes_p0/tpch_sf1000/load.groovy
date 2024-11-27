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

suite("load") {
    if (isCloudMode()) {
        return
    }
    String database = context.config.getDbNameByFile(context.file)
    sql "drop database if exists ${database}"
    sql "create database ${database}"
    sql "use ${database}"
    sql """
    drop table if exists lineitem;
    """
    sql """
    CREATE TABLE lineitem (
        l_shipdate    DATEV2 NOT NULL,
        l_orderkey    bigint NOT NULL,
        l_linenumber  int not null,
        l_partkey     int NOT NULL,
        l_suppkey     int not null,
        l_quantity    decimal(15, 2) NOT NULL,
        l_extendedprice  decimal(15, 2) NOT NULL,
        l_discount    decimal(15, 2) NOT NULL,
        l_tax         decimal(15, 2) NOT NULL,
        l_returnflag  VARCHAR(1) NOT NULL,
        l_linestatus  VARCHAR(1) NOT NULL,
        l_commitdate  DATEV2 NOT NULL,
        l_receiptdate DATEV2 NOT NULL,
        l_shipinstruct VARCHAR(25) NOT NULL,
        l_shipmode     VARCHAR(10) NOT NULL,
        l_comment      VARCHAR(44) NOT NULL
    )ENGINE=OLAP
    DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
        "replication_num" = "1",
        "colocate_with" = "lineitem_orders"
    );
    """

    sql """
    drop table if exists orders;
    """

    sql '''
    CREATE TABLE orders  (
        o_orderkey       bigint NOT NULL,
        o_orderdate      DATEV2 NOT NULL,
        o_custkey        int NOT NULL,
        o_orderstatus    VARCHAR(1) NOT NULL,
        o_totalprice     decimal(15, 2) NOT NULL,
        o_orderpriority  VARCHAR(15) NOT NULL,
        o_clerk          VARCHAR(15) NOT NULL,
        o_shippriority   int NOT NULL,
        o_comment        VARCHAR(79) NOT NULL
    )ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_orderdate`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
        "replication_num" = "1",
        "colocate_with" = "lineitem_orders"
    ); '''

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
    drop table if exists part;
    '''

    sql '''
    CREATE TABLE part (
        p_partkey          int NOT NULL,
        p_name        VARCHAR(55) NOT NULL,
        p_mfgr        VARCHAR(25) NOT NULL,
        p_brand       VARCHAR(10) NOT NULL,
        p_type        VARCHAR(25) NOT NULL,
        p_size        int NOT NULL,
        p_container   VARCHAR(10) NOT NULL,
        p_retailprice decimal(15, 2) NOT NULL,
        p_comment     VARCHAR(23) NOT NULL
    )ENGINE=OLAP
    DUPLICATE KEY(`p_partkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24
    PROPERTIES (
        "replication_num" = "1",
        "colocate_with" = "part_partsupp"
    );
    '''

    sql '''
    drop table if exists customer;
    '''

    sql '''
    CREATE TABLE customer (
        c_custkey     int NOT NULL,
        c_name        VARCHAR(25) NOT NULL,
        c_address     VARCHAR(40) NOT NULL,
        c_nationkey   int NOT NULL,
        c_phone       VARCHAR(15) NOT NULL,
        c_acctbal     decimal(15, 2)   NOT NULL,
        c_mktsegment  VARCHAR(10) NOT NULL,
        c_comment     VARCHAR(117) NOT NULL
    )ENGINE=OLAP
    DUPLICATE KEY(`c_custkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24
    PROPERTIES (
        "replication_num" = "1"
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
    
    sql '''
    drop view if exists revenue0;
    '''
    
    sql '''
    create view revenue0 (supplier_no, total_revenue) as
    select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
    from
        lineitem
    where
        l_shipdate >= date '1996-01-01'
        and l_shipdate < date '1996-01-01' + interval '3' month
    group by
        l_suppkey;
    '''
    
   
sql '''
alter table lineitem modify column l_shipdate set stats ('ndv'='2539', 'num_nulls'='0', 'min_value'='1992-01-02', 'max_value'='1998-12-01', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_orderkey set stats ('ndv'='1491920000', 'num_nulls'='0', 'min_value'='1', 'max_value'='6000000000', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_linenumber set stats ('ndv'='7', 'num_nulls'='0', 'min_value'='1', 'max_value'='7', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_partkey set stats ('ndv'='200778064', 'num_nulls'='0', 'min_value'='1', 'max_value'='200000000', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_suppkey set stats ('ndv'='10031328', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000000', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_quantity set stats ('ndv'='50', 'num_nulls'='0', 'min_value'='1.00', 'max_value'='50.00', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_extendedprice set stats ('ndv'='3793003', 'num_nulls'='0', 'min_value'='900.00', 'max_value'='104950.00', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_discount set stats ('ndv'='11', 'num_nulls'='0', 'min_value'='0.00', 'max_value'='0.10', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_tax set stats ('ndv'='9', 'num_nulls'='0', 'min_value'='0.00', 'max_value'='0.08', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_returnflag set stats ('ndv'='3', 'num_nulls'='0', 'min_value'='A', 'max_value'='R', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_linestatus set stats ('ndv'='2', 'num_nulls'='0', 'min_value'='F', 'max_value'='O', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_commitdate set stats ('ndv'='2473', 'num_nulls'='0', 'min_value'='1992-01-31', 'max_value'='1998-10-31', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_receiptdate set stats ('ndv'='2568', 'num_nulls'='0', 'min_value'='1992-01-03', 'max_value'='1998-12-31', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_shipinstruct set stats ('ndv'='4', 'num_nulls'='0', 'min_value'='COLLECT COD', 'max_value'='TAKE BACK RETURN', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_shipmode set stats ('ndv'='7', 'num_nulls'='0', 'min_value'='AIR', 'max_value'='TRUCK', 'row_count'='5999989709');
'''

sql '''
alter table lineitem modify column l_comment set stats ('ndv'='155259104', 'num_nulls'='0', 'min_value'=' Tiresias ', 'max_value'='zzle? unusual', 'row_count'='5999989709');
'''


sql '''
alter table orders modify column o_orderkey set stats ('ndv'='1491920000', 'num_nulls'='0', 'min_value'='1', 'max_value'='6000000000', 'row_count'='1500000000');
'''

sql '''
alter table orders modify column o_orderdate set stats ('ndv'='2417', 'num_nulls'='0', 'min_value'='1992-01-01', 'max_value'='1998-08-02', 'row_count'='1500000000');
'''

sql '''
alter table orders modify column o_custkey set stats ('ndv'='101410744', 'num_nulls'='0', 'min_value'='1', 'max_value'='149999999', 'row_count'='1500000000');
'''

sql '''
alter table orders modify column o_orderstatus set stats ('ndv'='3', 'num_nulls'='0', 'min_value'='F', 'max_value'='P', 'row_count'='1500000000');
'''

sql '''
alter table orders modify column o_totalprice set stats ('ndv'='41700404', 'num_nulls'='0', 'min_value'='810.87', 'max_value'='602901.81', 'row_count'='1500000000');
'''

sql '''
alter table orders modify column o_orderpriority set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='1-URGENT', 'max_value'='5-LOW', 'row_count'='1500000000');
'''

sql '''
alter table orders modify column o_clerk set stats ('ndv'='1013689', 'num_nulls'='0', 'min_value'='Clerk#000000001', 'max_value'='Clerk#001000000', 'row_count'='1500000000');
'''

sql '''
alter table orders modify column o_shippriority set stats ('ndv'='1', 'num_nulls'='0', 'min_value'='0', 'max_value'='0', 'row_count'='1500000000');
'''

sql '''
alter table orders modify column o_comment set stats ('ndv'='272632352', 'num_nulls'='0', 'min_value'=' Tiresias about the', 'max_value'='zzle? unusual requests w', 'row_count'='1500000000');
'''


sql '''
alter table partsupp modify column ps_partkey set stats ('ndv'='200778064', 'num_nulls'='0', 'min_value'='1', 'max_value'='200000000', 'row_count'='800000000');
'''

sql '''
alter table partsupp modify column ps_suppkey set stats ('ndv'='10031328', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000000', 'row_count'='800000000');
'''

sql '''
alter table partsupp modify column ps_availqty set stats ('ndv'='10008', 'num_nulls'='0', 'min_value'='1', 'max_value'='9999', 'row_count'='800000000');
'''

sql '''
alter table partsupp modify column ps_supplycost set stats ('ndv'='100279', 'num_nulls'='0', 'min_value'='1.00', 'max_value'='1000.00', 'row_count'='800000000');
'''

sql '''
alter table partsupp modify column ps_comment set stats ('ndv'='303150816', 'num_nulls'='0', 'min_value'=' Tiresias about the accounts detect quickly final foxes. instructions about the blithely unusual theodolites use blithely f', 'max_value'='zzle? unusual requests wake slyly. slyly regular requests are e', 'row_count'='800000000');
'''



sql '''
alter table part modify column p_partkey set stats ('ndv'='200778064', 'num_nulls'='0', 'min_value'='1', 'max_value'='200000000', 'row_count'='200000000');
'''

sql '''
alter table part modify column p_name set stats ('ndv'='196191408', 'num_nulls'='0', 'min_value'='almond antique aquamarine azure blush', 'max_value'='yellow white wheat violet red', 'row_count'='200000000');
'''

sql '''
alter table part modify column p_mfgr set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='Manufacturer#1', 'max_value'='Manufacturer#5', 'row_count'='200000000');
'''

sql '''
alter table part modify column p_brand set stats ('ndv'='25', 'num_nulls'='0', 'min_value'='Brand#11', 'max_value'='Brand#55', 'row_count'='200000000');
'''

sql '''
alter table part modify column p_type set stats ('ndv'='150', 'num_nulls'='0', 'min_value'='ECONOMY ANODIZED BRASS', 'max_value'='STANDARD POLISHED TIN', 'row_count'='200000000');
'''

sql '''
alter table part modify column p_size set stats ('ndv'='50', 'num_nulls'='0', 'min_value'='1', 'max_value'='50', 'row_count'='200000000');
'''

sql '''
alter table part modify column p_container set stats ('ndv'='40', 'num_nulls'='0', 'min_value'='JUMBO BAG', 'max_value'='WRAP PKG', 'row_count'='200000000');
'''

sql '''
alter table part modify column p_retailprice set stats ('ndv'='120904', 'num_nulls'='0', 'min_value'='900.00', 'max_value'='2099.00', 'row_count'='200000000');
'''

sql '''
alter table part modify column p_comment set stats ('ndv'='14213541', 'num_nulls'='0', 'min_value'=' Tire', 'max_value'='zzle? speci', 'row_count'='200000000');
'''



sql '''
alter table supplier modify column s_suppkey set stats ('ndv'='10031328', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000000', 'row_count'='10000000');
'''

sql '''
alter table supplier modify column s_name set stats ('ndv'='9992858', 'num_nulls'='0', 'min_value'='Supplier#000000001', 'max_value'='Supplier#010000000', 'row_count'='10000000');
'''

sql '''
alter table supplier modify column s_address set stats ('ndv'='10000390', 'num_nulls'='0', 'min_value'='   04SJW3NWgeWBx2YualVtK62DXnr', 'max_value'='zzzzr MaemffsKy', 'row_count'='10000000');
'''

sql '''
alter table supplier modify column s_nationkey set stats ('ndv'='25', 'num_nulls'='0', 'min_value'='0', 'max_value'='24', 'row_count'='10000000');
'''

sql '''
alter table supplier modify column s_phone set stats ('ndv'='9975965', 'num_nulls'='0', 'min_value'='10-100-101-9215', 'max_value'='34-999-999-3239', 'row_count'='10000000');
'''

sql '''
alter table supplier modify column s_acctbal set stats ('ndv'='1109296', 'num_nulls'='0', 'min_value'='-999.99', 'max_value'='9999.99', 'row_count'='10000000');
'''

sql '''
alter table supplier modify column s_comment set stats ('ndv'='9854117', 'num_nulls'='0', 'min_value'=' Customer  accounts are blithely furiousRecommends', 'max_value'='zzle? special packages haggle carefully regular inst', 'row_count'='10000000');
'''



sql '''
alter table customer modify column c_custkey set stats ('ndv'='151682592', 'num_nulls'='0', 'min_value'='1', 'max_value'='150000000', 'row_count'='150000000');
'''

sql '''
alter table customer modify column c_name set stats ('ndv'='149989056', 'num_nulls'='0', 'min_value'='Customer#000000001', 'max_value'='Customer#150000000', 'row_count'='150000000');
'''

sql '''
alter table customer modify column c_address set stats ('ndv'='149316720', 'num_nulls'='0', 'min_value'='    2WGW,hiM7jHg2', 'max_value'='zzzzyW,aeC8HnFV', 'row_count'='150000000');
'''

sql '''
alter table customer modify column c_nationkey set stats ('ndv'='25', 'num_nulls'='0', 'min_value'='0', 'max_value'='24', 'row_count'='150000000');
'''

sql '''
alter table customer modify column c_phone set stats ('ndv'='150226160', 'num_nulls'='0', 'min_value'='10-100-100-3024', 'max_value'='34-999-999-9215', 'row_count'='150000000');
'''

sql '''
alter table customer modify column c_acctbal set stats ('ndv'='1109296', 'num_nulls'='0', 'min_value'='-999.99', 'max_value'='9999.99', 'row_count'='150000000');
'''

sql '''
alter table customer modify column c_mktsegment set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='AUTOMOBILE', 'max_value'='MACHINERY', 'row_count'='150000000');
'''

sql '''
alter table customer modify column c_comment set stats ('ndv'='120255488', 'num_nulls'='0', 'min_value'=' Tiresias about the accounts haggle quiet, busy foxe', 'max_value'='zzle? special accounts about the iro', 'row_count'='150000000');
'''



sql '''
alter table region modify column r_regionkey set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='5');
'''

sql '''
alter table region modify column r_name set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='AFRICA', 'max_value'='MIDDLE EAST', 'row_count'='5');
'''

sql '''
alter table region modify column r_comment set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='ges. thinly even pinto beans ca', 'max_value'='uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl', 'row_count'='5');
'''



sql '''
alter table nation modify column n_nationkey set stats ('ndv'='25', 'num_nulls'='0', 'min_value'='0', 'max_value'='24', 'row_count'='25');
'''

sql '''
alter table nation modify column n_name set stats ('ndv'='25', 'num_nulls'='0', 'min_value'='ALGERIA', 'max_value'='VIETNAM', 'row_count'='25');
'''

sql '''
alter table nation modify column n_regionkey set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='25');
'''

sql '''
alter table nation modify column n_comment set stats ('ndv'='25', 'num_nulls'='0', 'min_value'=' haggle. carefully final deposits detect slyly agai', 'max_value'='y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be', 'row_count'='25');
'''

}
