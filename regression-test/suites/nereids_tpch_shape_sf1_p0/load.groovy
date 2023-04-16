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
    alter table lineitem modify column l_receiptdate set stats ('ndv'='2567', 'avg_size'='16', 'max_size'='16', 'num_nulls'='0', 'min_value'='1992-01-04', 'max_value'='1998-12-31', 'row_count'='6001215')
    '''
    
    sql '''
    alter table lineitem modify column l_returnflag set stats ('ndv'='3', 'avg_size'='1', 'max_size'='1', 'num_nulls'='0', 'min_value'='A', 'max_value'='R', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_tax set stats ('ndv'='9', 'avg_size'='16', 'max_size'='16', 'num_nulls'='0', 'min_value'='0', 'max_value'='0.08', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_shipmode set stats ('ndv'='7', 'avg_size'='4', 'max_size'='7', 'num_nulls'='0', 'min_value'='AIR', 'max_value'='TRUCK', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_suppkey set stats ('ndv'='9990', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_shipdate set stats ('ndv'='2549', 'avg_size'='16', 'max_size'='16', 'num_nulls'='0', 'min_value'='1992-01-02', 'max_value'='1998-12-01', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_commitdate set stats ('ndv'='2485', 'avg_size'='16', 'max_size'='16', 'num_nulls'='0', 'min_value'='1992-01-31', 'max_value'='1998-10-31', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_partkey set stats ('ndv'='196099', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000', 'row_count'='6001215');
    '''

    sql '''
    alter table lineitem modify column l_orderkey set stats ('ndv'='1500000', 'avg_size'='8', 'max_size'='8', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_quantity set stats ('ndv'='50', 'avg_size'='16', 'max_size'='16', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_linestatus set stats ('ndv'='2', 'avg_size'='1', 'max_size'='1', 'num_nulls'='0', 'min_value'='F', 'max_value'='o', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_comment set stats ('ndv'='4619207', 'avg_size'='26', 'max_size'='43', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_extendedprice set stats ('ndv'='929697', 'avg_size'='16', 'max_size'='16', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_linenumber set stats ('ndv'='7', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='7', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_discount set stats ('ndv'='11', 'avg_size'='16', 'max_size'='16', 'num_nulls'='0', 'min_value'='0', 'max_value'='0.1', 'row_count'='6001215');
    '''
    
    sql '''
    alter table lineitem modify column l_shipinstruct set stats ('ndv'='4', 'avg_size'='12', 'max_size'='17', 'num_nulls'='0', 'min_value'='COLLECT COD', 'max_value'='TAKE BACK RETURN', 'row_count'='6001215');
    '''
    
    sql '''
    alter table partsupp modify column ps_suppkey set stats ('ndv'='10009', 'avg_size'='4.0', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000', 'row_count'='800000');
    '''
    
    sql '''
    alter table partsupp modify column ps_availqty set stats ('ndv'='10008', 'avg_size'='4.0', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='9999', 'row_count'='800000');
    '''
    
    sql '''
    alter table partsupp modify column ps_comment set stats ('ndv'='794782', 'avg_size'='123.6', 'max_size'='198', 'num_nulls'='0', 'min_value'='Tiresias according to the quiet courts sleep against the ironic', 'max_value'='zzle. unusual decoys detec', 'row_count'='800000');
    '''
    
    sql '''
    alter table partsupp modify column ps_partkey set stats ('ndv'='196099', 'avg_size'='4.0', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='200000', 'row_count'='800000');
    '''
    
    sql '''
    alter table partsupp modify column ps_supplycost set stats ('ndv'='100274', 'avg_size'='16.0', 'max_size'='16', 'num_nulls'='0', 'min_value'='1', 'max_value'='1000', 'row_count'='800000');
    '''
    
    sql '''
    alter table supplier modify column s_comment set stats ('ndv'='10039', 'avg_size'='62.56950', 'max_size'='100', 'num_nulls'='0', 'min_value'='about the blithely express foxes. bli', 'max_value'='zzle furiously. bold accounts haggle fu', 'row_count'='10000');
    '''
    
    sql '''
    alter table supplier modify column s_phone set stats ('ndv'='10021', 'avg_size'='15.0', 'max_size'='15', 'num_nulls'='0', 'min_value'='10-102-116-6785', 'max_value'='34-998-900-4911', 'row_count'='10000');
    '''
    
    sql '''
    alter table supplier modify column s_nationkey set stats ('ndv'='25', 'avg_size'='4.0', 'max_size'='4', 'num_nulls'='0', 'min_value'='0', 'max_value'='24', 'row_count'='10000');
    '''
    
    sql '''
    alter table supplier modify column s_name set stats ('ndv'='10002', 'avg_size'='18.0', 'max_size'='18', 'num_nulls'='0', 'min_value'='Supplier#000000001', 'max_value'='Supplier#000010000', 'row_count'='10000');
    '''
    
    sql '''
    alter table supplier modify column s_acctbal set stats ('ndv'='9954', 'avg_size'='16.0', 'max_size'='16', 'num_nulls'='0', 'min_value'='-998.22', 'max_value'='9999.72', 'row_count'='10000');
    '''
    
    sql '''
    alter table supplier modify column s_address set stats ('ndv'='9888', 'avg_size'='24.9771', 'max_size'='40', 'num_nulls'='0', 'min_value'='   9aW1wwnBJJPnCx,n', 'max_value'='zzfDhdtZcvmVzA8rNFU,Yctj1zBN', 'row_count'='10000');
    '''
    
    sql '''
    alter table supplier modify column s_suppkey set stats ('ndv'='10000', 'avg_size'='4.0', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000', 'row_count'='10000');
    '''
    
    sql '''
    alter table part modify column p_partkey set stats ('ndv'='200000', 'avg_size'='4.0', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='200000', 'row_count'='200000');
    '''
    
    sql '''
    alter table part modify column p_container set stats ('ndv'='40', 'avg_size'='7.57', 'max_size'='10', 'num_nulls'='0', 'min_value'='JUMBO BAG', 'max_value'='WRAP PKG', 'row_count'='200000');
    '''
    
    sql '''
    alter table part modify column p_name set stats ('ndv'='200000', 'avg_size'='32.0', 'max_size'='50', 'num_nulls'='0', 'min_value'='almond antique blue royal burnished', 'max_value'='yellow white seashell lavender black', 'row_count'='200000');
    '''
    
    sql '''
    alter table part modify column p_comment set stats ('ndv'='133106', 'avg_size'='13', 'max_size'='22', 'num_nulls'='0', 'min_value'='Tire', 'max_value'='zzle. quickly si', 'row_count'='200000');
    '''
    
    sql '''
    alter table part modify column p_brand set stats ('ndv'='25', 'avg_size'='22', 'max_size'='22', 'num_nulls'='0', 'min_value'='Brand#11', 'max_value'='Brand#55', 'row_count'='200000');
    '''
    
    sql '''
    alter table part modify column p_retailprice set stats ('ndv'='21096', 'avg_size'='16.0', 'max_size'='16', 'num_nulls'='0', 'min_value'='901', 'max_value'='2098.99', 'row_count'='200000');
    '''
    
    sql '''
    alter table part modify column p_type set stats ('ndv'='150', 'avg_size'='20', 'max_size'='25', 'num_nulls'='0', 'min_value'='ECONOMY ANODIZED BRASSe', 'max_value'='STANDARD POLISHED TIN ', 'row_count'='200000');
    '''
    
    sql '''
    alter table part modify column p_size set stats ('ndv'='50', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='50', 'row_count'='200000');
    '''
    
    sql '''
    alter table part modify column p_mfgr set stats ('ndv'='5', 'avg_size'='14', 'max_size'='14', 'num_nulls'='0', 'min_value'='Manufacturer#1', 'max_value'='Manufacturer#5', 'row_count'='200000');
    '''
    
    sql '''
    alter table region modify column r_regionkey set stats ('ndv'='5', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='5');
    '''
    
    sql '''
    alter table region modify column r_name set stats ('ndv'='5', 'avg_size'='14', 'max_size'='14', 'num_nulls'='0', 'min_value'='AFRICA', 'max_value'='MIDDLE EAST', 'row_count'='5');
    '''
    
    sql '''
    alter table region modify column r_comment set stats ('ndv'='5', 'avg_size'='14', 'max_size'='14', 'num_nulls'='0', 'min_value'='AFRICA', 'max_value'='MIDDLE EAST', 'row_count'='5');
    '''
    
    sql '''
    alter table nation modify column n_nationkey set stats ('ndv'='25', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='0', 'max_value'='24', 'row_count'='25');
    '''
    
    sql '''
    alter table nation modify column n_regionkey set stats ('ndv'='5', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='25');
    '''
    
    sql '''
    alter table nation modify column n_name set stats ('ndv'='25', 'avg_size'='14', 'max_size'='14', 'num_nulls'='0', 'min_value'='ALGERIA', 'max_value'='VIETNAM', 'row_count'='25');
    '''
    
    sql '''
    alter table nation modify column n_comment set stats ('ndv'='25', 'avg_size'='14', 'max_size'='14', 'num_nulls'='0', 'min_value'='ALGERIA', 'max_value'='VIETNAM', 'row_count'='25');
    '''
    
    sql '''
    alter table customer modify column c_custkey set stats ('ndv'='150000', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='150000', 'row_count'='150000');
    '''
    
    sql '''
    alter table customer modify column c_acctbal set stats ('ndv'='142496', 'avg_size'='16', 'max_size'='16', 'num_nulls'='0', 'min_value'='-999.99', 'max_value'='9999.99', 'row_count'='150000');
    '''
    
    sql '''
    alter table customer modify column c_phone set stats ('ndv'='150000', 'avg_size'='15', 'max_size'='15', 'num_nulls'='0', 'min_value'='10-100-106-1617', 'max_value'='34-999-618-6881', 'row_count'='150000');
    '''
    
    sql '''
    alter table customer modify column c_mktsegment set stats ('ndv'='5', 'avg_size'='8.9', 'max_size'='10', 'num_nulls'='0', 'min_value'='AUTOMOBILE', 'max_value'='MACHINERY', 'row_count'='150000');
    '''
    
    sql '''
    alter table customer modify column c_address set stats ('ndv'='150000', 'avg_size'='25', 'max_size'='40', 'num_nulls'='0', 'min_value'='   2uZwVhQvwA', 'max_value'='zzxGktzXTMKS1BxZlgQ9nqQ', 'row_count'='150000');
    '''
    
    sql '''
    alter table customer modify column c_nationkey set stats ('ndv'='25', 'avg_size'='4', 'max_size'='4', 'num_nulls'='0', 'min_value'='0', 'max_value'='24', 'row_count'='150000');
    '''
    
    sql '''
    alter table customer modify column c_name set stats ('ndv'='150000', 'avg_size'='25', 'max_size'='25', 'num_nulls'='0', 'min_value'='Customer#000000001', 'max_value'='Customer#000150000', 'row_count'='150000');
    '''
    
    sql '''
    alter table customer modify column c_comment set stats ('ndv'='150000', 'avg_size'='72', 'max_size'='116', 'num_nulls'='0', 'min_value'=' Tiresias according to the sly', 'max_value'='zzle. blithely regu0', 'row_count'='150000');
    '''
    
    sql '''
    alter table orders modify column o_orderstatus set stats ('ndv'='3', 'avg_size'='1.0', 'max_size'='1', 'num_nulls'='0', 'min_value'='F', 'max_value'='P', 'row_count'='1500000');
    '''
    
    sql '''
    alter table orders modify column o_clerk set stats ('ndv'='988', 'avg_size'='15.0', 'max_size'='15', 'num_nulls'='0', 'min_value'='Clerk#000000001', 'max_value'='Clerk#000001000', 'row_count'='1500000');
    '''
    
    sql '''
    alter table orders modify column o_orderdate set stats ('ndv'='2417', 'avg_size'='16.0', 'max_size'='16', 'num_nulls'='0', 'min_value'='1992-01-01', 'max_value'='1998-08-02', 'row_count'='1500000');
    '''
    
    sql '''
    alter table orders modify column o_shippriority set stats ('ndv'='1', 'avg_size'='4.0', 'max_size'='4', 'num_nulls'='0', 'min_value'='0', 'max_value'='0', 'row_count'='1500000');
    '''
    
    sql '''
    alter table orders modify column o_custkey set stats ('ndv'='99149', 'avg_size'='4.0', 'max_size'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='149999', 'row_count'='1500000');
    '''
    
    sql '''
    alter table orders modify column o_totalprice set stats ('ndv'='1462416', 'avg_size'='16.0', 'max_size'='16', 'num_nulls'='0', 'min_value'='857.71', 'max_value'='555285.16', 'row_count'='1500000');
    '''
    
    sql '''
    alter table orders modify column o_orderkey set stats ('ndv'='1500000', 'avg_size'='8.0', 'max_size'='8', 'num_nulls'='0', 'min_value'='1', 'max_value'='6000000', 'row_count'='1500000');
    '''
    
    sql '''
    alter table orders modify column o_comment set stats ('ndv'='1465415', 'avg_size'='48.51387', 'max_size'='78', 'num_nulls'='0', 'min_value'='Tiresias about the blithely ironic a', 'max_value'='zzle? furiously ironic instructions among the unusual t ', 'row_count'='1500000');
    '''
    
    sql '''
    alter table orders modify column o_orderpriority set stats ('ndv'='5', 'avg_size'='8.4', 'max_size'='15', 'num_nulls'='0', 'min_value'='1-URGENT', 'max_value'='5-LOW', 'row_count'='1500000');
    '''
}
