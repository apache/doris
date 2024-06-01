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

suite("smoke_test_create_table_with_partition", "smoke") {
    sql "drop table if exists lineorder"

    sql """
        CREATE TABLE IF NOT EXISTS lineorder (
            lo_orderdate int(11) NOT NULL COMMENT "",
            lo_orderkey bigint(20) NOT NULL COMMENT "",
            lo_linenumber bigint(20) NOT NULL COMMENT "",
            lo_custkey int(11) NOT NULL COMMENT "",
            lo_partkey int(11) NOT NULL COMMENT "",
            lo_suppkey int(11) NOT NULL COMMENT "",
            lo_orderpriority varchar(64) NOT NULL COMMENT "",
            lo_shippriority int(11) NOT NULL COMMENT "",
            lo_quantity bigint(20) NOT NULL COMMENT "",
            lo_extendedprice bigint(20) NOT NULL COMMENT "",
            lo_ordtotalprice bigint(20) NOT NULL COMMENT "",
            lo_discount bigint(20) NOT NULL COMMENT "",
            lo_revenue bigint(20) NOT NULL COMMENT "",
            lo_supplycost bigint(20) NOT NULL COMMENT "",
            lo_tax bigint(20) NOT NULL COMMENT "",
            lo_commitdate bigint(20) NOT NULL COMMENT "",
            lo_shipmode varchar(64) NOT NULL COMMENT "" )
        ENGINE=OLAP
        UNIQUE KEY(lo_orderdate, lo_orderkey, lo_linenumber)
        COMMENT "OLAP"
        PARTITION BY RANGE(lo_orderdate) (
        PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 48;
        """

    sql """
        insert into lineorder (lo_orderdate, lo_orderkey, lo_linenumber, lo_custkey,
            lo_partkey, lo_suppkey, lo_orderpriority, lo_shippriority,
            lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount,
            lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode)
            values (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1);
        """

    sql """
        select * from lineorder;
        """
}
