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

suite("test_pk_fk_drop_table") {
    // case 1
    multi_sql """drop table if exists store_sales_test;
    drop table if exists customer_test;
    create table customer_test(c_customer_sk int,c_customer_id int);
    CREATE TABLE `store_sales_test` (
    `ss_customer_sk` int NULL,
    `d_date` date NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ss_customer_sk`, `d_date`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );
    alter table customer_test add constraint c_pk primary key(c_customer_sk);
    alter table store_sales_test add constraint ss_c_fk foreign key(ss_customer_sk) references customer_test(c_customer_sk);
    drop table store_sales_test;"""
    // expect: successful drop
    sql "alter table customer_test drop constraint c_pk;"

    multi_sql """
    drop table if exists store_sales_test;
    drop table if exists customer_test;
    create table customer_test(c_customer_sk int,c_customer_id int) PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );
    CREATE TABLE `store_sales_test` (
    `ss_customer_sk` int NULL,
    `d_date` date NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ss_customer_sk`, `d_date`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );
    alter table customer_test add constraint c_pk primary key(c_customer_sk);
    alter table store_sales_test add constraint ss_c_fk foreign key(ss_customer_sk) references customer_test(c_customer_sk);
    drop table customer_test;
    """
    // expect: not throw
    sql "show constraints from store_sales_test;"
}