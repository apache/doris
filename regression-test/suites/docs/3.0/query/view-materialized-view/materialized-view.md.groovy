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

import org.junit.jupiter.api.Assertions;

suite("docs/3.0/query/view-materialized-view/materialized-view.md") {
    try {
        multi_sql """
        create database demo;
        create table demo.sales_records(record_id int, seller_id int, store_id int, sale_date date, sale_amt bigint) distributed by hash(record_id);
        insert into demo.sales_records values(1,1,1,"2020-02-02",1);
        desc sales_records;
        create materialized view store_amt as select store_id, sum(sale_amt) from sales_records group by store_id;
        SHOW ALTER TABLE MATERIALIZED VIEW FROM demo;
        SELECT store_id, sum(sale_amt) FROM demo.sales_records GROUP BY store_id;
        EXPLAIN SELECT store_id, sum(sale_amt) FROM sales_records GROUP BY store_id;
        """

        multi_sql """
        create table demo.d_table (
           k1 int null,
           k2 int not null,
           k3 bigint null,
           k4 date null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3;
        
        insert into demo.d_table select 1,1,1,'2020-02-20';
        insert into demo.d_table select 2,2,2,'2021-02-20';
        insert into demo.d_table select 3,-3,null,'2022-02-20';
        
        select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from demo.d_table group by abs(k1)+k2+1;
        select bin(abs(k1)+k2+1),sum(abs(k2+2)+k3+3) from demo.d_table group by bin(abs(k1)+k2+1);
        select year(k4),month(k4) from demo.d_table;
        select year(k4)+month(k4) from demo.d_table where year(k4) = 2020;
        """
      
      
    } catch (Throwable t) {
        Assertions.fail("examples in docs/3.0/query/view-materialized-view/materialized-view.md failed to exec, please fix it", t)
    }
}
