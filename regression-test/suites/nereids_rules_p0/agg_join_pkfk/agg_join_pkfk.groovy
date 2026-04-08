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
suite("agg_join_pkfk") {
    multi_sql """
        SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject';
        set runtime_filter_mode=OFF;
        set disable_join_reorder=true;
        drop table if exists customer_test;
        CREATE TABLE customer_test (
                c_customer_sk INT not null ,
                        c_first_name VARCHAR(50),
                c_last_name VARCHAR(50)
        );
        drop table if exists store_sales_test;
        CREATE TABLE store_sales_test (
                ss_customer_sk INT,
                        d_date DATE
        );    

        INSERT INTO customer_test VALUES (1, 'John', 'Smith');
        INSERT INTO customer_test VALUES (2, 'John', 'Smith');
    
        INSERT INTO store_sales_test VALUES (1, '2024-01-01');
        INSERT INTO store_sales_test VALUES (2, '2024-01-01');
    
        alter table customer_test add constraint c_pk primary key (c_customer_sk);
        alter table store_sales_test add constraint ss_c_fk foreign key (ss_customer_sk) references customer_test(c_customer_sk);
    """
    explainAndOrderResult 'not_push_down', """
        SELECT DISTINCT c_last_name, c_first_name, d_date
        FROM store_sales_test inner join customer_test
        on store_sales_test.ss_customer_sk = customer_test.c_customer_sk;
    """

    explainAndOrderResult 'push_down', """
        SELECT DISTINCT c_first_name,c_customer_sk, d_date
        FROM store_sales_test inner join customer_test
        on store_sales_test.ss_customer_sk = customer_test.c_customer_sk;
    """

    explainAndOrderResult 'push_down_with_count', """
        SELECT  c_first_name,c_customer_sk, d_date,count(c_customer_sk) from (
        select * 
        FROM store_sales_test inner join customer_test
        on store_sales_test.ss_customer_sk = customer_test.c_customer_sk
        ) t
        group by  c_first_name,c_customer_sk, d_date;
    """
}