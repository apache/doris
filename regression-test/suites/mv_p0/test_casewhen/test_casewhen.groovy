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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_casewhen") {

    sql """ DROP TABLE IF EXISTS sales_records; """

    sql """
             create table sales_records(record_id int, seller_id int, store_id int, sale_date date, sale_amt bigint) distributed by hash(record_id) properties("replication_num" = "1");
        """

    sql """insert into sales_records values(1,1,1,"2020-02-02",11),(1,1,1,"2020-02-02",1);"""

    createMV ("create materialized view store_amt as select store_id, sum(case when sale_amt>10 then 1 else 2 end) from sales_records group by store_id;")

    sql """insert into sales_records values(1,1,1,"2020-02-02",1),(1,2,2,"2020-02-02",1);"""

    qt_select_star "select * from sales_records order by 1,2;"

    explain {
        sql("select store_id, sum(case when sale_amt>10 then 1 else 2 end) from sales_records group by store_id order by 1;")
        contains "(store_amt)"
    }
    qt_select_mv "select store_id, sum(case when sale_amt>10 then 1 else 2 end) from sales_records group by store_id order by 1;"
}
