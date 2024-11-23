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

suite("sub_query_alias") {
    sql """
        SET enable_nereids_planner=true
    """

    sql "SET enable_fallback_to_original_planner=false"

    qt_drop_1 """ drop table if exsist customers_44271 """
    qt_drop_2 """ drop table if exsist orderitems_44271 """
    qt_drop_3 """ drop table if exsist orders_44271 """

    qt_create_1 """ 
        CREATE TABLE customers_44271 (
            cust_id int(11) NOT NULL,
            cust_name varchar(150) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(cust_id)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(cust_id) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
    """
    qt_create_2 """ 
        CREATE TABLE orderitems_44271 (
            order_num int(11) NOT NULL,
            order_item int(11) NOT NULL,
        prod_id varchar(30) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(order_num, order_item)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(order_num, order_item) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
    """

    qt_create_3 """ 
        CREATE TABLE orders_44271 (
            order_num int(11) NOT NULL,
            order_date datetime NOT NULL,
        cust_id int(11) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(order_num)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(order_num) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    qt_select_6 """ 
    SELECT
	    cust_name,
	    cust_id 
    FROM
	    customers_44271 cm 
    WHERE
    	cust_id IN (
    	SELECT
	    	cust_id 
	    FROM
		    orders_44271 
	    WHERE
	        order_num IN (  SELECT order_num FROM orderitems_44271  WHERE prod_id = 'TNT2' and cm.cust_name='Place'));
    """

    qt_drop_4 """ drop table if exsist customers_44271 """
    qt_drop_5 """ drop table if exsist orderitems_44271 """
    qt_drop_6 """ drop table if exsist orders_44271 """
}