-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

INSERT INTO lineorder_flat
SELECT
    lo_orderdate, lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey,
    lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice,
    lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode,
    c_name, c_address, c_city, c_nation, c_region, c_phone, c_mktsegment,
    s_name, s_address, s_city, s_nation, s_region, s_phone,
    p_name, p_mfgr, p_category, p_brand, p_color, p_type, p_size, p_container
FROM lineorder l
INNER JOIN customer c ON c.c_custkey = l.lo_custkey
INNER JOIN supplier s ON s.s_suppkey = l.lo_suppkey
INNER JOIN part p ON p.p_partkey = l.lo_partkey
WHERE lo_orderdate >= @YEAR@0101 AND lo_orderdate < @NEXT_YEAR@0101;
