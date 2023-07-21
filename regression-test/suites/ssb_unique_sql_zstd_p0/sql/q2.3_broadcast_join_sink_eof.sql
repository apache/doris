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

SELECT
    /*+SET_VAR(experimental_enable_pipeline_engine=false,parallel_fragment_exec_instance_num=2)*/
    SUM(lo_revenue),
    d_year,
    p_brand
FROM
    lineorder
    join [broadcast] date on lo_orderdate = d_datekey
    join [broadcast] part on lo_partkey = p_partkey
    join [broadcast] supplier on lo_suppkey = s_suppkey
WHERE
    p_brand = 'MFGR#2239'
    AND s_region = 'EUROPE'
GROUP BY
    d_year,
    p_brand
ORDER BY
    d_year,
    p_brand;
