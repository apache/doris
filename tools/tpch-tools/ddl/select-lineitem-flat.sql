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

SELECT l.*, o.*, p.*, ps.*, c.*, s.*,
       cn.n_nationkey AS c_n_nationkey, cn.n_name AS c_n_name,
       cn.n_regionkey AS c_n_regionkey, cn.n_comment AS c_n_comment,
       cr.r_regionkey AS c_r_regionkey, cr.r_name AS c_r_name, cr.r_comment AS c_r_comment,
       sn.n_nationkey AS s_n_nationkey, sn.n_name AS s_n_name,
       sn.n_regionkey AS s_n_regionkey, sn.n_comment AS s_n_comment,
       sr.r_regionkey AS s_r_regionkey, sr.r_name AS s_r_name, sr.r_comment AS s_r_comment
FROM lineitem l
LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey
LEFT JOIN part p ON l.l_partkey = p.p_partkey
LEFT JOIN partsupp ps ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey
LEFT JOIN customer c ON o.o_custkey = c.c_custkey
LEFT JOIN supplier s ON l.l_suppkey = s.s_suppkey
LEFT JOIN nation cn ON c.c_nationkey = cn.n_nationkey
LEFT JOIN region cr ON cn.n_regionkey = cr.r_regionkey
LEFT JOIN nation sn ON s.s_nationkey = sn.n_nationkey
LEFT JOIN region sr ON sn.n_regionkey = sr.r_regionkey
