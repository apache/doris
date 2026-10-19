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
-- Run with the reference binary before copying the task's tablet/meta directories.
use nullable_migration;
set enable_agg_state=true;
drop table if exists old_perf_states;
create table old_perf_states (
 g int,
 s0 agg_state<sum(bigint)> generic, a0 agg_state<avg(decimal(27,9))> generic,
 s1 agg_state<sum(bigint)> generic, a1 agg_state<avg(decimal(27,9))> generic,
 s50 agg_state<sum(bigint)> generic, a50 agg_state<avg(decimal(27,9))> generic,
 s100 agg_state<sum(bigint)> generic, a100 agg_state<avg(decimal(27,9))> generic,
 w agg_state<avg_weighted(double,double)> generic,
 m agg_state<min(string)> generic, c agg_state<collect_set(string)> generic
) aggregate key(g) distributed by hash(g) buckets 4
properties("replication_num"="1","disable_auto_compaction"="true");
insert into old_perf_states
select cast(id%100000 as int),sum_state(v),avg_state(d),
 sum_state(if(id*17%100<1,null,v)),avg_state(if(id*17%100<1,null,d)),
 sum_state(if(id*17%100<50,null,v)),avg_state(if(id*17%100<50,null,d)),
 sum_state(cast(null as bigint)),avg_state(cast(null as decimal(27,9))),
 avg_weighted_state(if(id%5=0,null,cast(v as double)),if(id%11=0,null,w)),
 min_state(if(id%5=0,null,s)),collect_set_state(if(id%5=0,null,s))
from perf_input where id<200000;
insert into old_perf_states
select cast(id%100000 as int),sum_state(v),avg_state(d),
 sum_state(if(id*17%100<1,null,v)),avg_state(if(id*17%100<1,null,d)),
 sum_state(if(id*17%100<50,null,v)),avg_state(if(id*17%100<50,null,d)),
 sum_state(cast(null as bigint)),avg_state(cast(null as decimal(27,9))),
 avg_weighted_state(if(id%5=0,null,cast(v as double)),if(id%11=0,null,w)),
 min_state(if(id%5=0,null,s)),collect_set_state(if(id%5=0,null,s))
from perf_input where id>=200000 and id<400000;
