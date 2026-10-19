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
create database if not exists nullable_migration;
use nullable_migration;
set enable_agg_state=true;
drop table if exists perf_input;
create table perf_input(id bigint not null,v bigint,d decimal(27,9),w double,s string)
duplicate key(id) distributed by hash(id) buckets 8 properties("replication_num"="1");
insert into perf_input select number, number%1000, cast(number%1000 as decimal(27,9)),cast(number%13+1 as double),concat(repeat('wide-',128),cast(number%127 as string)) from numbers("number"="4000000");
drop table if exists old_states;
create table old_states(g int,s agg_state<sum(bigint)> generic,a agg_state<avg(decimal(27,9))> generic,m agg_state<min(string)> generic,w agg_state<avg_weighted(double,double)> generic,b agg_state<bitmap_intersect(bitmap)> generic,c agg_state<collect_set(string)> generic,ar agg_state<array_agg(string)> generic,mp agg_state<map_agg_v2(string not null,string)> generic)
aggregate key(g) distributed by hash(g) buckets 1 properties("replication_num"="1","disable_auto_compaction"="true");
insert into old_states select cast(id%8 as int),sum_state(if(id%8=7,null,v)),avg_state(if(id%8=7,null,d)),min_state(if(id%8=7,null,s)),avg_weighted_state(if(id%8=7,null,cast(v as double)),w),bitmap_intersect_state(if(id%8=7,null,bitmap_from_string('1,2'))),collect_set_state(s),array_agg_state(s),map_agg_v2_state(cast(id as string),s) from perf_input where id<8000;
insert into old_states select cast(id%8 as int),sum_state(if(id%8=7,null,v)),avg_state(if(id%8=7,null,d)),min_state(if(id%8=7,null,s)),avg_weighted_state(if(id%8=7,null,cast(v as double)),w),bitmap_intersect_state(if(id%8=7,null,bitmap_from_string('1,2'))),collect_set_state(s),array_agg_state(s),map_agg_v2_state(cast(id as string),s) from perf_input where id>=8000 and id<16000;
select g,sum_merge(s),avg_merge(a),min_merge(m),avg_weighted_merge(w),bitmap_to_string(bitmap_intersect_merge(b)),array_size(collect_set_merge(c)),array_size(array_agg_merge(ar)),map_size(map_agg_v2_merge(mp)) from old_states group by g order by g;

-- Create this empty table on the reference build so its tablet schema stays version 19.
drop table if exists old_empty_bitmap_states;
create table old_empty_bitmap_states(g int,b agg_state<bitmap_intersect(bitmap)> generic)
aggregate key(g) distributed by hash(g) buckets 1
properties("replication_num"="1","disable_auto_compaction"="true");
