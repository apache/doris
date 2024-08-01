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

SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select 1 in (1, null);
select 1 in (2, null);

select 1 not in (1, null);
select 1 not in (2, null);

select 1 in (2, null, 1);
select 1 in (null, 2);

select 1 not in (null, 1);
select 1 not in (null, 2);


select timestamp '2008-08-08 00:00:00' in ('2008-08-08');
select case when true then timestamp '2008-08-08 00:00:00' else '2008-08-08' end;

select 1 is true, 1 is not true, 0 is false, 0 is not false;
select -1 is true, -1 is not true, -0 is false, -1 is not false;
select 1.1 is true, 1.1 is not true, 1.1 is false, 1.1 is not false;
select 'ab' is true, 'ab' is not true, 'ab' is false, 'ab' is not false;
select cast('2028-01-01' as date) is true, cast('2028-01-01' as date) is not true, cast('2028-01-01' as date) is false, cast('2028-01-01' as date) is not false;
select cast('2028-01-01 01:00:00' as datetime) is true, cast('2028-01-01 01:00:00'  as datetime) is not true, cast('2028-01-01 01:00:00' as datetime) is false, cast('2028-01-01 01:00:00' as datetime) is not false;