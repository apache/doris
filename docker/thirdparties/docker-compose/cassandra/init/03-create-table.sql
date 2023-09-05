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

create table if not exists doris_test.test1(
    id              bigint,
    c_ascii         ascii,
    c_bigint        bigint,
    c_boolean       boolean,
    c_decimal       decimal,
    c_double        double,
    c_float         float,
    c_int           int,
    c_timestamp     timestamp,
    c_uuid          uuid,
    c_text          text,
    c_varint        varint,
    c_timeuuid      timeuuid,
    c_inet          inet,
    c_date          date,
    c_smallint      smallint,
    c_tinyint       tinyint,
    c_list          list<int>,
    c_set           set<int>,
    c_map           map<text,int>,
    PRIMARY KEY (id)
);
