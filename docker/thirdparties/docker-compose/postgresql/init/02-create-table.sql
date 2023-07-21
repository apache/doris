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

create table doris_test.test1 (
    k1 bit,
    k2 char(100),
    k3 varchar(128),
    k4 date,
    k5 real,
    k6 smallint,
    k7 int,
    k8 bigint,
    k9 timestamp,
    k10 decimal(10, 3)
);

CREATE TABLE doris_test.test2 (
        id int8 NOT NULL,
        c_user varchar(500) NULL,
        c_time timestamp(0) NULL,
        m_user varchar(500) NULL,
        m_time timestamp(0) NULL,
        app_id int8 NULL,
        t_id int8 NULL,
        deleted bool NULL DEFAULT false,
        code varchar(255) NOT NULL DEFAULT ''::character varying,
        name varchar(500) NOT NULL DEFAULT ''::character varying,
        cate_id int8 NULL,
        tag varchar(255) NOT NULL DEFAULT ''::character varying,
        remark varchar(500) NULL,
        CONSTRAINT p_dict PRIMARY KEY (id)
);

create table doris_test.test2_item (
        id int8 NOT NULL,
        c_user varchar(500) NULL,
        c_time timestamp(0) NULL,
        m_user varchar(500) NULL,
        m_time timestamp(0) NULL,
        app_id int8 NULL,
        t_id int8 NULL,
        deleted bool NULL DEFAULT false,
        dict_id int8 NOT NULL,
        o_idx int2 NULL,
        label varchar(255) NULL,
        value varchar(500) NULL,
        CONSTRAINT p_dict_item PRIMARY KEY (id)
);

CREATE OR REPLACE VIEW doris_test.test2_view
AS SELECT item.id as id,
    dict.code as code,
    item.value as value,
    item.label as label,
    item.deleted as deleted,
    item.o_idx as o_idx
FROM doris_test.test2 dict
LEFT JOIN doris_test.test2_item item ON item.dict_id = dict.id
WHERE dict.deleted = false AND item.deleted = false;

create table doris_test.test3 (
  id int,
  name varchar(20)
);

create table doris_test.test4 (
  id int
);

CREATE TABLE doris_test.test5 (
	id serial4 NOT NULL,
	"result" jsonb null
);

CREATE TABLE doris_test.test6 (
	id serial4 NOT NULL,
	"result" jsonb null,
	"limit" varchar(60) null
);

CREATE TABLE doris_test.test7 (
 	id serial4 NOT NULL,
 	"QueryResult" jsonb null
);

CREATE TABLE doris_test.test8 (
  "id" int8 NOT NULL,
  "c_user" varchar(100) COLLATE "pg_catalog"."default",
  "c_time" timestamp(6),
  "m_user" varchar(100) COLLATE "pg_catalog"."default",
  "m_time" timestamp(6),
  "app_id" int8,
  "t_id" int8,
  "deleted" bool DEFAULT false,
  "w_t_s" timestamp(6),
  "rf_id" varchar(64) COLLATE "pg_catalog"."default",
  "e_info" jsonb,
  "f_id" int8,
  "id_code" varchar(255) COLLATE "pg_catalog"."default",
  CONSTRAINT "f_a_w_r_p" PRIMARY KEY ("id")
);

CREATE TABLE doris_test.test9 (
  id1 smallint,
  id2 int,
  id3 bool,
  id4 varchar(10),
  id5 bigint
);

CREATE TABLE catalog_pg_test.test10 (
   ID INT NOT NULL,
   bit_value bit,
   real_value real,
   cidr_value cidr,
   inet_value inet,
   macaddr_value macaddr,
   bitn_value bit(10),
   bitnv_value bit varying(10),
   serial4_value serial4,
   jsonb_value jsonb
);

CREATE TABLE catalog_pg_test.test11 (
   ID INT PRIMARY KEY NOT NULL,
   point_value point,
   line_value line,
   lseg_value lseg,
   box_value box,
   path_value path,
   polygon_value polygon,
   circle_value circle
);

CREATE TABLE catalog_pg_test.test12 (
   ID INT NOT NULL,
   uuid_value uuid
);

CREATE TABLE catalog_pg_test.test_insert (
   id varchar(128),
   name varchar(128),
   age int
);

CREATE TABLE catalog_pg_test.wkb_test (
   id SERIAL PRIMARY KEY,
   location bytea
);

CREATE TABLE catalog_pg_test.dt_test (
    ts_field TIMESTAMP(3),
    tzt_field TIMESTAMPTZ(3)
);

CREATE TABLE catalog_pg_test.json_test (
    id serial PRIMARY KEY,
    type varchar(10),
    value json
);

CREATE TABLE catalog_pg_test.jsonb_test (
    id serial PRIMARY KEY,
    type varchar(10),
    value jsonb
);
