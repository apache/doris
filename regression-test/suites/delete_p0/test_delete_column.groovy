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

suite("test_delete_column","delete_p0") {
def tb_name_1 = "t1"
def tb_name_2 = "t2"
def tb_name_3 = "t3"
def tb_name_11 = "t11"
def tb_name_12 = "t12"

sql """ drop table if exists ${tb_name_1}; """
sql """ drop table if exists ${tb_name_2}; """
sql """ drop table if exists ${tb_name_3}; """
sql """ drop table if exists ${tb_name_11};"""
sql """ drop table if exists ${tb_name_12};"""

sql """ CREATE TABLE ${tb_name_1} (a tinyint(3), b tinyint(5)) ENGINE=OLAP DUPLICATE KEY(a, b) COMMENT "OLAP" DISTRIBUTED BY HASH(a) BUCKETS 3 PROPERTIES ( "replication_num" = "1" );"""
sql """ INSERT INTO  ${tb_name_1} VALUES (1,1); """
sql """ INSERT INTO  ${tb_name_1} VALUES (1,3); """
qt_sql """ DELETE from  ${tb_name_1} where a=1 ;   """
 
sql """ INSERT INTO  ${tb_name_1} VALUES (1,1); """
qt_sql """ DELETE from  ${tb_name_1} where a=1;  """
sql """ INSERT INTO  ${tb_name_1} VALUES (1,2);  """
sql """ INSERT INTO  ${tb_name_1} VALUES (1,2);  """
sql """ SET AUTOCOMMIT=0; """
qt_sql """ DELETE from  ${tb_name_1} where a=1; """
sql """ SET AUTOCOMMIT=1; """
qt_sql """ drop table  ${tb_name_1}; """

sql """ create table ${tb_name_1} (
  a bigint  NOT NULL COMMENT "",
  b bigint  NOT NULL COMMENT "",
  c bigint  NOT NULL COMMENT "",
  d bigint  NOT NULL COMMENT "",
  e bigint  NOT NULL COMMENT "",
  f bigint  NOT NULL COMMENT "",
  g bigint  NOT NULL COMMENT "",
  h bigint  NOT NULL COMMENT "",
  i bigint  NOT NULL COMMENT "",
  j bigint  NOT NULL COMMENT "")
  ENGINE=OLAP 
UNIQUE KEY (a,b,c,d,e,f,g,h,i,j)
DISTRIBUTED BY HASH (`a`) BUCKETS 4
PROPERTIES ( "replication_num" = "1" ); """
	
sql """ insert into ${tb_name_1} (a,b,c,d,e,f,g,h,i,j) values (2,4,6,8,10,12,14,16,18,20);  """
qt_sql """ delete from ${tb_name_1} where a=2;  """
qt_sql """ drop table ${tb_name_1};  """

sql """ CREATE TABLE `${tb_name_1}` (
  `i` int(10) NOT NULL default '0',
  `i2` int(10) NOT NULL default '0')
	UNIQUE KEY (`i`)
DISTRIBUTED BY HASH (`i`) BUCKETS 4
PROPERTIES ( "replication_num" = "1" );
"""
qt_sql """ drop table ${tb_name_1}; """


sql """ CREATE TABLE ${tb_name_1} (
  `bool`     char(10) default NULL,
  `not_null` varchar(20) NOT NULL default '',
  `misc`     int NOT NULL )
DUPLICATE KEY(`bool`) COMMENT 'OLAP'
DISTRIBUTED BY HASH(`bool`) BUCKETS 5
PROPERTIES ( "replication_num" = "1" );"""

sql """ INSERT INTO ${tb_name_1} VALUES (NULL,'a',4), (NULL,'b',5), (NULL,'c',6), (NULL,'d',7);"""

qt_sql """ select * from ${tb_name_1} where misc > 5 and bool is null;"""
qt_sql """ delete   from ${tb_name_1} where misc > 5 and bool is null;"""
qt_sql """ select * from ${tb_name_1} where misc > 5 and bool is null;"""

qt_sql """ select count(*) from ${tb_name_1};"""
qt_sql """ delete from ${tb_name_1} where misc > 2;"""
qt_sql """ select count(*) from ${tb_name_1};"""
qt_sql """ delete from ${tb_name_1} where misc > 2;"""
qt_sql """ select count(*) from ${tb_name_1};"""

qt_sql """ drop table ${tb_name_1};"""

sql """ create table ${tb_name_1} (a int not null, b char(32)) 
UNIQUE KEY (a)
DISTRIBUTED BY HASH (`a`) BUCKETS 4
PROPERTIES ( "replication_num" = "1" );"""

sql """ insert into ${tb_name_1} values (1,'apple'), (2,'apple'); """
qt_sql """ select * from ${tb_name_1}; """
qt_sql """ delete  from ${tb_name_1} where ${tb_name_1}.a=1; """
qt_sql """ select * from ${tb_name_1}; """
qt_sql """ drop table ${tb_name_1}; """

sql """ create table ${tb_name_11} (a int NOT NULL, b int)UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" );"""
sql """ create table ${tb_name_12} (a int NOT NULL, b int)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" );"""
sql """ create table ${tb_name_2} (a int NOT NULL, b int)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" );"""
sql """ insert into ${tb_name_11} values (0, 10),(1, 11),(2, 12);"""
sql """ insert into ${tb_name_12} values (33, 10),(0, 11),(2, 12);"""
sql """ insert into ${tb_name_2} values (1, 21),(2, 12),(3, 23);"""

qt_sql """ select count(1) from ${tb_name_11}; """
qt_sql """ select count(1) from ${tb_name_12}; """
qt_sql """ select count(1) from ${tb_name_2}; """

sql """ insert into ${tb_name_11} values (2, 12); """
qt_sql """ delete from ${tb_name_11} where ${tb_name_1}1.a = 2; """
qt_sql """ select count(1) from ${tb_name_11}; """

qt_sql """ drop table ${tb_name_11}; """
qt_sql """ drop table ${tb_name_12}; """
qt_sql """ drop table ${tb_name_2}; """

sql """ CREATE TABLE t (a INT)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" );"""
sql """ INSERT INTO t VALUES (1), (2), (3), (4), (5), (6), (7);"""

qt_sql """ DELETE FROM t WHERE a = 6;"""

qt_sql """ DROP TABLE t;"""

sql """ CREATE TABLE ${tb_name_1}(a INTEGER) UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_1} VALUES(10),(20); """

sql """ CREATE TABLE ${tb_name_2}(b INTEGER)DISTRIBUTED BY HASH (`b`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_2} VALUES(10),(20); """
sql """ SET SESSION sql_safe_updates=1;"""
qt_sql """ DELETE  FROM ${tb_name_1} WHERE a > 10 ; """
sql """ SET SESSION sql_safe_updates=default; """
qt_sql """ DROP TABLE ${tb_name_1}; """
qt_sql """ DROP TABLE ${tb_name_2}; """

sql """ create table ${tb_name_1} (a int, b int)UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ insert into ${tb_name_1} values (3, 3), (7, 7); """
qt_sql """ delete from ${tb_name_1} where a = 3; """
qt_sql """ select * from ${tb_name_1}; """
qt_sql """ drop table ${tb_name_1}; """
 
sql """ CREATE TABLE ${tb_name_1} ( a int  )UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
qt_sql """ DELETE FROM ${tb_name_1} WHERE ${tb_name_1}.a > 0 ; """
sql """ INSERT INTO ${tb_name_1} VALUES (0),(1),(2); """
qt_sql """ DELETE FROM ${tb_name_1} WHERE ${tb_name_1}.a > 0 ; """
qt_sql """ SELECT * FROM ${tb_name_1}; """
qt_sql """ DROP TABLE ${tb_name_1}; """

sql """ create table ${tb_name_1} (a int)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
qt_sql """ delete  from ${tb_name_1}  where a = 5; """
qt_sql """ drop table ${tb_name_1}; """

sql """ create table ${tb_name_1}(f1 int )UNIQUE KEY (f1) DISTRIBUTED BY HASH (`f1`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ insert into ${tb_name_1} values (4),(3),(1),(2); """
qt_sql """ drop table ${tb_name_1}; """

sql """ CREATE TABLE ${tb_name_1} (
  `seq` int(10) NOT NULL ,
  `date` date ,
  `time` datetime 
)UNIQUE KEY (seq) DISTRIBUTED BY HASH (`seq`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_1} VALUES (1,'2022-10-10','2022-10-10 10:10:10'); """
qt_sql """ select * from ${tb_name_1}; """
qt_sql """ DELETE FROM ${tb_name_1} where `seq`='1' ; """
qt_sql """ drop table ${tb_name_1}; """


sql """ CREATE TABLE ${tb_name_1} (a int not null,b int not null)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ CREATE TABLE ${tb_name_2} (a int not null, b int not null)UNIQUE KEY (a,b) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ CREATE TABLE ${tb_name_3} (a int not null, b int not null)UNIQUE KEY (a,b) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ insert into ${tb_name_1} values (1,1),(2,1),(1,3); """
sql """ insert into ${tb_name_2} values (1,1),(2,2),(3,3); """
sql """ insert into ${tb_name_3} values (1,1),(2,1),(1,3); """
qt_sql """ select * from ${tb_name_1},${tb_name_2},${tb_name_3} where ${tb_name_1}.a=${tb_name_2}.a AND ${tb_name_2}.b=${tb_name_3}.a and ${tb_name_1}.b=${tb_name_3}.b order by ${tb_name_1}.a,${tb_name_1}.b; """
qt_sql """ select * from ${tb_name_1},${tb_name_2},${tb_name_3} where ${tb_name_1}.a=${tb_name_2}.a AND ${tb_name_2}.b=${tb_name_3}.a and ${tb_name_1}.b=${tb_name_3}.b; """
qt_sql """ delete from ${tb_name_1} WHERE a= 1 and b=3 ; """
qt_sql """ delete from ${tb_name_2} WHERE a= 1 and b=3 ; """
qt_sql """ delete from ${tb_name_3} WHERE a= 1 and b=3 ; """
qt_sql """ select * from ${tb_name_3} order by a; """
qt_sql """ drop table ${tb_name_1}; """
qt_sql """ drop table ${tb_name_2}; """
qt_sql """ drop table ${tb_name_3}; """

sql """ create table ${tb_name_1}(a date not null)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ insert  into ${tb_name_1} values ('2022-10-11'); """
qt_sql """ select * from ${tb_name_1} ; """
qt_sql """ delete from ${tb_name_1} where a is null; """
qt_sql """ select count(*) from ${tb_name_1}; """
qt_sql """ drop table ${tb_name_1}; """

sql """ CREATE TABLE ${tb_name_1} ( a INT)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ CREATE TABLE ${tb_name_2} ( a INT)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ CREATE DATABASE db1; """
sql """ CREATE TABLE db1.${tb_name_1} ( a INT)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO db1.${tb_name_1} (a) SELECT * FROM ${tb_name_1}; """

sql """ CREATE DATABASE db2; """
sql """ CREATE TABLE db2.${tb_name_1} ( a INT)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO db2.${tb_name_1} (a) SELECT * FROM ${tb_name_2}; """

qt_sql """ DELETE FROM ${tb_name_1}  WHERE a = 1; """
qt_sql """ SELECT * FROM ${tb_name_1}; """

qt_sql """ DELETE FROM ${tb_name_1}  WHERE a = 2; """
qt_sql """ SELECT * FROM ${tb_name_1}; """

qt_sql """ DROP TABLE ${tb_name_1}; """
qt_sql """ DROP TABLE ${tb_name_2}; """
qt_sql """ DROP DATABASE db1; """
qt_sql """ DROP DATABASE db2; """

sql """ create table ${tb_name_1}(a date not null)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ insert  into ${tb_name_1} values ('2022-10-11'); """
qt_sql """ select * from ${tb_name_1} ; """
qt_sql """ delete from ${tb_name_1} where a is null; """
qt_sql """ select count(*) from ${tb_name_1}; """
qt_sql """ drop table ${tb_name_1}; """

sql """ CREATE TABLE ${tb_name_1}(a INT)DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ CREATE TABLE ${tb_name_2}(b INT)DISTRIBUTED BY HASH (`b`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """

sql """ INSERT INTO ${tb_name_1} VALUES (1); """
sql """ INSERT INTO ${tb_name_2} VALUES (1); """

qt_sql """ DELETE  FROM ${tb_name_1} WHERE a = 1; """

qt_sql """ DROP TABLE  ${tb_name_1} FORCE; """
qt_sql """ DROP TABLE  ${tb_name_2} FORCE; """

qt_sql """ drop table if exists b;"""
qt_sql """ drop VIEW if exists x;"""
sql """ CREATE TABLE b (a INT) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ CREATE MATERIALIZED VIEW y AS SELECT a FROM b AS e GROUP BY a; """
sql """ CREATE VIEW x AS SELECT 1 FROM b; """
qt_sql """ DROP TABLE b FORCE; """
qt_sql """ DROP VIEW x; """

sql """ CREATE TABLE ${tb_name_1} (a INT ) UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_1} (a) VALUES (1); """
sql """ CREATE TABLE ${tb_name_2} (a INT ) UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_2} (a) VALUES (1); """
sql """ CREATE TABLE ${tb_name_3} (a INT, b INT) UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_3} (a, b) VALUES (1, 1); """
qt_sql """ SELECT * FROM ${tb_name_1}; """
qt_sql """ SELECT * FROM ${tb_name_2}; """
qt_sql """ DROP TABLE ${tb_name_1}; """
qt_sql """ DROP TABLE ${tb_name_2}; """
qt_sql """ DROP TABLE ${tb_name_3}; """

sql """ CREATE TABLE ${tb_name_1}(a INT) UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ CREATE TABLE ${tb_name_2}(a VARCHAR) UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """

sql """ INSERT INTO ${tb_name_2} VALUES('a'); """
qt_sql """ DELETE FROM ${tb_name_2} WHERE a='a'; """
qt_sql """ DROP TABLE ${tb_name_1}; """
qt_sql """ DROP TABLE ${tb_name_2}; """

sql """ CREATE TABLE ${tb_name_1} (a INT, b CHAR(8), pk INT)UNIQUE KEY (a) DISTRIBUTED BY HASH (`a`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_1} (a,b) VALUES (10000,'foobar'),(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'); """
sql """ INSERT INTO ${tb_name_1} (a,b) SELECT a, b FROM ${tb_name_1}; """

sql """ CREATE TABLE ${tb_name_2} (pk INT, c CHAR(8), d INT)UNIQUE KEY (pk) DISTRIBUTED BY HASH (`pk`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_2} (c,d) SELECT b, a FROM ${tb_name_1}; """
qt_sql """ DELETE FROM ${tb_name_1} WHERE a is NOT NULL; """

qt_sql """ SELECT a,b FROM ${tb_name_1}; """

qt_sql """ DROP TABLE ${tb_name_1}; """
qt_sql """ DROP TABLE ${tb_name_2}; """

sql """ CREATE TABLE d3 (
                    pk int(11) NOT NULL ,
                    col_int int(11) DEFAULT NULL,
                    col_varchar varchar(1) DEFAULT NULL
                    
)UNIQUE KEY (pk) DISTRIBUTED BY HASH (`pk`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """

sql """ INSERT INTO d3 VALUES (96,9,'t'),(97,0,'x'); """

sql """ CREATE TABLE e3 (
                    col_varchar varchar(1) DEFAULT NULL,
                    pk int(11) NOT NULL 
) UNIQUE KEY (col_varchar) DISTRIBUTED BY HASH (`col_varchar`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """

sql """ INSERT INTO e3 VALUES ('v',986); """
qt_sql """ DELETE FROM d3 where pk is not null; """
qt_sql """ DELETE FROM e3 where col_varchar is  null; """

qt_sql """ DROP TABLE d3; """
qt_sql """ DROP TABLE e3; """

sql """ CREATE TABLE ${tb_name_1} (c1 INT) DISTRIBUTED BY HASH (`c1`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_1} VALUES (1), (2); """
qt_sql """ DELETE FROM ${tb_name_1}  WHERE c1 = 2; """
qt_sql """ SELECT * FROM ${tb_name_1}; """

sql """ CREATE TABLE ${tb_name_2} (c2 INT) DISTRIBUTED BY HASH (`c2`) BUCKETS 4 PROPERTIES ( "replication_num" = "1" ); """
sql """ INSERT INTO ${tb_name_2} VALUES (1), (2); """
qt_sql """ SELECT * FROM ${tb_name_2}; """
qt_sql """ DELETE FROM ${tb_name_1} WHERE c1 > 1; """
qt_sql """ SELECT * FROM ${tb_name_2}; """

qt_sql """ DROP TABLE ${tb_name_1}; """
qt_sql """ DROP TABLE ${tb_name_2}; """


}
