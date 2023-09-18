#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

separator='\t'
# hdfs_data_path='hdfs://xxxxxx'
# broker_property="WITH BROKER 'hdfs' ('username'='root', 'password'='')"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "DROP DATABASE IF EXISTS ${FE_DB}"
mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create database ${FE_DB}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.baseall(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date not null, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap partition by list(k10) (partition p1 values in ('1989-03-21','1991-08-11'), partition p2 values in ('2015-04-02','2012-03-14'), partition p3 values in ('1988-03-21','2014-11-11'), partition p4 values in ('1901-12-31','3124-10-10'), partition p5 values in ('2015-01-01','9999-12-12')) distributed by hash(k1) buckets 5 properties('storage_type'='column')"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.test(k1 tinyint not null, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap partition by list(k1) (partition p1 values in ('-127','-126','-125','-124','-123','-122','-121','-120','-119','-118','-117','-116','-115','-114','-113','-112'),  partition p2 values in ('-111','-110','-109','-108','-107','-106','-105','-104','-103','-102','-101','-100','-99','-98','-97','-96'),  partition p3 values in ('-95','-94','-93','-92','-91','-90','-89','-88','-87','-86','-85','-84','-83','-82','-81','-80'),  partition p4 values in ('-79','-78','-77','-76','-75','-74','-73','-72','-71','-70','-69','-68','-67','-66','-65','-64'),  partition p5 values in ('-63','-62','-61','-60','-59','-58','-57','-56','-55','-54','-53','-52','-51','-50','-49','-48'),  partition p6 values in ('-47','-46','-45','-44','-43','-42','-41','-40','-39','-38','-37','-36','-35','-34','-33','-32'),  partition p7 values in ('-31','-30','-29','-28','-27','-26','-25','-24','-23','-22','-21','-20','-19','-18','-17','-16'),  partition p8 values in ('-15','-14','-13','-12','-11','-10','-9','-8','-7','-6','-5','-4','-3','-2','-1','0'),  partition p9 values in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16'),  partition p10 values in ('17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32'),  partition p11 values in ('33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48'),  partition p12 values in ('49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64'),  partition p13 values in ('65','66','67','68','69','70','71','72','73','74','75','76','77','78','79','80'),  partition p14 values in ('81','82','83','84','85','86','87','88','89','90','91','92','93','94','95','96'),  partition p15 values in ('97','98','99','100','101','102','103','104','105','106','107','108','109','110','111','112'),  partition p16 values in ('113','114','115','116','117','118','119','120','121','122','123','124','125','126','127')) distributed by hash(k1) buckets 5 properties('storage_type'='column')"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "create table ${FE_DB}.bigtable(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5) not null, k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap partition by list(k6) (partition p1 values in ('true'), partition p2 values in ('false')) distributed by hash(k1) buckets 5 properties('storage_type'='column')"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_1 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE baseall COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_2 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE test COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_3 (DATA INFILE('${hdfs_data_path}/qe/xaaa') INTO TABLE test COLUMNS TERMINATED BY '${separator}') ${broker_property}"

mysql -h "${FE_HOST}" -P"${FE_QUERY_PORT}" -u"${FE_USER}" -p"${FE_PASSWORD}" -e "LOAD LABEL ${FE_DB}.label_4 (DATA INFILE('${hdfs_data_path}/qe/baseall.txt') INTO TABLE bigtable COLUMNS TERMINATED BY '${separator}') ${broker_property}"


sleep 20
