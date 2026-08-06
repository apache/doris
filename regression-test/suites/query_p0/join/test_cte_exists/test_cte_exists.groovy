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

suite("test_cte_exists") {
    def tableName = "table_20_50_undef_partitions2_keys3_properties4_distributed_by5"
    
    sql """ DROP TABLE IF EXISTS ${tableName} """
    
    sql "set enable_left_semi_direct_return_opt = true; "

    sql "set parallel_pipeline_task_num=16;"

    sql "set runtime_filter_max_in_num=10;"

    sql "set runtime_filter_type = 'IN';"

    sql "set enable_local_shuffle = true;"
    
    sql """
        create table ${tableName} (
            pk int,
            col_int_undef_signed int    ,
            col_int_undef_signed__index_inverted int    ,
            col_decimal_20_5__undef_signed decimal(20,5)    ,
            col_decimal_20_5__undef_signed__index_inverted decimal(20,5)    ,
            col_varchar_100__undef_signed varchar(100)    ,
            col_varchar_100__undef_signed__index_inverted varchar(100)    ,
            col_char_50__undef_signed char(50)    ,
            col_char_50__undef_signed__index_inverted char(50)    ,
            col_string_undef_signed string    ,
            col_date_undef_signed date    ,
            col_date_undef_signed__index_inverted date    ,
            col_datetime_undef_signed datetime    ,
            col_datetime_undef_signed__index_inverted datetime    ,
            col_tinyint_undef_signed tinyint    ,
            col_tinyint_undef_signed__index_inverted tinyint    ,
            col_decimal_10_0__undef_signed decimal(10,0)    ,
            col_decimal_10_0__undef_signed__index_inverted decimal(10,0)    ,
            col_decimal_38_10__undef_signed decimal(38,10)    ,
            col_decimal_38_10__undef_signed__index_inverted decimal(38,10)    ,
            INDEX col_int_undef_signed__index_inverted_idx (`col_int_undef_signed__index_inverted`) USING INVERTED,
            INDEX col_decimal_20_5__undef_signed__index_inverted_idx (`col_decimal_20_5__undef_signed__index_inverted`) USING INVERTED,
            INDEX col_varchar_100__undef_signed__index_inverted_idx (`col_varchar_100__undef_signed__index_inverted`) USING INVERTED,
            INDEX col_char_50__undef_signed__index_inverted_idx (`col_char_50__undef_signed__index_inverted`) USING INVERTED,
            INDEX col_date_undef_signed__index_inverted_idx (`col_date_undef_signed__index_inverted`) USING INVERTED,
            INDEX col_datetime_undef_signed__index_inverted_idx (`col_datetime_undef_signed__index_inverted`) USING INVERTED,
            INDEX col_tinyint_undef_signed__index_inverted_idx (`col_tinyint_undef_signed__index_inverted`) USING INVERTED,
            INDEX col_decimal_10_0__undef_signed__index_inverted_idx (`col_decimal_10_0__undef_signed__index_inverted`) USING INVERTED,
            INDEX col_decimal_38_10__undef_signed__index_inverted_idx (`col_decimal_38_10__undef_signed__index_inverted`) USING INVERTED
        ) engine=olap
        DUPLICATE KEY(pk, col_int_undef_signed, col_int_undef_signed__index_inverted)
        distributed by hash(pk) buckets 10
        properties("store_row_column" = "false", "replication_num" = "1");
    """

    sql """
        insert into ${tableName}(pk,col_int_undef_signed,col_int_undef_signed__index_inverted,col_decimal_20_5__undef_signed,col_decimal_20_5__undef_signed__index_inverted,col_varchar_100__undef_signed,col_varchar_100__undef_signed__index_inverted,col_char_50__undef_signed,col_char_50__undef_signed__index_inverted,col_string_undef_signed,col_date_undef_signed,col_date_undef_signed__index_inverted,col_datetime_undef_signed,col_datetime_undef_signed__index_inverted,col_tinyint_undef_signed,col_tinyint_undef_signed__index_inverted,col_decimal_10_0__undef_signed,col_decimal_10_0__undef_signed__index_inverted,col_decimal_38_10__undef_signed,col_decimal_38_10__undef_signed__index_inverted) values (49,-814629551,-320575127,92.1994,null,'and','2010-09-03 21:32:52','x','well','z','2009-08-21','2017-12-05','2005-08-24','2012-04-06 19:44:44',5,14,93.0179,94.1152,20.0309,94.0261),(48,26182,null,85.0923,13.1901,'🍕','all',null,'2007-11-02 14:22:31','y','2014-02-24','2006-05-10','2003-04-02','2005-06-12',13,6,49.1025,25.0165,36.0866,97.1200),(47,-1585723780,null,68.0513,59.1660,'could',null,'2011-01-12','m','b','2012-11-15','2003-10-03','2016-05-08','2002-05-23',null,5,61.0293,99.0048,30.0624,null),(46,77,-520266361,null,0.0322,null,'2015-01-22 14:31:27',',','“','r','2013-06-21','2019-07-24','2018-03-21','2017-05-09 13:37:34',10,8,90.0392,60.0064,63.0708,83.0129),(45,-19092,-10,75.1091,37.0712,'2017-12-16 09:51:22','his','é','2012-11-28 05:50:31','f','2003-01-27','2018-02-12','2018-03-12','2005-02-08',5,4,98.1199,14.1843,96.1744,91.0384),(44,21183,91.0638,49.1348,16.1083,'2008-07-05 09:22:06','2006-02-24','it''s','2000-01-14 09:38:35','y','2007-10-11','2010-09-19','2014-10-23','2001-06-23 07:32:12',14,15,null,11.0102,null,20.1074),(43,1486635733,null,23.0272,9.1786,'get','could',null,'2008-11-27 14:13:54','d','2005-09-27 01:08:35','2016-12-05','2017-11-23 15:48:28','2015-07-17 07:44:43',7,14,8.1533,41.0329,19.1376,27.0721),(42,null,75.1619,15.1165,30.0668,'him','about','2008-04-13','2008-01-12 23:59:57','x','2011-04-07','2016-06-26','2000-01-23','2019-08-19',13,14,80.1130,48.1896,60.1511,50.1581),(41,36,60.0881,82.1739,100.0391,null,'2017-12-08 06:14:04','2000-05-22','from','p','2011-12-17 01:20:56','2000-11-14','2019-04-04','2019-03-21',11,11,null,null,56.0672,54.1659),(40,80,5324,null,null,'\\n','his','©',null,'s','2001-06-26 08:14:03','2005-01-11','2013-05-20 12:26:11','2011-06-23',null,11,2.0134,3.1942,null,84.1767),(39,-44,18.0401,41.0164,69.0373,'2014-11-04 19:39:52','2019-05-17 02:09:47','2001-10-25','a','u','2015-04-07','2003-02-28','2018-09-08 10:56:03','2007-05-21 08:08:42',2,3,66.0449,63.0707,30.1838,53.1648),(38,19.0304,null,87.0749,61.0768,'something','s','2011-10-24','2010-03-19 10:30:14','s','2016-04-08','2010-04-04','2009-10-05 13:25:07','2013-01-02',2,5,8.0332,98.1324,null,36.1245),(37,2231,-140663446,82.1434,79.1935,null,'2004-10-01 05:53:25',null,null,null,'2002-02-23','2002-04-03','2019-05-24','2012-03-02',null,3,51.1051,15.1941,75.0719,81.1907),(36,16516,-112,13.1014,null,null,'+','2013-11-27 05:39:59','“','d','2004-02-19','2004-01-18 02:19:06','2001-09-27','2013-03-27 10:45:14',3,11,null,null,21.1940,10.1906),(35,-83,-11149,47.1433,null,'who','i',null,'back','x','2006-02-19','2015-10-28','2007-08-20','2012-04-26',13,10,null,62.0652,78.0910,36.0115),(34,null,null,null,5.1688,'i','time',null,'can',null,'2009-05-28','2018-01-13','2017-03-06','2016-12-23',10,11,64.0864,null,95.1136,35.1326),(33,-27712,109,20.1771,5.1539,'2019-09-06','yeah','2009-12-14 07:08:30','here','k','2004-08-01 08:24:03','2007-06-07','2000-11-16','2010-03-12',13,null,0.1376,null,null,81.1432),(32,null,-8223,27.1245,68.0055,'b','oh','how',null,'g','2019-01-02 19:04:44','2014-12-05','2016-11-17','2016-03-13',7,8,47.1272,20.1898,4.0035,33.1087),(31,null,null,35.1779,null,'2004-04-21','2013-09-04','2018-06-02','2004-09-24 01:32:23','p','2015-08-01','2017-11-08','2013-10-21','2009-02-13 22:10:15',5,12,null,78.0853,72.1830,34.1290),(30,1678810266,1507463100,62.0334,23.0812,'t','2003-07-06','you''re','it','r','2018-02-15','2004-09-28','2003-04-21 15:21:25','2019-03-25',5,12,9.0800,85.0061,32.0118,57.1966),(29,1026224919,-2052859566,null,50.1127,'2018-12-16','2006-12-05',null,'2015-05-28','e','2007-11-17','2016-09-02','2017-10-10','2006-05-16',7,11,null,5.0113,null,79.0511),(28,null,80.0048,84.0184,null,'2008-09-08 09:41:36','2018-10-08','2017-01-08','y','n','2007-12-15','2010-08-27 18:44:19','2005-04-25','2007-07-22',12,8,36.1799,53.0696,63.1981,25.1409),(27,-4501,-37,72.1681,7.0869,'2017-01-09',null,'as','2001-05-15 11:36:04','z','2002-07-20','2003-02-09','2002-04-02 15:01:52','2012-08-22',3,1,15.1878,85.0408,36.0699,71.0232),(26,5.0476,-4733,null,null,'look','”','time','2009-03-15 01:08:06',null,'2013-07-01','2014-04-02','2009-12-26','2003-09-01',7,1,null,88.1905,18.1736,21.0422),(25,108,85.0323,null,69.1796,'w','2012-11-03','v','2005-09-25','u','2019-09-21','2012-04-13','2017-09-23','2000-03-15',5,12,56.1423,59.1902,22.0961,9.0001),(24,174,null,15.0592,53.1774,'a','2014-01-02 09:56:05','look','when','d','2010-12-22','2004-01-27 06:26:50','2017-01-03','2014-07-26 17:04:22',1,12,34.1395,16.0221,null,31.1348),(23,47.0331,55.0423,92.1418,11.0656,null,'m','2019-06-21 09:51:53','2003-12-20 22:29:10',null,'2013-10-03','2000-07-14','2005-08-08 00:50:51','2001-09-22',11,5,23.1143,54.1735,67.0501,72.1124),(22,-30359,18454,62.0652,59.0615,'o','2006-06-18 19:24:41','2001-04-19','i','c','2011-09-21','2019-05-20','2007-05-01','2000-01-18',7,11,17.0427,39.1485,60.1966,27.0348),(21,83504484,102,67.0769,17.0948,'2016-03-08 09:20:32','look','u','2005-01-24 13:11:51','t','2017-11-10 21:06:45','2015-03-04','2013-02-17 02:49:09','2009-01-15',6,3,34.1455,88.0412,93.1511,90.1330),(20,null,-367544364,15.1601,69.0202,'2019-03-16','x','and','2012-11-25',null,'2003-12-16','2001-12-12','2019-05-08','2013-09-23',14,5,44.1047,null,null,null),(19,96.1228,-5149,10.1778,2.0482,'oh','é','a','f','z','2004-06-26','2014-10-23','2004-06-15','2015-12-06',1,12,91.0255,86.0787,null,41.1384),(18,null,-28299,62.1882,37.0786,'some','2006-12-18','2015-09-12',null,null,'2013-05-25','2006-01-07','2006-11-12 01:13:11','2001-12-27',12,7,90.0832,41.1857,82.0662,2.0787),(17,-94,-426161667,48.0144,18.0298,'k','2015-05-16 19:40:14','2013-11-12',null,'e','2009-12-08','2010-04-03','2005-01-17','2014-05-07 07:32:31',null,13,null,null,20.1666,null),(16,7876,34,19.0243,98.1134,'♪','up','2010-09-17 15:10:51','2011-05-14','h','2006-02-12','2009-01-13','2000-04-26 00:04:07','2010-10-12',6,9,83.1949,76.1111,null,6.0435),(15,87.1791,-39,65.0623,66.1088,'2014-10-27 07:04:00','¥','n',null,'e','2001-10-14','2003-05-15 05:26:54','2013-06-26','2016-11-12 10:50:57',1,9,10.1842,91.0549,null,60.1894),(14,-14681,46,35.1714,83.0753,'2008-03-25 17:28:53','you''re','k','2016-02-28','f','2012-04-21','2004-01-24','2010-05-20','2018-05-10 18:31:16',4,1,9.0975,25.1719,18.1370,null),(13,null,213501779,70.1039,28.0679,'DEL','about','mean','2013-08-24','u','2002-09-06','2001-07-20','2008-06-04 04:41:48','2013-09-09',8,14,3.1372,95.0694,68.1937,15.1266),(12,840810997,null,20.1511,31.1105,'2005-06-04 09:31:03',null,'2015-04-03 21:51:11','2015-06-25',null,'2018-05-24','2011-10-24','2007-04-23','2016-05-26 03:48:15',null,13,null,19.0977,55.1909,87.1879),(11,-1535053845,-82,null,null,'y','2008-09-04','u','l','m','2018-11-05 02:40:53','2016-07-15','2006-07-08','2014-03-16',5,1,13.1261,28.1630,null,92.0579),(10,1339657358,6.1863,67.0776,63.0165,'I''ll','or','b','just',null,'2010-06-11','2006-03-09','2018-09-01','2017-01-27',2,3,null,73.0994,18.1075,18.1771),(9,77.0965,-321144118,48.1147,32.1083,'k',null,null,'j','q','2011-05-22','2008-03-10 07:09:49','2017-03-27','2003-11-13',14,4,39.0487,66.1470,14.1892,27.0975),(8,null,2109123390,20.0954,60.0321,'then','b','tell','2004-04-05 13:37:57','m','2004-12-13','2007-03-25 05:17:35','2018-10-14','2018-07-02',6,14,32.0084,40.1017,null,31.1016),(7,10,null,69.0986,75.1214,null,'w','were','2000-01-25 06:33:02','a','2003-11-01','2008-07-11','2006-04-24 21:56:50','2013-11-25',1,13,46.1074,14.0294,3.1337,37.1948),(6,null,-102,null,47.1439,null,'2017-12-26','2001-08-13',null,'i','2015-12-14','2013-11-15','2009-09-06','2004-06-14 14:01:25',5,14,null,56.0859,44.1504,21.1430),(5,null,null,94.1004,89.1428,null,'was',null,'÷','f','2011-12-10','2008-01-13','2019-01-22 10:32:16','2014-11-11 22:37:16',5,6,84.0005,34.1868,61.0046,58.1874),(4,-2138043370,-86,null,47.0020,'€',null,'k','if',null,'2008-06-02','2017-08-24 22:58:57','2015-09-08','2012-01-13',1,6,83.1353,50.0385,47.1468,46.1740),(3,108,-18369,53.1664,68.1944,'back','2005-07-03 15:52:40',null,'2007-05-06','o','2014-04-11','2002-06-22 02:21:07','2004-12-27 10:53:04','2016-02-12',2,8,null,72.0088,49.1006,84.1622),(2,15,null,30.0087,42.0959,'“','2016-12-25','w','will',null,'2018-04-23','2011-02-17','2007-02-23 10:25:54','2008-06-14',null,13,53.0056,null,14.0125,98.0868),(1,1.1632,-100,41.1716,null,'⭐','now',null,null,'r','2013-08-21','2009-05-26 16:44:38','2005-10-11','2003-01-17 14:47:30',2,9,6.0786,13.0705,2.1381,32.0589),(0,-26516,42.0241,28.1735,66.0165,null,'on',null,'my',null,'2002-03-23 10:12:28','2015-05-15','2003-04-22','2007-04-10',13,8,null,25.1330,78.1141,51.1176);
    """

    qt_select_with_cte_exists """
        with cte1 as (select pk from ${tableName} where col_decimal_20_5__undef_signed != 8) 
        select * from ${tableName} o where exists(select 1 from cte1 au where au.pk = o.pk) order by pk;
    """
}
