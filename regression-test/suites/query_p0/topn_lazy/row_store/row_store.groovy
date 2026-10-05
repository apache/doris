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

suite("row_store") {
    sql """
    drop table if exists A;
    create table A (
    pk int,
    col_int_undef_signed int    ,
    col_int_undef_signed__index_inverted int    ,
    col_decimal_20_5__undef_signed decimal(20,5)    ,
    col_decimal_20_5__undef_signed__index_inverted decimal(20,5)    ,
    col_varchar_100__undef_signed varchar(100)    ,
    col_varchar_100__undef_signed__index_inverted varchar(100)    ,
    col_char_50__undef_signed char(50)    ,
    col_char_50__undef_signed__index_inverted char(50)    ,
    col_date_undef_signed date    ,
    col_date_undef_signed__index_inverted date    ,
    col_datetime_undef_signed datetime    ,
    col_datetime_undef_signed__index_inverted datetime    ,
    INDEX col_int_undef_signed__index_inverted_idx (`col_int_undef_signed__index_inverted`) USING INVERTED,
    INDEX col_decimal_20_5__undef_signed__index_inverted_idx (`col_decimal_20_5__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_varchar_100__undef_signed__index_inverted_idx (`col_varchar_100__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_char_50__undef_signed__index_inverted_idx (`col_char_50__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_date_undef_signed__index_inverted_idx (`col_date_undef_signed__index_inverted`) USING INVERTED,
    INDEX col_datetime_undef_signed__index_inverted_idx (`col_datetime_undef_signed__index_inverted`) USING INVERTED
    ) engine=olap
    DUPLICATE KEY(pk)
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1", "store_row_column" = "true");
    """
    sql """
    insert into A(pk,col_int_undef_signed,col_int_undef_signed__index_inverted,col_decimal_20_5__undef_signed,col_decimal_20_5__undef_signed__index_inverted,col_varchar_100__undef_signed,col_varchar_100__undef_signed__index_inverted,col_char_50__undef_signed,col_char_50__undef_signed__index_inverted,col_date_undef_signed,col_date_undef_signed__index_inverted,col_datetime_undef_signed,col_datetime_undef_signed__index_inverted) values (19,-19340,434458342,25287,82.1284,' ','oh','didn''t','2018-11-23','2007-09-07','2003-03-10 09:22:44','2011-01-16','2009-04-24'),(18,-33,null,-3,32133,'j','l','now','p','2005-05-18','2009-10-13','2012-04-05','2000-04-12'),(17,-24531,19.1824,-1007073555,-23161,'2010-10-13 08:28:50','that','i','at','2007-05-06','2012-11-22 20:01:29','2009-06-20','2006-10-22'),(16,783088656,null,null,null,'2019-09-16','2003-12-13','2007-08-27 00:09:18','≠','2006-07-24','2008-11-06','2006-06-11','2003-10-27'),(15,65.0177,708339588,-32040,-572119702,'2019-06-18 13:03:39','as','2001-12-14 00:00:08','2014-11-03','2011-12-17','2006-11-26 02:28:30','2012-02-20 14:14:10','2010-03-21 04:57:36'),(14,-12259,14250,77,null,'c','v',null,null,'2010-08-17','2010-08-02','2010-09-27','2016-05-22'),(13,30.0548,-2699,25.1965,-22153,'well','have',null,'she','2016-08-25','2009-06-02','2015-09-09 20:24:45','2019-02-17'),(12,-6720,null,40,23.1792,'2015-10-08 09:20:28',null,null,'2002-03-03','2018-04-07','2016-04-22','2002-11-21','2012-10-11'),(11,-29325,null,-326078691,49.1424,null,'¥','z','2005-12-07 03:13:53','2009-07-16','2018-02-22','2000-07-07','2015-02-28 22:32:57'),(10,null,9151,61.0288,-1592544956,'and','2012-01-15 21:47:20',null,'with','2005-09-02 00:08:00','2002-02-23','2012-09-12 12:48:44','2001-11-07'),(9,93,1210133742,116,89,'2018-08-16 22:14:28','2017-06-02','¥','for','2001-02-27','2015-08-04','2005-03-04','2003-05-12'),(8,-34,-1559575710,null,-1270839701,'c','a','2017-12-14 07:57:22','i','2017-08-04 14:25:20','2015-07-12','2002-05-04','2014-05-14'),(7,null,null,-2084638070,29791,'could','2017-11-27 21:51:39','come','how','2019-12-04','2009-11-07','2007-08-22','2000-06-25 20:57:36'),(6,60,101132558,null,-498775585,'2002-02-28',null,null,'2004-08-05','2005-07-17','2004-01-19','2003-02-15','2015-11-02'),(5,18.0955,null,29073,-29982,'here','o','i','but','2009-06-06','2016-02-14','2016-03-23','2015-11-19 05:12:12'),(4,1762161475,-66,-4696,10.0915,'n','2003-04-03 15:27:53','2008-11-19','2007-02-07','2008-04-28','2012-03-09','2017-06-20','2003-07-03 00:58:17'),(3,-469009905,-117,-1742334207,null,'2006-04-13','were','2009-01-05','2012-02-02','2008-05-22','2007-06-01 19:57:48','2004-08-27','2005-10-17 01:33:15'),(2,25.0759,null,-762,-1494902124,'right','2003-09-14','if','but','2008-06-27','2016-09-18','2004-04-10','2001-04-28 15:31:20'),(1,28968,-52,null,66.0576,'there',null,'k','a','2017-04-26','2010-10-13','2000-01-08','2016-05-19'),(0,null,60.0780,2703,22341,'we','a','m','2019-04-03 14:15:19','2011-02-11','2003-08-17','2006-10-14','2013-05-04');
    """

    sql """
    drop table if exists B;
    create table B (
    pk int,
    col_int_undef_signed int    ,
    col_int_undef_signed__index_inverted int    ,
    col_decimal_20_5__undef_signed decimal(20,5)    ,
    col_decimal_20_5__undef_signed__index_inverted decimal(20,5)    ,
    col_varchar_100__undef_signed varchar(100)    ,
    col_varchar_100__undef_signed__index_inverted varchar(100)    ,
    col_char_50__undef_signed char(50)    ,
    col_char_50__undef_signed__index_inverted char(50)    ,
    col_date_undef_signed date    ,
    col_date_undef_signed__index_inverted date    ,
    col_datetime_undef_signed datetime    ,
    col_datetime_undef_signed__index_inverted datetime    ,
    INDEX col_int_undef_signed__index_inverted_idx (`col_int_undef_signed__index_inverted`) USING INVERTED,
    INDEX col_decimal_20_5__undef_signed__index_inverted_idx (`col_decimal_20_5__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_varchar_100__undef_signed__index_inverted_idx (`col_varchar_100__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_char_50__undef_signed__index_inverted_idx (`col_char_50__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_date_undef_signed__index_inverted_idx (`col_date_undef_signed__index_inverted`) USING INVERTED,
    INDEX col_datetime_undef_signed__index_inverted_idx (`col_datetime_undef_signed__index_inverted`) USING INVERTED
    ) engine=olap
    DUPLICATE KEY(pk)
    distributed by hash(pk) buckets 10
    properties("store_row_column" = "false", "replication_num" = "1");
    insert into B(pk,col_int_undef_signed,col_int_undef_signed__index_inverted,col_decimal_20_5__undef_signed,col_decimal_20_5__undef_signed__index_inverted,col_varchar_100__undef_signed,col_varchar_100__undef_signed__index_inverted,col_char_50__undef_signed,col_char_50__undef_signed__index_inverted,col_date_undef_signed,col_date_undef_signed__index_inverted,col_datetime_undef_signed,col_datetime_undef_signed__index_inverted) values (49,-4,-91,34.0802,null,'this','2009-05-01 07:57:04','2019-10-18 09:39:59',null,'2002-07-26','2005-01-13','2009-05-25','2002-09-23 19:25:56'),(48,null,52.0444,-8777,-13,'2011-02-25 11:05:52','x','2006-05-02 22:50:57','k','2008-01-16','2008-10-07','2010-08-25','2017-08-14 15:19:16'),(47,null,27269,87.1635,-16730,'y','that','up',null,'2001-11-24','2002-09-08','2017-08-10 11:17:37','2006-11-22'),(46,null,2033562533,3.1717,-86,'ok',null,'t','i','2013-03-16','2017-08-23','2000-06-21','2012-03-05 18:33:04'),(45,-871016251,null,null,9508,'2008-01-18 00:28:04','n','f','⭐','2019-08-17','2003-03-23','2017-05-14','2007-07-26'),(44,-15716,26793,13099,1358208306,'I''m','what','+',null,'2005-11-27','2010-10-02','2001-01-01','2005-03-10'),(43,933200434,null,81,18.1519,'my','why','r','y','2017-11-03','2014-07-16','2003-05-20','2019-08-14'),(42,15414,34.1659,77176296,null,'-','can','™','don''t','2005-05-04','2009-09-02','2018-11-14','2017-02-13'),(41,2554,69.0324,696878699,-1119048018,null,'2001-07-18 13:01:43',null,':','2008-02-20','2010-03-18 19:23:55','2019-12-24','2001-08-06'),(40,-1998173102,-31,63.0521,-1103575828,'2014-07-03 21:18:07','♪','2017-03-06 23:55:31','2004-05-06 08:51:43','2006-02-25','2003-09-21','2009-05-10 00:30:07','2003-02-20'),(39,null,-120,-18,17,'g','DEL','2018-12-02','of','2017-11-20','2017-12-24','2011-05-27','2008-04-25'),(38,8960,-1539795988,118,5274,'2011-04-01 02:58:07','2012-11-18 10:57:41','d',null,'2014-07-23','2001-03-20','2012-03-14','2006-09-09'),(37,-122,-41,-10,null,'2014-07-02 21:58:47','c','oh','2000-08-03 20:58:41','2013-12-20','2007-08-26','2006-10-08','2017-06-13'),(36,76,null,46.1871,26.1015,'2014-12-23','2004-09-15 22:06:15','2000-08-08','b','2018-01-16','2014-11-22 20:16:03','2015-07-09','2005-06-19'),(35,-16709,1304,189895257,8577,null,'2009-01-26 12:20:37','look',null,'2019-08-18','2012-02-02','2003-05-12 08:59:21','2013-01-20'),(34,13,28153,null,22401,'i','me','2001-04-10 14:28:35','2003-01-20','2019-09-14','2004-03-28','2009-12-09','2009-08-24 05:48:14'),(33,null,37,933453759,447773053,'2000-09-08','c',null,'2002-08-14 20:23:22','2011-11-27','2015-11-04','2004-07-25','2009-12-17 15:38:01'),(32,-2103704472,17317,null,null,'hey','2008-12-18 19:02:58','y','h','2003-07-18','2016-10-11','2016-11-16 15:11:49','2007-05-21'),(31,null,null,-71,68,'who',null,'really','2019-11-09','2016-02-17','2004-01-10 19:41:55','2000-05-10','2001-12-27'),(30,1696191545,1028363102,-57,null,'he''s',null,'2011-09-13 15:38:54',null,'2017-10-11','2012-11-28 09:25:36','2008-12-10 11:41:41','2013-12-06'),(29,21,-1964197111,-2070059748,null,'y','2004-03-14 22:26:33','why','2016-10-14','2016-02-09','2004-03-06','2006-05-11 09:39:08','2009-05-21'),(28,86.1119,81.1540,-1025346736,-9,'in',null,'2016-07-18 07:37:52','DEL','2018-03-18','2012-02-03 06:38:50','2007-04-26','2011-07-12'),(27,null,-121,-15036,-24793,'2005-07-20 07:52:37','2004-04-10','2017-09-01','f','2001-10-19','2001-11-18','2016-07-16','2004-04-12 13:40:51'),(26,-931,-1649524270,58.1375,106,'do',null,'ok','from','2014-12-11','2000-06-04','2015-10-10','2000-11-21'),(25,120,34.0837,null,21.1020,null,'come','2004-12-02 16:19:32','q','2000-02-16 21:53:26','2000-02-24','2006-02-10','2007-04-03 13:50:25'),(24,59.0018,-2038257821,82.0129,111,'2001-08-01 10:27:27',null,'2015-10-07 04:40:19','2019-01-19','2018-09-06','2010-10-27','2018-03-01 23:54:46','2018-06-25'),(23,-2260,41.1206,41.0889,919870987,'something','be','2009-07-19 08:54:46',null,'2003-09-01','2015-04-26','2001-08-21','2016-01-02'),(22,null,2.0893,27599,null,'know',null,null,'got','2002-06-08','2012-10-21','2006-01-02','2018-09-27 07:37:59'),(21,22897,560301985,20.0735,-16431,'∞','2017-04-17','g','™','2001-01-13','2004-02-11','2003-01-06','2011-03-16'),(20,67.0190,null,-68,448376673,'2012-07-15 13:15:05',null,null,'2018-12-16','2008-09-02','2010-12-27','2002-01-16','2004-10-13'),(19,null,31.1559,-20394,-616964999,null,'2006-08-12 10:38:19','h','2013-06-03','2003-01-26','2010-06-11','2005-09-11','2017-06-13'),(18,-16109,-531429086,2078394455,43.0824,'b','his','2005-02-19',null,'2019-02-17','2005-06-03','2016-03-20','2018-08-27'),(17,null,15780,4.0743,-46,'2019-05-13','2008-03-14 17:55:43','now','n','2010-03-03','2004-12-23','2007-01-01 09:22:21','2010-05-09'),(16,null,23324,-817683742,54,'2007-07-05 16:22:32','or','2009-05-16','2004-01-10 21:43:58','2018-07-09 19:14:14','2016-12-05','2005-09-05','2018-12-01 01:41:29'),(15,452231883,-1163046110,null,222549700,null,'could','2016-03-11 05:20:30','2010-05-06','2004-06-06','2004-05-04 22:49:18','2004-04-02','2014-06-11'),(14,1744751135,26063,9392,null,null,'2014-01-20','why','this','2016-04-22','2014-12-26','2003-08-14','2019-08-25'),(13,null,74,3.0934,-124,'b','≠','can''t','☀️','2010-06-04','2011-10-21 16:56:16','2012-07-14 16:07:24','2010-02-10'),(12,-2109763182,-2,null,-18643,'when','2003-10-28','back','your','2017-10-09','2003-07-09','2012-01-01','2013-08-20 03:44:24'),(11,15003,44,1363216025,-1416368492,'there','when',null,'?','2004-03-09','2013-10-17','2002-02-22 15:40:12','2014-11-18'),(10,-90,-3,42.1123,12.1795,null,'2002-10-28','2008-08-09 09:46:34','with','2012-09-11','2007-12-15','2008-12-15','2013-08-10'),(9,null,-29455,-2077735059,-17001,'2000-07-17 10:32:05','2005-04-23 09:06:33','2019-10-03','tell','2007-10-05','2015-04-16','2017-06-07 15:45:27','2019-12-19 08:24:18'),(8,19.1187,-669934709,-26512,77.1716,'p','r','2009-01-08','2011-08-24 09:25:52','2002-10-07','2013-04-23','2010-06-05','2015-01-07 12:28:13'),(7,82,12949,-26388,-19,'2016-06-27',null,'2017-07-10 05:52:05',null,'2003-02-02','2008-09-12 12:53:58','2008-08-08','2003-03-22'),(6,63.0015,24,25525,null,'how','2017-05-05 07:12:07','2003-03-28 21:51:43','r','2011-10-04','2014-02-15','2012-01-02 10:03:40','2003-06-25'),(5,1372452467,null,71.1966,655627297,null,'£','i','think','2008-10-26','2018-05-15','2010-03-10','2002-10-01'),(4,43,null,-56,-773225606,'or','as',null,'that','2009-05-17','2004-09-01','2019-09-21','2004-01-06'),(3,71.1359,80.1212,1336762501,330140767,'right','÷','n','2001-02-12 07:46:02','2007-12-20','2010-08-21','2006-03-12','2000-09-18'),(2,null,-114,null,null,'because',null,'2002-03-09 17:30:38','2013-10-27 10:49:41','2002-05-09','2011-11-25 12:26:23','2010-04-01','2016-07-26'),(1,-32280,null,null,15.1855,'2013-10-27','think','a',null,'2003-04-21','2017-05-06 20:53:33','2013-11-26','2011-01-07 20:19:48'),(0,null,22,null,-28983,'2014-09-18 08:24:54','k','2018-06-27 00:39:29','o','2018-09-17','2015-08-18','2002-07-24','2001-07-22');
    """


    sql """
    drop table if exists C;
    create table C (
    pk int,
    col_int_undef_signed int    ,
    col_int_undef_signed__index_inverted int    ,
    col_decimal_20_5__undef_signed decimal(20,5)    ,
    col_decimal_20_5__undef_signed__index_inverted decimal(20,5)    ,
    col_varchar_100__undef_signed varchar(100)    ,
    col_varchar_100__undef_signed__index_inverted varchar(100)    ,
    col_char_50__undef_signed char(50)    ,
    col_char_50__undef_signed__index_inverted char(50)    ,
    col_date_undef_signed date    ,
    col_date_undef_signed__index_inverted date    ,
    col_datetime_undef_signed datetime    ,
    col_datetime_undef_signed__index_inverted datetime    ,
    INDEX col_int_undef_signed__index_inverted_idx (`col_int_undef_signed__index_inverted`) USING INVERTED,
    INDEX col_decimal_20_5__undef_signed__index_inverted_idx (`col_decimal_20_5__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_varchar_100__undef_signed__index_inverted_idx (`col_varchar_100__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_char_50__undef_signed__index_inverted_idx (`col_char_50__undef_signed__index_inverted`) USING INVERTED,
    INDEX col_date_undef_signed__index_inverted_idx (`col_date_undef_signed__index_inverted`) USING INVERTED,
    INDEX col_datetime_undef_signed__index_inverted_idx (`col_datetime_undef_signed__index_inverted`) USING INVERTED
    ) engine=olap
    DUPLICATE KEY(pk, col_int_undef_signed, col_int_undef_signed__index_inverted)
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into C(pk,col_int_undef_signed,col_int_undef_signed__index_inverted,col_decimal_20_5__undef_signed,col_decimal_20_5__undef_signed__index_inverted,col_varchar_100__undef_signed,col_varchar_100__undef_signed__index_inverted,col_char_50__undef_signed,col_char_50__undef_signed__index_inverted,col_date_undef_signed,col_date_undef_signed__index_inverted,col_datetime_undef_signed,col_datetime_undef_signed__index_inverted) values (19,null,-540615579,21099,2.1903,null,'had','see',null,'2000-06-05 12:12:33','2001-11-13','2010-02-21 02:08:07','2014-08-02'),(18,27859,72,72.0429,-126,'here',null,'2018-11-13 19:03:20','you''re','2019-01-21 21:32:39','2018-10-23','2006-04-02','2009-02-23'),(17,10.0666,113,null,87.0957,'.','2002-09-14 18:31:10','2003-11-15 06:30:27','like','2003-07-04 22:00:55','2006-05-19','2014-01-20','2001-12-14'),(16,null,71.0301,11,62.1881,'at','2016-01-28','a','2002-03-15 00:00:17','2008-08-04','2010-04-19 05:24:57','2018-09-12','2007-04-12'),(15,277901530,529102855,96693896,-953140883,'¥','some',null,'could','2009-01-20','2004-08-11','2003-09-23','2015-02-01'),(14,85.1993,61,null,60.0479,null,'d',null,null,'2005-12-22','2018-08-28','2004-05-20 02:34:26','2018-11-04'),(13,null,6.0043,473269699,97,'s','2017-12-18 05:15:48','2019-11-23',null,'2017-04-27','2001-07-07 01:34:19','2014-06-21','2007-07-13 20:19:46'),(12,89.1517,null,null,1557678895,null,'\t','2009-11-18','2013-08-27 08:42:13','2010-05-10','2012-05-22','2000-12-26 16:35:27','2017-03-23'),(11,null,-178082324,-1635,-6477,null,'™',null,'ok','2015-07-15 20:54:43','2000-04-25','2003-12-11','2015-07-11'),(10,-11545,null,35,23584,'2004-09-16 00:14:40','did','2015-04-05','y','2017-01-17','2016-02-23','2010-07-13','2007-02-26'),(9,63.1150,-116,20825,-29,'b',':','2000-09-19 19:51:41','no','2013-07-12','2016-11-17','2002-12-16','2002-06-09'),(8,-9217,97.1995,25.0433,74.0286,'had','got','p','l','2018-07-20','2017-09-08','2008-10-19','2004-11-18'),(7,1216474025,11907,31.1819,-489418557,'had','ب','2017-07-09',null,'2004-12-24','2008-02-12','2013-01-14','2010-06-20'),(6,27553523,22150,24976,-31082,'2002-12-12',null,'hey','2002-03-17','2011-07-10 08:28:51','2009-11-03','2019-12-21','2019-11-09 15:30:18'),(5,98.1698,-23,81433887,48.0263,'2007-02-24 13:32:15','2000-11-28','back','d','2005-01-12','2013-04-01','2015-07-21','2008-12-22'),(4,-1520221975,null,80,390075296,'he''s','r','2002-04-11','2018-08-10 07:19:20','2018-10-27','2004-06-22','2006-10-13','2019-06-25 15:46:25'),(3,35.1378,58.1581,-85,58,'j','2001-09-14 11:15:26','e','have','2008-06-08','2007-03-23','2010-02-06','2018-04-16 17:45:05'),(2,31.0357,38.0991,null,77.0809,'2019-07-25 02:35:53','”','with','2012-03-15 21:13:06','2009-09-03','2006-08-26','2003-05-05','2001-07-08 04:31:09'),(1,5.1074,118,1804658583,null,'p',null,'because',null,'2002-09-16','2019-03-14','2019-09-02','2001-03-15'),(0,1389653584,5.1490,null,66,'2018-07-19','o','2012-11-25','not','2005-03-19 09:06:56','2005-09-06','2000-07-01','2006-06-19');

    """

// make sure row store flag order is correct
    qt_row_store """
    SELECT
    *
    FROM
        A AS t1
        RIGHT JOIN B AS t2 ON t1.pk = t2.pk
        LEFT JOIN C AS t3 ON t2.pk = t3.pk
    ORDER BY
        t3.pk,
        t2.pk ASC,
        t1.pk ASC
    LIMIT
        6;
    """

}