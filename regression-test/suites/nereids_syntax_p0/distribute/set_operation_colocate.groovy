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

suite("set_operation_colocate") {
    multi_sql """
        drop table if exists table_20_undef_partitions2_keys3_properties4_distributed_by52;
        create table table_20_undef_partitions2_keys3_properties4_distributed_by52 (
           pk int,
           col_int_undef_signed int    ,
           col_smallint_undef_signed smallint    ,
           col_float_undef_signed float    ,
           col_datetimev2_undef_signed datetimev2    ,
           col_datev2_undef_signed datev2    ,
           col_decimalv3_20_10__undef_signed decimalv3(20,10)    ,
           col_varchar_20__undef_signed varchar(20)
        ) engine=olap
        DUPLICATE KEY(pk, col_int_undef_signed)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into table_20_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_smallint_undef_signed,col_float_undef_signed,col_datetimev2_undef_signed,col_datev2_undef_signed,col_decimalv3_20_10__undef_signed,col_varchar_20__undef_signed) values (0,30.1260,10555,32.0749,'2013-09-15 01:45:26','2002-01-19',66.0324,null),(1,-3231441,11504,68.1936,'2019-10-28 01:04:45','2009-04-16',11.0481,'y'),(2,10,31917,23.0015,'2006-05-06 18:15:33','2013-02-14',19.0817,'k'),(3,1476021,18644,84.1746,'2017-01-09 02:47:16','2000-06-21',75.1672,null),(4,5.0904,30294,13.1517,'2010-04-18 00:55:35','2007-04-21',33.1809,'there'),(5,null,3353,98.1781,'2016-01-19 16:47:01','2015-08-13',76.0000,'good'),(6,619657,-12125,11.0114,'2007-11-28 09:25:32','2014-08-28',97.0607,'w'),(7,null,23507,97.1316,'2016-09-09 05:23:54','2017-09-16',30.0162,'y'),(8,-72,-16184,27.1092,'2008-11-23 12:16:50','2019-11-13',84.1321,'was'),(9,-18,12136,84.0826,'2010-11-25 15:41:53','2014-10-15',12.1243,null),(10,16703,-29322,34.0313,'2003-09-12 05:29:11','2007-12-03',82.0828,null),(11,4.1416,4916,11.1961,'2000-05-26 18:37:53','2019-01-28',84.1033,null),(12,64.1032,-20707,15.1669,'2017-08-09 01:59:44','2016-10-22',71.0905,'i'),(13,null,1806,71.0824,'2007-04-02 18:31:51','2007-05-03',54.0616,'think'),(14,64,-31860,4.1957,'2003-12-05 13:42:09','2000-04-25',10.0313,'d'),(15,-47,-9075,1.1447,'2016-09-04 05:09:22','2009-09-13',58.0938,'just'),(16,-70,1249,30.0983,'2017-07-25 15:01:44','2013-12-03',14.0998,null),(17,31.1339,-21674,60.0593,'2016-04-14 16:24:34','2009-03-13',2.1360,null),(18,null,-13165,70.0237,'2018-03-13 22:28:31','2009-08-23',30.0458,'how'),(19,-4949206,-1988,52.1404,'2014-02-06 14:42:15','2006-11-18',45.1643,null);
        
        set session enable_runtime_filter_prune = false;
        SET SESSION enable_fold_constant_by_be=true;
        SET SESSION nth_optimized_plan = 1;
        set enable_sql_cache=false;
        set enable_query_cache=false;
        """

    String s = """SELECT *
        FROM (SELECT *
              FROM table_20_undef_partitions2_keys3_properties4_distributed_by52
              WHERE col_varchar_20__undef_signed > 'q'
              EXCEPT
              SELECT *
              FROM table_20_undef_partitions2_keys3_properties4_distributed_by52
              WHERE col_datetimev2_undef_signed > '2004-09-24 11:57:37') t1
                 RIGHT JOIN (SELECT *
                             FROM table_20_undef_partitions2_keys3_properties4_distributed_by52
                             WHERE col_smallint_undef_signed > 6
                             INTERSECT
                             SELECT *
                             FROM table_20_undef_partitions2_keys3_properties4_distributed_by52
                             WHERE col_smallint_undef_signed > 7
                             INTERSECT
                             SELECT *
                             FROM table_20_undef_partitions2_keys3_properties4_distributed_by52
                             WHERE col_varchar_20__undef_signed > 'j') t2
                            ON t1.col_datetimev2_undef_signed = t2.col_datetimev2_undef_signed
        
        """

    sleep(60 * 1000)

    for (int i = 1; i <= 65; ++i) {
        sql "set parallel_pipeline_task_num=" + i
        quickTest("test_" + i, s, true)
    }
}