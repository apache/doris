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

suite("rqg1333347798") {
    sql """
    DROP TABLE IF EXISTS `table_50_undef_partitions2_keys3_properties4_distributed_by53`;
    """
    sql """
create table table_50_undef_partitions2_keys3_properties4_distributed_by53 (
pk int,
col_bigint_undef_signed bigint   ,
col_bigint_undef_signed2 bigint   
) engine=olap
DUPLICATE KEY(pk)
distributed by hash(pk) buckets 10
properties("replication_num" = "1");
    """

    sql """insert into table_50_undef_partitions2_keys3_properties4_distributed_by53(pk,col_bigint_undef_signed,col_bigint_undef_signed2) values (0,null,18332),(1,788547,null),(2,4644959,-56),(3,8364628,72),(4,null,-5581),(5,2344024,-62),(6,-2689177,22979),(7,1320,-41),(8,null,-54),(9,12,-6236),(10,-8321648,null),(11,153691,null),(12,-8056,null),(13,-12,-2343514),(14,-35,-3361960),(15,62,null),(16,-551249,750),(17,-14,null),(18,36,109),(19,null,9365),(20,null,-2574125),(21,null,-739080),(22,-5772468,-74),(23,-1399,113),(24,-23711,8803),(25,118,47),(26,4528265,89),(27,34,null),(28,-7,-2255925),(29,13393,30843),(30,-5615,-693406),(31,749,null),(32,127,31588),(33,-2363619,null),(34,null,57),(35,26116,-3734512),(36,null,-3142945),(37,null,35),(38,48,null),(39,8367482,-26),(40,-90,-2794228),(41,null,5681382),(42,8268283,null),(43,null,-18860),(44,30861,14000),(45,-6207,25),(46,-1292030,-3411881),(47,null,57),(48,4000947,-94),(49,null,118);
    """
    sql """analyze table table_50_undef_partitions2_keys3_properties4_distributed_by53 with sync;
 """


        sql """
    DROP TABLE IF EXISTS `table_100_undef_partitions2_keys3_properties4_distributed_by52`;
    """
    sql """
create table table_100_undef_partitions2_keys3_properties4_distributed_by52 (
pk int,
col_bigint_undef_signed bigint   ,
col_bigint_undef_signed2 bigint   
) engine=olap
DUPLICATE KEY(pk, col_bigint_undef_signed)
distributed by hash(pk) buckets 10
properties("replication_num" = "1");
    """

    sql """insert into table_100_undef_partitions2_keys3_properties4_distributed_by52(pk,col_bigint_undef_signed,col_bigint_undef_signed2) values (0,null,69),(1,97,-61),(2,-6336,999069),(3,-32178,null),(4,18555,7),(5,null,-7825686),(6,92,11525),(7,106,null),(8,null,-22098),(9,-39,null),(10,31317,-17962),(11,null,-3402748),(12,1494928,1915512),(13,-25,-15251),(14,null,-5533979),(15,-6919683,71),(16,-30968,80),(17,58,null),(18,null,-1),(19,null,107),(20,null,21),(21,-764352,null),(22,14590,-2840),(23,5900022,null),(24,-12528,-61),(25,-4786407,28),(26,-4906255,-2433636),(27,-14,null),(28,-2704637,52),(29,1336262,12387),(30,-1536053,16722),(31,8198832,-123),(32,45,-2768949),(33,null,-922790),(34,null,29),(35,null,null),(36,3733,71),(37,26893,null),(38,866797,28),(39,-6984218,null),(40,92,null),(41,-12374,7716863),(42,-5039508,-1016),(43,null,null),(44,null,-8323),(45,13751,-6423208),(46,46,-111),(47,5750125,7058920),(48,50,-4254739),(49,null,3767),(50,27018,-2525385),(51,null,-38),(52,7661435,null),(53,126,31951),(54,-18230,null),(55,-5896492,9544),(56,107,23878),(57,2694217,4990),(58,-47,null),(59,6796246,null),(60,127,null),(61,28178,-78),(62,71,-17),(63,null,-10568),(64,30175,-3491),(65,7090289,-55),(66,-93,15453),(67,null,-114),(68,null,6149711),(69,31,10172),(70,97,-24510),(71,null,4011614),(72,8105474,9273),(73,-22636,46),(74,null,104),(75,null,3677142),(76,15057,25091),(77,21810,-121),(78,-126,387596),(79,-43,null),(80,null,-3434730),(81,48,null),(82,6336892,null),(83,10795,null),(84,28865,107),(85,null,-2475364),(86,-862371,-28),(87,null,null),(88,4720,17941),(89,2397,6182496),(90,-4691,651),(91,null,null),(92,null,-63),(93,111,2711),(94,19327,null),(95,19560,7062793),(96,-3186020,null),(97,20572,-48),(98,-2529181,22424),(99,null,-53);
    """
    sql """analyze table table_100_undef_partitions2_keys3_properties4_distributed_by52 with sync;
 """


    sql """
    DROP TABLE IF EXISTS `table_100_undef_partitions2_keys3_properties4_distributed_by5`;
    """
    sql """
create table table_100_undef_partitions2_keys3_properties4_distributed_by5 (
col_bigint_undef_signed bigint/*agg_type_placeholder*/   ,
col_bigint_undef_signed2 bigint/*agg_type_placeholder*/   ,
pk int/*agg_type_placeholder*/
) engine=olap
distributed by hash(pk) buckets 10
properties("replication_num" = "1");
    """

    sql """insert into table_100_undef_partitions2_keys3_properties4_distributed_by5(pk,col_bigint_undef_signed,col_bigint_undef_signed2) values (0,3429168,null),(1,-8095203,null),(2,null,null),(3,5227651,null),(4,-50,6026740),(5,-43,null),(6,-10,-77),(7,-13598,null),(8,13156,112),(9,-16585,18163),(10,-1184022,-5541355),(11,2386763,90),(12,-29492,-7934048),(13,-30940,-21),(14,22803,null),(15,27132,null),(16,17,-411),(17,6965,-29093),(18,32341,98),(19,-14991,116),(20,-7075162,null),(21,34,51),(22,null,21037),(23,null,1347),(24,18117,-73),(25,46,119),(26,-12,null),(27,9773,null),(28,66,-3097177),(29,-6430976,null),(30,null,11621),(31,null,110),(32,86,101),(33,-60,null),(34,4769996,-3),(35,28,-38),(36,null,-4350183),(37,null,-26947),(38,null,-5137310),(39,null,-20),(40,1844,-78),(41,32070,101),(42,2829368,-3338950),(43,null,6424566),(44,null,null),(45,-31302,-31934),(46,-9531,-107),(47,-2224664,5123063),(48,20520,-10540),(49,7,null),(50,32283,11838),(51,47,126),(52,null,null),(53,null,15248),(54,null,9721),(55,null,null),(56,2093726,23923),(57,28,-10114),(58,null,50),(59,null,-93),(60,null,-2571432),(61,-4934,24083),(62,-3506786,null),(63,-1560712,2491786),(64,null,-45),(65,25,5400504),(66,25054,15),(67,5,-22323),(68,15932,2099875),(69,7409124,null),(70,7137108,-1260937),(71,7116922,-4126902),(72,-16744,15950),(73,null,5082451),(74,null,-31583),(75,null,-128),(76,125,29159),(77,-479413,8178666),(78,2309057,-5791699),(79,-2271,-55),(80,40,-5447925),(81,-3239,2117918),(82,7887251,null),(83,-26,-3294041),(84,null,2466064),(85,30,null),(86,15543,5163523),(87,-4134908,7254),(88,null,31074),(89,null,10100),(90,50,-4062245),(91,null,82),(92,-13828,73),(93,1986044,54),(94,11317,-23336),(95,null,3837322),(96,null,null),(97,-28425,null),(98,14866,null),(99,null,-78);
    """
    sql """analyze table table_100_undef_partitions2_keys3_properties4_distributed_by5 with sync;
 """

    qt_test """
    SELECT  T2.col_bigint_undef_signed AS C1   FROM table_50_undef_partitions2_keys3_properties4_distributed_by53 AS T1  RIGHT OUTER JOIN  table_100_undef_partitions2_keys3_properties4_distributed_by52 AS T2 ON T1.col_bigint_undef_signed  <=>  T2.col_bigint_undef_signed2   AND  T1.col_bigint_undef_signed IN  (SELECT T3.col_bigint_undef_signed FROM table_100_undef_partitions2_keys3_properties4_distributed_by5 AS T3 WHERE T1.col_bigint_undef_signed  >=  T3.col_bigint_undef_signed2) ORDER BY C1  LIMIT 6, 3;
    """
}