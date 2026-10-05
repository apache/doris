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


        sql """
    DROP TABLE IF EXISTS `table_100_undef_partitions2_keys3_properties4_distributed_by53`;
    """
    sql """
create table table_100_undef_partitions2_keys3_properties4_distributed_by53 (
pk int,
col_bigint_undef_signed bigint   ,
col_bigint_undef_signed2 bigint   
) engine=olap
DUPLICATE KEY(pk)
distributed by hash(pk) buckets 10
properties("replication_num" = "1");
    """

    sql """insert into table_100_undef_partitions2_keys3_properties4_distributed_by53(pk,col_bigint_undef_signed,col_bigint_undef_signed2) values (0,55,-58),(1,49,29792),(2,95,3551878),(3,7833628,-6983097),(4,-27400,null),(5,1070487,null),(6,null,-889026),(7,null,7),(8,8064787,-21951),(9,13186,24466),(10,null,-8),(11,null,null),(12,-18,6017454),(13,null,-18),(14,21681,14079),(15,31241,-17653),(16,5825,13559),(17,null,-10508),(18,null,20682),(19,2013160,-98),(20,12,4882),(21,null,-8190232),(22,-5333,1),(23,-26,-18696),(24,null,2990000),(25,null,-24258),(26,null,-5319680),(27,null,null),(28,-61,7098),(29,-71,-4935965),(30,-3593860,null),(31,3992650,-34),(32,-6153942,null),(33,56,null),(34,-82,-30),(35,-23019,3216053),(36,-115,-74),(37,-18674,null),(38,-11887,802224),(39,-7917399,-118),(40,-7849575,6558000),(41,-6419660,119),(42,5375756,-30237),(43,56,null),(44,null,-430671),(45,null,-14492),(46,-27,-1297890),(47,-197101,19459),(48,-205683,-18077),(49,3029249,-37),(50,-8063861,null),(51,-21743,-478274),(52,3,-126),(53,null,3897),(54,111,-8744),(55,-6340842,15459),(56,32253,333948),(57,7990982,null),(58,65,null),(59,-105,-6313947),(60,-5285102,4449175),(61,-555133,-25478),(62,-26320,96),(63,null,null),(64,19870,-1083235),(65,-2774,-7883),(66,4082401,24),(67,-12624,31),(68,108,8096804),(69,-4,-31818),(70,null,-19080),(71,null,-2485343),(72,null,43),(73,-2341108,24424),(74,31,-27428),(75,5665087,-718),(76,null,20822),(77,6169809,-1824549),(78,-15934,null),(79,78,-2683753),(80,8572,-3138622),(81,1733118,4077),(82,null,114),(83,10,-71),(84,-32489,5303589),(85,null,null),(86,-22984,32361),(87,26607,-4439629),(88,4023283,-262823),(89,-2434924,-11360),(90,24063,-28747),(91,7589,null),(92,-28,-5257917),(93,-74,-93),(94,121,null),(95,-107,3057),(96,-22993,null),(97,-34,-36),(98,null,70),(99,-3540406,6681370);"""

    qt_test """
   SELECT  T2.col_bigint_undef_signed2 AS C1   FROM table_100_undef_partitions2_keys3_properties4_distributed_by53 AS T1  LEFT OUTER JOIN  table_100_undef_partitions2_keys3_properties4_distributed_by53 AS T2 ON T1.col_bigint_undef_signed  <=>  T2.col_bigint_undef_signed   AND  T1.col_bigint_undef_signed NOT IN  (SELECT T3.col_bigint_undef_signed FROM table_100_undef_partitions2_keys3_properties4_distributed_by5 AS T3 WHERE T1.col_bigint_undef_signed2  <  T3.col_bigint_undef_signed2)  OR  T1.col_bigint_undef_signed2  <>  2 order by 1;
    """
}