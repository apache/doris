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

import groovy.io.FileType
import java.nio.file.Files
import java.nio.file.Paths

suite("test_conditional_function") {
    sql "set batch_size = 4096;"

    def tbName = "test_conditional_function"
    sql "DROP TABLE IF EXISTS ${tbName};"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                user_id INT
            )
            DISTRIBUTED BY HASH(user_id) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """
        INSERT INTO ${tbName} VALUES 
            (1),
            (2),
            (3),
            (4);
        """
    sql """
        INSERT INTO ${tbName} VALUES 
            (null),
            (null),
            (null),
            (null);
        """

    qt_sql "select user_id, case user_id when 1 then 'user_id = 1' when 2 then 'user_id = 2' else 'user_id not exist' end test_case from ${tbName} order by user_id;"
    qt_sql "select user_id, case when user_id = 1 then 'user_id = 1' when user_id = 2 then 'user_id = 2' else 'user_id not exist' end test_case from ${tbName} order by user_id;"

    qt_sql "select user_id, if(user_id = 1, \"true\", \"false\") test_if from ${tbName} order by user_id;"

    qt_sql "select coalesce(NULL, '1111', '0000');"

    qt_sql "select ifnull(1,0);"
    qt_sql "select ifnull(null,10);"
    qt_sql "select ifnull(1,user_id) from ${tbName} order by user_id;"
    qt_sql "select ifnull(user_id,1) from ${tbName} order by user_id;"
    qt_sql "select ifnull(null,user_id) from ${tbName} order by user_id;"
    qt_sql "select ifnull(user_id,null) from ${tbName} order by user_id;"

    qt_sql "select nullif(1,1);"
    qt_sql "select nullif(1,0);"
    qt_sql "select nullif(1,user_id) from ${tbName} order by user_id;"
    qt_sql "select nullif(user_id,1) from ${tbName} order by user_id;"
    qt_sql "select nullif(null,user_id) from ${tbName} order by user_id;"
    qt_sql "select nullif(user_id,null) from ${tbName} order by user_id;"


    qt_sql "select nullif(1,1);"
    qt_sql "select nullif(1,0);"


    qt_sql "select is_null_pred(user_id) from ${tbName} order by user_id"
    qt_sql "select is_not_null_pred(user_id) from ${tbName} order by user_id"

    qt_sql """select if(date_format(CONCAT_WS('', '9999-07', '-26'), '%Y-%m')= DATE_FORMAT( curdate(), '%Y-%m'),
	        curdate(),
	        DATE_FORMAT(DATE_SUB(month_ceil ( CONCAT_WS('', '9999-07', '-26')), 1), '%Y-%m-%d'));"""

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),3);"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-01'), '%Y-%m'),3);"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'));"

    qt_sql "select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),date_format(CONCAT_WS('', '9999-07', '-26'), '%Y-%m'));"

    qt_sql "select ifnull( user_id, '9999') r from ${tbName} order by r"

    qt_sql "select ifnull( user_id, 999) r from ${tbName} order by r"

    qt_if_true_then_nullable """select IF(true, DAYOFWEEK("2022-12-06 17:48:46"), 1) + 1;"""
    qt_if_true_else_nullable """select IF(true, 1, DAYOFWEEK("2022-12-06 17:48:46")) + 1;"""

    qt_if_false_then_nullable """select IF(false, DAYOFWEEK("2022-12-06 17:48:46"), 1) + 1;"""
    qt_if_false_else_nullable """select IF(false, 1, DAYOFWEEK("2022-12-06 17:48:46")) + 1;"""

    sql "DROP TABLE ${tbName};"

    sql "set enable_decimal256=true"
    sql """ drop table if exists t1; """
     sql """ drop table if exists t2; """
    sql """ create table t1(
        k1 int,
        k2 date,
        k22 date,
        k3 datetime,
        k33 datetime,
        k4 decimalv3(76, 6),
        k44 decimalv3(76, 6)
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """
     sql """ create table t2(
        k1 int,
        k2 array<decimalv3(76, 6)>
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """
    sql """
    insert into t1 values
    (1, null, '2023-01-02', '2023-01-01 00:00:00', '2023-01-02 00:00:00','2222222222222222222222222222222222222222222222222222222222222222222222','3333333333333333333333333333333333333333333333333333333333333333333333'),(2, '2023-02-01', null, '2023-02-01 00:00:00', '2023-02-02 00:00:00', null,'5555555555555555555555555555555555555555555555555555555555555555555555'),(3, '2023-03-01', '2023-03-02', null, '2023-03-02 00:00:00', null,'666666666666666666666666666666666666666666666666')
    """
    sql """
    insert into t2 select k1, array_agg(k4) from t1 group by k1;
    """
    qt_test "select k1, coalesce(k2, k22),coalesce(k3, k33) from t1 order by k1"
    qt_test "select k1, coalesce(k2, k22),coalesce(k4, k44) from t1 order by k1"
    qt_test "select k1, case when k1 = 1 then k4 when k1 = 2 then k44 when k1 = 3 then k4 when k1 = 4 then k44 else k4 end from t1 order by k1"

    qt_test "SELECT k1, width_bucket(k2, 1, 9999999999, 4),width_bucket(k3, 1, 9999999999, 4) AS w FROM t1 ORDER BY k1;"

    qt_test "SELECT k1, width_bucket(k4, 1, 9999999999, 4),width_bucket(k44, 1, 9999999999, 4) AS w FROM t1 ORDER BY k1;"

    qt_test "SELECT k1, width_bucket(k44, 1, 9999999999, 4),width_bucket(k44, 1, 9999999999, 4) AS w FROM t1 ORDER BY k1;"

    qt_test "select array_join(array_agg(k44), '_', 'null') from t1 where k1=1;"

    qt_test "select k1,avg_weighted(k4,1),avg_weighted(k44,1) from t1 group by k1 order by k1;"

    qt_test "select array_cum_sum(k2) from t2 where k1=1;"

    sql "set enable_decimal256=false"
    qt_test "select array_cum_sum(k2) from t2 where k1=1;"

    sql "drop table if exists table_200_undef_partitions2_keys3_properties4_distributed_by5;"
    sql """
create table table_200_undef_partitions2_keys3_properties4_distributed_by5 (
col_date_undef_signed_not_null date  not null ,
col_bigint_undef_signed_not_null bigint  not null ,
col_int_undef_signed int  null ,
col_int_undef_signed_not_null int  not null ,
col_bigint_undef_signed bigint  null ,
col_date_undef_signed date  null ,
col_varchar_10__undef_signed varchar(10)  null ,
col_varchar_10__undef_signed_not_null varchar(10)  not null ,
col_varchar_1024__undef_signed varchar(1024)  null ,
col_varchar_1024__undef_signed_not_null varchar(1024)  not null ,
pk int
) engine=olap
UNIQUE KEY(col_date_undef_signed_not_null, col_bigint_undef_signed_not_null)
PARTITION BY             RANGE(col_date_undef_signed_not_null) (
                FROM ('2023-12-09') TO ('2024-03-09') INTERVAL 1 DAY,
                FROM ('2025-02-16') TO ('2025-03-09') INTERVAL 1 DAY,
                FROM ('2025-06-18') TO ('2025-06-20') INTERVAL 1 DAY,
                FROM ('2026-01-01') TO ('2026-03-09') INTERVAL 1 DAY,
                FROM ('2027-01-01') TO ('2027-02-09') INTERVAL 1 DAY
            )
        
distributed by hash(col_bigint_undef_signed_not_null) buckets 30
properties("enable_unique_key_merge_on_write" = "true", "replication_num" = "1");
    """
    sql """
insert into table_200_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_int_undef_signed_not_null,col_bigint_undef_signed,col_bigint_undef_signed_not_null,col_date_undef_signed,col_date_undef_signed_not_null,col_varchar_10__undef_signed,col_varchar_10__undef_signed_not_null,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_not_null) values (0,-10,-10,null,2645586035756450619,'2023-12-10','2024-02-18','y','b',null,'h'),(1,5,2,-2690774847697924751,1215476708431778901,'2024-02-18','2023-12-11','a','i','d','g'),(2,7,6,null,-8503685257450164774,null,'2024-02-18','x','v',null,'n'),(3,-10,-10,520762380322315749,-5497359840167914951,'2023-12-18','2024-02-18','u','o',null,'a'),(4,1,8,8426571400400527236,-6855182030117411879,null,'2023-12-15','h','p',null,'z'),(5,6,0,7890005520962505648,8471917651989306356,null,'2024-01-17','r','w','o','e'),(6,6,3,4564003077312279253,-5279002579025909756,'2023-12-15','2024-01-09','y','s','o','o'),(7,null,0,null,1132995983816292240,'2023-12-09','2023-12-17','u','x',null,'m'),(8,0,6,-1699287281520199807,2822764153664286747,'2024-02-18','2024-02-18','d','v',null,'r'),(9,null,-10,6334572184750612746,-5613178684708470111,'2023-12-19','2025-02-17','i','z',null,'z'),(10,null,2,-1992436339054842132,2034094530304445409,'2023-12-18','2023-12-09','n','t','y','h'),(11,5,4,4749548640745252992,-5156951223674084855,'2024-01-17','2023-12-20','f','u','h','k'),(12,7,-4,null,7909917511343685905,'2024-02-18','2023-12-17','t','j','p','d'),(13,-10,-4,null,-6758723579021226316,'2024-02-18','2023-12-12','s','y','k','i'),(14,3,3,null,7133643810781621084,'2024-02-18','2024-02-18','s','k','c','t'),(15,null,8,-6831107303276650640,-9086077819847862049,'2023-12-13','2027-01-09','j','g','h','t'),(16,2,-10,-7498861201033902386,3969006027560321797,'2023-12-15','2023-12-13','x','y',null,'w'),(17,7,-10,-8105575907795959065,1690192715022907441,'2027-01-16','2026-02-18','k','s','b','t'),(18,-4,7,-8885162528006297273,-6542433357775550501,null,'2026-02-18','m','p','m','c'),(19,6,5,null,-7082942929823812266,'2023-12-09','2024-02-18','j','x','f','o'),(20,null,6,550611770383001102,3097604765602788923,'2023-12-15','2027-01-16','j','b','a','z'),(21,-4,1,null,6855938526244903249,'2024-02-18','2024-01-31','k','e','x','t'),(22,null,5,null,4149143608990714211,'2027-01-09','2025-06-18','t','h','p','c'),(23,9,3,-1557157682461309640,-901152637546506456,'2023-12-10','2023-12-18','m','x',null,'f'),(24,-4,-4,3950657143112196560,-3070402654311007073,'2025-02-18','2024-01-09','h','q','l','r'),(25,null,-10,7646256222121626512,-8590358394491527798,'2027-01-09','2024-01-09','f','y','u','k'),(26,4,-10,-2648869623569245829,2760135151514585499,'2024-01-19','2025-02-17',null,'h','q','b'),(27,null,-4,8656442618627726600,-8948215279383341295,'2026-01-18','2023-12-19','p','s','e','f'),(28,null,5,6134406933012952591,-1358402315923614396,'2023-12-10','2024-02-18','s','q','h','s'),(29,6,7,2436714607099279193,-1279756124579970218,'2023-12-11','2023-12-17','m','p','d','p'),(30,7,5,-1750443579854282361,7106118794883753564,'2026-02-18','2023-12-12','g','k','t','t'),(31,-4,6,null,64842552461434207,null,'2024-01-09','f','q','f','z'),(32,7,-10,-4338090687532952715,-6208232298469211838,'2023-12-17','2023-12-11','u','e','d','u'),(33,null,-10,-4674542846645924019,5735521293616448577,'2023-12-14','2026-02-18','t','a',null,'z'),(34,9,9,3627932811692661320,3795967147978115152,'2023-12-15','2026-02-18','p','e','c','a'),(35,1,6,5536158700337375024,-591766285162144412,'2023-12-12','2024-02-18','c','i','l','h'),(36,null,8,1329891442119596171,-1302850587082830804,null,'2023-12-18','n','a','b','d'),(37,7,1,-6726486506994963987,-2551755988662995089,'2025-02-17','2023-12-16','e','r','n','u'),(38,9,-10,8155129466143198463,-7021397803442808636,null,'2023-12-18','r','n',null,'f'),(39,null,-10,null,-9207523170784038623,null,'2023-12-16','k','e',null,'r'),(40,2,9,-6721578756409031987,-6577320516087790787,'2023-12-10','2024-02-18','f','y','z','o'),(41,-10,-4,null,4508709402058736544,'2025-06-18','2023-12-09',null,'u','o','g'),(42,-10,6,402269795075225932,-2298504324508633554,'2023-12-18','2024-01-17','u','s','s','t'),(43,3,6,459733246883385048,1109831300978452707,'2024-01-17','2026-02-18','q','q','a','y'),(44,0,8,-3671115414193458658,1820212151119373421,'2023-12-13','2023-12-13','t','b','n','o'),(45,5,-10,-3480021505815205468,-6013887805368815266,null,'2024-01-09','w','b','e','j'),(46,null,5,7158675948400165237,3238930492704256944,'2024-01-09','2024-02-18','t','j',null,'b'),(47,6,5,null,2400042264989461448,'2024-01-08','2023-12-09','v','f','n','v'),(48,2,-10,-1907573593382808291,5868076010708171873,'2027-01-16','2023-12-16','h','r','f','r'),(49,2,-4,6175251538526429832,-7296146889268486617,'2023-12-10','2023-12-15','q','z','z','a'),(50,2,5,-6853615757273098893,-4477860997180100143,'2025-02-17','2025-02-18','n','h','k','t'),(51,9,8,-5474847916989646893,1881931482227450304,'2023-12-13','2023-12-11','j','m','v','k'),(52,-4,-10,316983665207761471,2880066856551004645,'2023-12-16','2025-06-18','p','u',null,'s'),(53,7,2,5436812104250699063,-5378783616311145349,'2025-02-18','2026-01-18','o','r','u','x'),(54,null,5,null,-3935130114756799733,'2023-12-10','2027-01-09',null,'l','g','k'),(55,1,5,null,-7944134629303276090,'2026-01-18','2024-01-08','g','h','y','e'),(56,2,-4,null,3526197536355187784,null,'2024-02-18','s','l',null,'y'),(57,-10,-10,4700358426229775076,3656507838703567577,'2024-02-18','2024-02-18','w','q','g','w'),(58,-10,-10,-6760334660136209645,-3323188714851336901,'2025-06-18','2023-12-20',null,'n','t','s'),(59,null,-10,2367161100508758500,-4755616101153147837,'2024-02-18','2024-01-17','y','c','r','d'),(60,5,9,-3579984224774194844,-3289445185637326787,'2024-01-17','2023-12-14','q','v','o','r'),(61,-10,1,7700189458474030176,976322239058470994,'2023-12-15','2024-02-18','c','z','f','g'),(62,2,-4,3419871630970256173,3742803623488250094,'2023-12-20','2023-12-14','u','a','q','o'),(63,3,3,null,8167879809510195755,null,'2024-01-09',null,'p','i','c'),(64,9,0,-5315740114754333984,6698096798804481639,'2025-06-18','2023-12-09',null,'m','d','q'),(65,-10,-4,3522789608006467478,565931047026295150,'2023-12-11','2024-01-19','h','p','p','s'),(66,6,-10,-6087428281007475222,4748176753105582444,'2024-02-18','2024-01-17','r','b','v','h'),(67,-10,3,-8988581264051453635,-3204113458070138868,'2027-01-09','2024-01-09','f','b','b','z'),(68,-4,6,-4088730199622548081,3245715051970397687,'2026-01-18','2025-02-17','m','k','v','g'),(69,-4,4,4485833650089492792,-5765440807092507850,null,'2023-12-13','j','b',null,'z'),(70,9,5,null,5699121742440989860,'2023-12-10','2026-02-18','i','k','e','x'),(71,-10,6,-6814075566389250498,4592845973073001590,'2025-02-18','2023-12-09','b','b','q','g'),(72,-4,-10,1502271229590221601,1243911740314869036,'2024-01-31','2023-12-17',null,'d',null,'h'),(73,5,9,null,-8496507876400782040,'2023-12-15','2023-12-14','s','h','y','z'),(74,null,4,null,-4564341848973592398,'2024-01-19','2023-12-20','r','f','d','l'),(75,1,-4,null,4297122139172774002,'2027-01-09','2027-01-09','j','c','q','v'),(76,7,-10,-1082614759903233841,4152575464907206785,'2023-12-14','2023-12-20','q','c','q','i'),(77,8,-10,null,7655895529419836098,'2023-12-19','2023-12-19','n','a','l','r'),(78,3,6,null,3811806633731562579,'2023-12-18','2023-12-16','m','h','z','k'),(79,-4,6,3016854459719381114,6499340236894414342,'2025-02-18','2023-12-18','x','m',null,'u'),(80,3,-10,-1507927599654581900,-911600993618886096,'2023-12-15','2023-12-09','p','y','b','e'),(81,0,1,-6708031186879538270,-6718150174608592798,'2026-02-18','2024-01-09','n','r','l','r'),(82,-4,8,null,-8552745601761971692,'2023-12-16','2023-12-14','i','z','e','k'),(83,4,-4,null,3090364581051975340,null,'2027-01-09','h','m','r','l'),(84,3,1,null,4057104115786585494,'2024-01-08','2024-01-08',null,'y','g','f'),(85,-4,0,1934713437373693072,-3344178905056928069,null,'2023-12-15','m','f','w','w'),(86,null,-10,-1900965900260657188,-2288273624988425009,'2024-02-18','2024-01-19','l','h',null,'i'),(87,null,4,null,6837314911593716925,'2023-12-09','2024-01-17','t','r','d','s'),(88,7,7,8031568680593562701,3122363206325250303,'2023-12-12','2027-01-09','y','n','p','y'),(89,5,4,-4602797504108242766,-4610267520006165889,'2023-12-10','2025-06-18','g','l','p','c'),(90,-10,1,null,-2186750569063124388,'2024-02-18','2025-02-17','e','r','a','d'),(91,9,9,777568294778675143,-2271766622706929315,'2025-02-17','2024-02-18','u','b','c','v'),(92,-4,6,3029387940824977021,3904555112700735405,'2027-01-16','2023-12-18','r','a','c','d'),(93,8,1,null,556090244295109032,'2023-12-15','2027-01-16',null,'k','z','x'),(94,7,4,null,-6234771716503677956,'2023-12-09','2023-12-16',null,'c',null,'e'),(95,-4,-10,-1513358233642519311,7979629453757136611,'2026-02-18','2023-12-14','p','w','w','p'),(96,0,-4,-2586617901382664647,2522909274199498768,null,'2026-02-18','x','b','u','v'),(97,-4,4,168929976776618324,7437208277532023736,'2025-06-18','2023-12-13','i','z','d','p'),(98,null,-10,4042601544833935634,-2258105390626666658,'2025-02-18','2024-01-19',null,'f','q','d'),(99,-4,5,-2635345250712442911,-2515828472595451777,'2024-01-31','2024-01-17','t','r','f','c'),(100,6,8,null,9005148203691199181,'2023-12-20','2023-12-09','b','j','q','a'),(101,6,-10,null,-5397364938953997666,'2023-12-17','2024-01-17','v','h','l','r'),(102,2,1,null,6492189422681315180,'2027-01-16','2026-01-18','e','a','t','y'),(103,0,6,null,-2743200820651975688,'2023-12-20','2023-12-15','r','m','r','w'),(104,9,9,6358840242872816544,4969071517159194722,'2025-06-18','2023-12-18',null,'s',null,'i'),(105,1,2,6361225611918147495,-4937163762212626370,'2024-01-08','2023-12-18','z','i','x','u'),(106,null,-4,null,-3432611196346309207,'2024-01-31','2023-12-16','i','n',null,'u'),(107,null,-10,6977182218690809649,-8112668674022841848,'2024-01-17','2024-01-09','e','p','n','k'),(108,-4,9,null,-8551053746430388921,null,'2023-12-12','j','h','u','l'),(109,9,0,-8695664601555622573,-2734196876806183851,'2023-12-14','2025-02-18','h','o','p','w'),(110,-10,5,8277442478207486189,-907711000632592369,'2023-12-09','2023-12-10','y','g','e','r'),(111,6,6,3711206094525456768,8582612312966005569,'2024-01-19','2024-02-18',null,'h',null,'c'),(112,-4,-4,-3269284785438982767,-7988217800239738626,'2023-12-20','2023-12-11','j','x','z','n'),(113,-10,-4,null,1786511470681546871,'2024-01-09','2025-02-18','x','r','d','m'),(114,-10,-4,5820178648521766461,2918495086831707113,'2024-01-17','2023-12-16','w','p',null,'v'),(115,-4,4,3270654326588765976,856034802480716278,'2027-01-09','2023-12-09','p','l',null,'q'),(116,-4,-10,597503071660166564,-69672135048573048,'2024-01-19','2023-12-16','c','z','h','c'),(117,5,4,null,8040729038705729508,'2023-12-12','2025-06-18','g','z','d','z'),(118,-10,-4,-7816572951369836464,-1514946022128726145,'2024-02-18','2025-06-18','x','b','i','y'),(119,4,7,5763324860717179567,-7124490056569846505,'2024-01-09','2024-01-19','q','x',null,'p'),(120,-4,8,-8560931913435517355,-243601470889502253,'2024-01-17','2024-01-09','p','b','t','l'),(121,null,3,null,-4336674969975011221,'2023-12-11','2024-01-09',null,'a','f','n'),(122,1,3,5187959453646393795,-8668963241579459761,'2024-02-18','2024-01-31',null,'g','o','c'),(123,-4,2,7061170843064234324,1735551525202102587,'2024-02-18','2023-12-10','k','j',null,'a'),(124,5,3,2150731458285670288,-8333163586917077463,'2024-01-09','2023-12-13','u','k','l','p'),(125,-4,2,-949585586963081328,5928250464394705254,'2023-12-15','2023-12-11',null,'i','n','z'),(126,-10,8,2054051926406507790,-4546389793428633140,'2023-12-15','2023-12-16','f','q','d','b'),(127,-4,-4,null,2506967531756073858,'2023-12-14','2023-12-14','k','u','r','g'),(128,1,-4,-4139040739112905789,5649574289594995478,'2024-02-18','2025-02-18',null,'d','u','t'),(129,9,-10,null,7135703846815456016,'2024-01-09','2024-02-18','d','m','e','e'),(130,-4,0,-3902460631371915857,-5774089896327669357,'2025-06-18','2024-02-18','v','b',null,'r'),(131,2,4,-1679528823790026727,6576965272591411154,null,'2023-12-15','a','k',null,'a'),(132,-4,3,4936526220922132848,2020256054180519408,'2024-01-09','2027-01-16','i','d','j','v'),(133,6,-10,-2656379775551453710,1596636018876811742,'2023-12-11','2025-06-18','n','l',null,'p'),(134,9,5,null,4741434430993074598,'2024-01-09','2025-02-18','z','j','z','c'),(135,-10,7,null,6990661637512043369,null,'2023-12-15','v','k','m','n'),(136,-10,-10,-1385386442041772901,946814442956243052,'2025-02-18','2023-12-13','a','d','s','y'),(137,null,1,-8088729767464845214,-4387286241209120971,'2024-02-18','2024-01-17','p','b','y','o'),(138,7,9,null,-977049954057827126,'2024-02-18','2026-01-18','l','v',null,'l'),(139,null,-4,705058180273238395,3503150154449551016,'2024-01-09','2023-12-11',null,'x',null,'v'),(140,1,7,3105404046646393246,-5424578922132556588,'2023-12-16','2023-12-09','k','x','f','h'),(141,3,4,-8221390215106914701,6438192845507482354,'2024-01-19','2026-02-18',null,'h','o','y'),(142,7,5,-1666861909405577191,8904325144234876946,null,'2027-01-16','o','v','o','a'),(143,1,-4,-6881774561253611432,-4159689698469060278,'2024-01-09','2023-12-14','w','b','y','u'),(144,7,6,null,126380004013122061,'2023-12-18','2023-12-14','f','l','g','k'),(145,-10,-4,6712916083658850805,687985511817204992,'2024-01-17','2023-12-14',null,'o','k','v'),(146,9,5,null,-4393278387096963228,'2026-02-18','2024-01-31','k','w','u','d'),(147,8,0,2004598620384150731,1130300787542069753,'2027-01-09','2023-12-10','l','w','x','b'),(148,8,2,8190386974592470981,-7389987331051735615,'2024-01-08','2023-12-15','f','z','a','r'),(149,6,-4,-7898134302050031961,7332000800450884842,'2026-01-18','2023-12-14','e','x','w','r'),(150,8,-10,null,-6140365730257001742,'2027-01-16','2023-12-12','p','v',null,'e'),(151,5,3,4293862417650220458,-1794444817080991426,'2024-01-19','2023-12-10','a','o','s','t'),(152,8,9,5932554131107685773,-2137353008812718324,null,'2027-01-09',null,'c',null,'a'),(153,9,-4,null,8473065625799885981,'2023-12-13','2025-02-18',null,'c','y','f'),(154,-4,5,3824764435098340729,-1313399362647027628,'2024-01-17','2025-06-18','e','l','o','o'),(155,-4,7,-4160697755020063268,846279066898771471,'2023-12-11','2027-01-16','o','q','x','g'),(156,8,-10,null,-4788267531961234997,null,'2025-02-17','u','x',null,'w'),(157,-4,7,-6378663341843309751,6634207025761515173,'2023-12-09','2024-01-31','l','d','m','d'),(158,7,6,-5080583267215669003,-4897768467702192440,'2023-12-18','2024-01-09','a','h',null,'e'),(159,-10,-10,null,3328371625086658757,'2024-01-08','2024-01-08','m','a','b','x'),(160,null,4,-7752689046150900257,-8757275378860279375,'2024-01-19','2027-01-09',null,'o','a','a'),(161,null,4,7343412063174120849,7186281192492274336,'2024-01-19','2024-01-17','u','m','b','m'),(162,-4,1,-6651841659474084692,-2023405967058491811,'2023-12-15','2023-12-09','e','w','w','g'),(163,2,0,3361641075790029243,-901088697703884129,'2023-12-20','2023-12-17','g','b','j','i'),(164,-10,2,-7230017577969421953,8204819811797830303,'2024-02-18','2024-01-17','v','m',null,'b'),(165,-10,2,1583526886368346361,-7161853984252725069,'2024-01-09','2026-02-18',null,'x','n','j'),(166,-10,1,2298516492664175007,3472496461528494286,'2023-12-16','2023-12-12',null,'n','y','s'),(167,-4,-4,-5315605963609373403,-9102296427952412061,'2025-02-18','2026-02-18',null,'w','p','i'),(168,9,-4,null,-1252481590995350050,'2024-02-18','2023-12-17','b','e','s','d'),(169,5,1,-8753877353543564203,-6175053478845016314,'2025-06-18','2023-12-16','s','f','u','q'),(170,5,0,990800295682477613,2799075535320043718,null,'2025-02-17','f','q','f','c'),(171,5,5,null,-591755362293704290,null,'2024-01-09','z','a','u','l'),(172,3,-4,8201569815117240304,2242308006407256165,'2023-12-13','2023-12-14',null,'x','y','c'),(173,9,2,null,1852058823734074775,'2023-12-18','2025-06-18','p','u','m','c'),(174,null,5,-8889561335223354419,6221999840242465455,'2025-06-18','2024-02-18','j','j',null,'z'),(175,2,-4,-2349754296131649302,-820303390170684951,null,'2025-02-17','r','w',null,'g'),(176,-4,4,-7362681390194413726,3677959699184423505,'2023-12-19','2026-01-18','o','g','y','u'),(177,3,4,null,-3564414761463401291,'2023-12-17','2023-12-16','f','p','b','y'),(178,7,3,-2128518821089208141,-6350088635617477381,null,'2024-01-17','n','b',null,'p'),(179,2,-10,7305419881717844511,1180316536940998245,'2023-12-20','2023-12-19','y','h','r','q'),(180,8,2,null,338600889123554537,'2023-12-16','2026-01-18','f','j','d','g'),(181,1,5,null,-2386855020256701509,'2024-01-17','2024-02-18','b','a',null,'t'),(182,8,-4,3534780095798127505,407639411207232849,null,'2023-12-16',null,'r','x','p'),(183,2,3,-5534383628677821218,-6923472615972728417,null,'2023-12-16','d','x','e','k'),(184,9,7,null,6519608354520951567,'2024-01-19','2023-12-19','k','o','u','q'),(185,-4,-10,null,5092925997579855568,'2023-12-20','2023-12-15','j','q',null,'y'),(186,7,1,6270317506128505289,3990621658248220425,'2024-01-17','2023-12-14','c','m','u','e'),(187,2,-10,null,-5002887458641756442,'2023-12-20','2023-12-18','y','z','u','k'),(188,2,-10,-5236526017717588017,5409551118106523639,'2024-01-19','2024-01-09',null,'v','d','y'),(189,null,3,5059017458869123548,291007899042420028,'2023-12-10','2023-12-14','e','q','q','v'),(190,-4,9,null,-7683700134368776817,'2024-02-18','2027-01-09','g','d',null,'a'),(191,3,-4,null,7510804564796880197,null,'2025-02-17','x','c','k','d'),(192,0,7,1712035495949299925,-6477785049089542387,'2024-01-08','2026-02-18','x','k',null,'s'),(193,-4,3,6381830129766418659,-2500838801221063480,'2024-02-18','2023-12-14','e','h',null,'v'),(194,5,3,-572306848787275516,2341692547342192817,'2023-12-10','2027-01-16',null,'z','z','o'),(195,2,4,790136624445165943,-8339618146200088691,'2025-02-18','2027-01-09','t','o','p','o'),(196,1,-10,null,4646568368267145076,'2026-01-18','2023-12-14','v','e',null,'x'),(197,9,7,8410191155989992217,-1416560322666412812,'2027-01-16','2026-02-18','r','o','v','z'),(198,null,3,4484323874065921341,2323040331736148079,'2025-02-18','2024-02-18',null,'w','r','f'),(199,null,-10,2800861564203872391,-8182937391176904133,'2024-01-31','2024-02-18','h','u','f','p');
    """
    sql "drop table if exists table_50_undef_partitions2_keys3_properties4_distributed_by54"
    sql """
create table table_50_undef_partitions2_keys3_properties4_distributed_by54 (
col_date_undef_signed_not_null date  not null ,
col_bigint_undef_signed_not_null bigint  not null ,
col_int_undef_signed int  null ,
col_int_undef_signed_not_null int  not null ,
col_bigint_undef_signed bigint  null ,
col_date_undef_signed date  null ,
col_varchar_10__undef_signed varchar(10)  null ,
col_varchar_10__undef_signed_not_null varchar(10)  not null ,
col_varchar_1024__undef_signed varchar(1024)  null ,
col_varchar_1024__undef_signed_not_null varchar(1024)  not null ,
pk int
) engine=olap
UNIQUE KEY(col_date_undef_signed_not_null, col_bigint_undef_signed_not_null)
PARTITION BY             RANGE(col_date_undef_signed_not_null) (
                PARTITION p0 VALUES LESS THAN ('2023-12-11'),
                PARTITION p1 VALUES LESS THAN ('2023-12-15'),
                PARTITION p2 VALUES LESS THAN ('2023-12-16'),
                PARTITION p3 VALUES LESS THAN ('2023-12-25'),
                PARTITION p4 VALUES LESS THAN ('2024-01-18'),
                PARTITION p5 VALUES LESS THAN ('2026-02-18'),
                PARTITION p6 VALUES LESS THAN ('5024-02-18'),
                PARTITION p100 VALUES LESS THAN ('9999-12-31')
            )
        
distributed by hash(col_bigint_undef_signed_not_null) buckets 30
properties("enable_unique_key_merge_on_write" = "true", "replication_num" = "1");
    """

    sql """
insert into table_50_undef_partitions2_keys3_properties4_distributed_by54(pk,col_int_undef_signed,col_int_undef_signed_not_null,col_bigint_undef_signed,col_bigint_undef_signed_not_null,col_date_undef_signed,col_date_undef_signed_not_null,col_varchar_10__undef_signed,col_varchar_10__undef_signed_not_null,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_not_null) values (0,-4,0,-4166296231541746094,9091136508347176422,'2023-12-15','2024-02-18','v','b','d','n'),(1,3,2,3325443409139458861,-8010992434382725403,null,'2023-12-11','z','q','s','h'),(2,null,3,-2887017536899616286,7356384502503341164,null,'2025-02-18','z','t','l','b'),(3,2,8,-8048311323177415055,3873410225424626279,'2024-01-19','2025-06-18','y','b','c','o'),(4,null,5,null,-1464527940131577867,'2023-12-10','2024-01-08','e','i','b','j'),(5,null,7,4544594356663084024,3602972684527900545,'2023-12-10','2023-12-15','o','h','a','d'),(6,-10,9,3350577541990167057,-4169305554433277308,'2024-02-18','2023-12-18','u','v','g','k'),(7,3,-10,-6605894629720570225,-2600472129800689748,null,'2025-06-18',null,'j','q','e'),(8,-4,-10,-2825321777707017621,-7389541799034672280,'2026-02-18','2024-01-31','o','e','q','z'),(9,2,2,8288749973141658542,1940436605730153428,'2024-01-17','2024-02-18',null,'i','u','m'),(10,0,0,-5299071034298035980,1091644826671031731,'2024-02-18','2023-12-11','b','q','t','x'),(11,3,2,-1352652135375662433,2658141508999815628,'2024-01-17','2023-12-18','o','a',null,'i'),(12,-10,7,null,153150344220771350,'2024-01-19','2024-01-17','g','q',null,'m'),(13,6,4,7997073523974410508,5278963976700533058,'2027-01-16','2023-12-13','n','e',null,'c'),(14,8,9,1038442964279859693,-2769552201273293063,'2023-12-12','2023-12-11','y','x','z','p'),(15,4,2,3955338675754944382,-630958506909184164,null,'2023-12-13','a','m','v','n'),(16,9,9,-740785311415533808,217339660992136633,'2023-12-15','2025-06-18','b','c','h','m'),(17,0,4,2780240607503305467,-6770607060145536645,'2024-01-17','2023-12-17','w','d',null,'p'),(18,4,4,-1463419968601152699,-4536569457109597126,'2024-01-17','2023-12-16','x','s',null,'d'),(19,-4,-10,6178268017244634721,3565553075207636604,'2024-01-17','2024-01-31','t','o','g','w'),(20,1,-10,null,8873250775367908916,'2024-02-18','2025-02-17',null,'r',null,'l'),(21,4,1,null,-6478990544672348514,'2024-01-08','2023-12-14','r','t','y','q'),(22,9,1,-3613005284814268385,-1180779656176182474,'2026-02-18','2025-02-17',null,'z','j','i'),(23,-4,2,-5086433636725140156,4160748444503848549,'2024-01-19','2025-06-18','q','a',null,'f'),(24,0,0,-6753387890836012744,789299336756559485,null,'2025-06-18','u','r','z','f'),(25,-4,-10,-8477048764472819243,-2475749746811852930,'2023-12-11','2026-01-18',null,'h','z','i'),(26,-10,9,2897237664796857285,7570476253224031,'2025-02-17','2025-02-17','z','p',null,'v'),(27,-4,9,-864547328366123265,665402194200777428,'2023-12-17','2023-12-20',null,'m',null,'x'),(28,-4,6,6652764571122917520,-3218906701556234341,'2023-12-19','2023-12-17','t','v',null,'h'),(29,-4,-10,null,6113347714178116592,'2024-02-18','2023-12-19','k','j','j','l'),(30,null,2,543485052212526501,255472557184745812,'2024-02-18','2024-02-18','m','u','z','t'),(31,null,-10,null,5883932925395761967,'2024-01-31','2024-02-18','r','r','w','c'),(32,8,0,7796032066845856506,7939870072648062159,null,'2023-12-15','t','w','f','i'),(33,4,7,5097727617456781058,-4796666542506820482,null,'2024-02-18','f','y',null,'z'),(34,4,4,null,2605902601141323830,null,'2023-12-17',null,'x','n','b'),(35,6,-4,3535279716386235508,9019241816926147784,'2025-02-18','2023-12-10','u','b','a','f'),(36,7,-10,null,-9222186925904062015,'2023-12-09','2025-06-18','y','m','m','j'),(37,-4,6,3515684917295413993,-7978400029691926991,null,'2024-01-09','l','e','c','h'),(38,7,5,null,7906586646358959329,'2027-01-16','2023-12-16','a','v','g','y'),(39,8,3,3716002649148325773,2591123033741207742,'2024-01-08','2024-01-17','n','x','a','e'),(40,-4,0,5736479373189694512,-2574575054522288980,'2023-12-20','2027-01-16','v','g','r','i'),(41,5,1,-5030319170172995004,5958431021725051460,'2025-02-17','2024-01-08','m','d',null,'m'),(42,6,-4,null,6106119907422035695,null,'2023-12-15','v','a',null,'v'),(43,9,6,null,317869140536544021,'2024-01-19','2025-06-18','u','o',null,'m'),(44,0,-4,4644231163497973928,7258852274143003939,null,'2026-02-18','y','y','d','l'),(45,3,5,-9048212892226464048,-4405963059284225546,'2027-01-09','2025-06-18','z','j','t','y'),(46,4,2,5050908234295313165,-1944666115706246625,'2024-01-09','2024-02-18','y','u','g','q'),(47,4,3,-8641879335991108274,2015291673128708627,'2023-12-12','2026-02-18','r','j','w','x'),(48,2,5,4002545200906629227,6786699306726066257,'2024-01-09','2023-12-11',null,'b','a','j'),(49,7,9,-8612109966811613034,-8778053141729838272,'2023-12-19','2023-12-11',null,'b','i','q');
    """
    qt_test """
SELECT TO_DATE ( table1 . `col_date_undef_signed_not_null` ) AS field1, MAX( distinct table1 . `col_int_undef_signed_not_null` ) AS field2, ( TO_DATE (CASE table1 . col_date_undef_signed_not_null WHEN table1 . col_date_undef_signed_not_null THEN DATE_ADD( table1 . `col_date_undef_signed_not_null` , INTERVAL 3 YEAR ) WHEN table1 . col_date_undef_signed THEN '2024-01-31' WHEN '2025-02-18' THEN '2024-02-18' WHEN '2008-09-25' THEN DATE_SUB( table1 . `col_date_undef_signed` , INTERVAL 7 DAY ) ELSE DATE_ADD( table1 . `col_date_undef_signed_not_null` , INTERVAL 2 DAY ) END)) AS field3, COUNT( distinct TO_DATE (ifnull( table1 . `col_date_undef_signed_not_null` , table1 . `col_date_undef_signed_not_null` )) ) AS field4, table1 . col_int_undef_signed AS field5, MIN( distinct table1 . `col_int_undef_signed` ) AS field6 FROM table_50_undef_partitions2_keys3_properties4_distributed_by54 AS table1 INNER JOIN table_50_undef_partitions2_keys3_properties4_distributed_by54 AS table2 ON ( table2 . `col_date_undef_signed` = table1 . `col_date_undef_signed` ) RIGHT JOIN table_200_undef_partitions2_keys3_properties4_distributed_by5 AS table3 ON ( table3 . `col_date_undef_signed` = table2 . `col_date_undef_signed` ) WHERE  ( table2 . `col_int_undef_signed` != 4 )  GROUP BY field1,field3,field5  ORDER BY field1,field3,field5 LIMIT 1000 OFFSET 9; 
    """

    sql "drop table if exists table_800_undef_partitions2_keys3_properties4_distributed_by524;"
    sql """
create table table_800_undef_partitions2_keys3_properties4_distributed_by524 (
pk int,
col_int_undef_signed_index_inverted int  null  ,
col_date_undef_signed_not_null date  not null  ,
col_varchar_1024__undef_signed varchar(1024)  null  ,
col_boolean_undef_signed boolean  null  ,
col_boolean_undef_signed_not_null boolean  not null  ,
col_tinyint_undef_signed tinyint  null  ,
col_tinyint_undef_signed_index_inverted tinyint  null  ,
col_tinyint_undef_signed_not_null tinyint  not null  ,
col_tinyint_undef_signed_not_null_index_inverted tinyint  not null  ,
col_smallint_undef_signed smallint  null  ,
col_smallint_undef_signed_index_inverted smallint  null  ,
col_smallint_undef_signed_not_null smallint  not null  ,
col_smallint_undef_signed_not_null_index_inverted smallint  not null  ,
col_int_undef_signed int  null  ,
col_int_undef_signed_not_null int  not null  ,
col_int_undef_signed_not_null_index_inverted int  not null  ,
col_bigint_undef_signed bigint  null  ,
col_bigint_undef_signed_index_inverted bigint  null  ,
col_bigint_undef_signed_not_null bigint  not null  ,
col_bigint_undef_signed_not_null_index_inverted bigint  not null  ,
col_decimal_16__8__undef_signed decimal(16, 8)  null  ,
col_decimal_16__8__undef_signed_index_inverted decimal(16, 8)  null  ,
col_decimal_16__8__undef_signed_not_null decimal(16, 8)  not null  ,
col_decimal_16__8__undef_signed_not_null_index_inverted decimal(16, 8)  not null  ,
col_decimal_38__9__undef_signed decimal(38, 9)  null  ,
col_decimal_38__9__undef_signed_index_inverted decimal(38, 9)  null  ,
col_decimal_38__9__undef_signed_not_null decimal(38, 9)  not null  ,
col_decimal_38__9__undef_signed_not_null_index_inverted decimal(38, 9)  not null  ,
col_decimal_38__30__undef_signed decimal(38, 30)  null  ,
col_decimal_38__30__undef_signed_index_inverted decimal(38, 30)  null  ,
col_decimal_38__30__undef_signed_not_null decimal(38, 30)  not null  ,
col_decimal_38__30__undef_signed_not_null_index_inverted decimal(38, 30)  not null  ,
col_date_undef_signed date  null  ,
col_date_undef_signed_index_inverted date  null  ,
col_date_undef_signed_not_null_index_inverted date  not null  ,
col_datetime_undef_signed datetime  null  ,
col_datetime_undef_signed_index_inverted datetime  null  ,
col_datetime_undef_signed_not_null datetime  not null  ,
col_datetime_undef_signed_not_null_index_inverted datetime  not null  ,
col_datetime_3__undef_signed datetime(3)  null  ,
col_datetime_3__undef_signed_index_inverted datetime(3)  null  ,
col_datetime_3__undef_signed_not_null datetime(3)  not null  ,
col_datetime_3__undef_signed_not_null_index_inverted datetime(3)  not null  ,
col_datetime_6__undef_signed datetime(6)  null  ,
col_datetime_6__undef_signed_index_inverted datetime(6)  null  ,
col_datetime_6__undef_signed_not_null datetime(6)  not null  ,
col_datetime_6__undef_signed_not_null_index_inverted datetime(6)  not null  ,
col_char_255__undef_signed char(255)  null  ,
col_char_255__undef_signed_index_inverted char(255)  null  ,
col_char_255__undef_signed_index_inverted_p_e char(255)  null  ,
col_char_255__undef_signed_index_inverted_p_u char(255)  null  ,
col_char_255__undef_signed_not_null char(255)  not null  ,
col_char_255__undef_signed_not_null_index_inverted char(255)  not null  ,
col_char_255__undef_signed_not_null_index_inverted_p_e char(255)  not null  ,
col_char_255__undef_signed_not_null_index_inverted_p_u char(255)  not null  ,
col_varchar_1024__undef_signed_index_inverted varchar(1024)  null  ,
col_varchar_1024__undef_signed_index_inverted_p_e varchar(1024)  null  ,
col_varchar_1024__undef_signed_index_inverted_p_u varchar(1024)  null  ,
col_varchar_1024__undef_signed_not_null varchar(1024)  not null  ,
col_varchar_1024__undef_signed_not_null_index_inverted varchar(1024)  not null  ,
col_varchar_1024__undef_signed_not_null_index_inverted_p_e varchar(1024)  not null  ,
col_varchar_1024__undef_signed_not_null_index_inverted_p_u varchar(1024)  not null  ,
INDEX col_tinyint_undef_signed_index_inverted_idx (`col_tinyint_undef_signed_index_inverted`) USING INVERTED,
INDEX col_tinyint_undef_signed_not_null_index_inverted_idx (`col_tinyint_undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_smallint_undef_signed_index_inverted_idx (`col_smallint_undef_signed_index_inverted`) USING INVERTED,
INDEX col_smallint_undef_signed_not_null_index_inverted_idx (`col_smallint_undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_bigint_undef_signed_index_inverted_idx (`col_bigint_undef_signed_index_inverted`) USING INVERTED,
INDEX col_bigint_undef_signed_not_null_index_inverted_idx (`col_bigint_undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_decimal_16__8__undef_signed_index_inverted_idx (`col_decimal_16__8__undef_signed_index_inverted`) USING INVERTED,
INDEX col_decimal_16__8__undef_signed_not_null_index_inverted_idx (`col_decimal_16__8__undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_decimal_38__9__undef_signed_index_inverted_idx (`col_decimal_38__9__undef_signed_index_inverted`) USING INVERTED,
INDEX col_decimal_38__9__undef_signed_not_null_index_inverted_idx (`col_decimal_38__9__undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_decimal_38__30__undef_signed_index_inverted_idx (`col_decimal_38__30__undef_signed_index_inverted`) USING INVERTED,
INDEX col_decimal_38__30__undef_signed_not_null_index_inverted_idx (`col_decimal_38__30__undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_date_undef_signed_index_inverted_idx (`col_date_undef_signed_index_inverted`) USING INVERTED,
INDEX col_date_undef_signed_not_null_index_inverted_idx (`col_date_undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_datetime_undef_signed_index_inverted_idx (`col_datetime_undef_signed_index_inverted`) USING INVERTED,
INDEX col_datetime_undef_signed_not_null_index_inverted_idx (`col_datetime_undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_datetime_3__undef_signed_index_inverted_idx (`col_datetime_3__undef_signed_index_inverted`) USING INVERTED,
INDEX col_datetime_3__undef_signed_not_null_index_inverted_idx (`col_datetime_3__undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_datetime_6__undef_signed_index_inverted_idx (`col_datetime_6__undef_signed_index_inverted`) USING INVERTED,
INDEX col_datetime_6__undef_signed_not_null_index_inverted_idx (`col_datetime_6__undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_char_255__undef_signed_index_inverted_idx (`col_char_255__undef_signed_index_inverted`) USING INVERTED,
INDEX col_char_255__undef_signed_index_inverted_p_e_idx (`col_char_255__undef_signed_index_inverted_p_e`) USING INVERTED PROPERTIES("parser" = "english"),
INDEX col_char_255__undef_signed_index_inverted_p_u_idx (`col_char_255__undef_signed_index_inverted_p_u`) USING INVERTED PROPERTIES("parser" = "unicode"),
INDEX col_char_255__undef_signed_not_null_index_inverted_idx (`col_char_255__undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_char_255__undef_signed_not_null_index_inverted_p_e_idx (`col_char_255__undef_signed_not_null_index_inverted_p_e`) USING INVERTED PROPERTIES("parser" = "english"),
INDEX col_char_255__undef_signed_not_null_index_inverted_p_u_idx (`col_char_255__undef_signed_not_null_index_inverted_p_u`) USING INVERTED PROPERTIES("parser" = "unicode"),
INDEX col_varchar_1024__undef_signed_index_inverted_idx (`col_varchar_1024__undef_signed_index_inverted`) USING INVERTED,
INDEX col_varchar_1024__undef_signed_index_inverted_p_e_idx (`col_varchar_1024__undef_signed_index_inverted_p_e`) USING INVERTED PROPERTIES("parser" = "english"),
INDEX col_varchar_1024__undef_signed_index_inverted_p_u_idx (`col_varchar_1024__undef_signed_index_inverted_p_u`) USING INVERTED PROPERTIES("parser" = "unicode"),
INDEX col_varchar_1024__undef_signed_not_null_index_inverted_idx (`col_varchar_1024__undef_signed_not_null_index_inverted`) USING INVERTED,
INDEX col_varchar_1024__undef_signed_not_null_index_inverted_p_e_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_p_e`) USING INVERTED PROPERTIES("parser" = "english"),
INDEX col_varchar_1024__undef_signed_not_null_index_inverted_p_u_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_p_u`) USING INVERTED PROPERTIES("parser" = "unicode")
) engine=olap
UNIQUE KEY(pk, col_int_undef_signed_index_inverted, col_date_undef_signed_not_null, col_varchar_1024__undef_signed)
distributed by hash(pk) buckets 10
properties("bloom_filter_columns" = "col_int_undef_signed, col_int_undef_signed_not_null, col_date_undef_signed_not_null, col_varchar_1024__undef_signed, col_varchar_1024__undef_signed_not_null", "replication_num" = "1");
    """
    def sqlFile = new File(context.file.parent+'/data.txt')
    sql """$sqlFile.text"""

    qt_test """
SELECT
    col_date_undef_signed
FROM
    table_800_undef_partitions2_keys3_properties4_distributed_by524
where
    (
        case
            col_date_undef_signed
            when "2024-01-09" then 1
            when "2023-12-10" then 2
            else 0
        end
    ) = 1;
    """
}
