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
suite("test_index_rqg_bug4", "test_index_rqg_bug"){
    def table1 = "test_index_rqg_bug4_table1"
    def table2 = "test_index_rqg_bug4_table2"

    sql "drop table if exists ${table1}"
    sql "drop table if exists ${table2}"

    sql """
      create table ${table1} (
      col_date_undef_signed_not_null_index_inverted date  not null  ,
      col_bigint_undef_signed_not_null_index_inverted bigint  not null  ,
      col_bigint_undef_signed_not_null bigint  not null  ,
      col_int_undef_signed int  null  ,
      col_int_undef_signed_index_inverted int  null  ,
      col_int_undef_signed_not_null int  not null  ,
      col_int_undef_signed_not_null_index_inverted int  not null  ,
      col_bigint_undef_signed bigint  null  ,
      col_bigint_undef_signed_index_inverted bigint  null  ,
      col_date_undef_signed date  null  ,
      col_date_undef_signed_index_inverted date  null  ,
      col_date_undef_signed_not_null date  not null  ,
      col_varchar_10__undef_signed varchar(10)  null  ,
      col_varchar_10__undef_signed_index_inverted varchar(10)  null  ,
      col_varchar_10__undef_signed_not_null varchar(10)  not null  ,
      col_varchar_10__undef_signed_not_null_index_inverted varchar(10)  not null  ,
      col_varchar_1024__undef_signed varchar(1024)  null  ,
      col_varchar_1024__undef_signed_index_inverted varchar(1024)  null  ,
      col_varchar_1024__undef_signed_not_null varchar(1024)  not null  ,
      col_varchar_1024__undef_signed_not_null_index_inverted varchar(1024)  not null  ,
      pk int,
      INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
      INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,
      INDEX col_bigint_undef_signed_index_inverted_idx (`col_bigint_undef_signed_index_inverted`) USING INVERTED,
      INDEX col_bigint_undef_signed_not_null_index_inverted_idx (`col_bigint_undef_signed_not_null_index_inverted`) USING INVERTED,
      INDEX col_date_undef_signed_index_inverted_idx (`col_date_undef_signed_index_inverted`) USING INVERTED,
      INDEX col_date_undef_signed_not_null_index_inverted_idx (`col_date_undef_signed_not_null_index_inverted`) USING INVERTED,
      INDEX col_varchar_10__undef_signed_index_inverted_idx (`col_varchar_10__undef_signed_index_inverted`) USING INVERTED,
      INDEX col_varchar_10__undef_signed_not_null_index_inverted_idx (`col_varchar_10__undef_signed_not_null_index_inverted`) USING INVERTED,
      INDEX col_varchar_1024__undef_signed_index_inverted_idx (`col_varchar_1024__undef_signed_index_inverted`) USING INVERTED,
      INDEX col_varchar_1024__undef_signed_not_null_index_inverted_idx (`col_varchar_1024__undef_signed_not_null_index_inverted`) USING INVERTED
      ) engine=olap
      UNIQUE KEY(col_date_undef_signed_not_null_index_inverted, col_bigint_undef_signed_not_null_index_inverted, col_bigint_undef_signed_not_null)
      PARTITION BY             RANGE(col_date_undef_signed_not_null_index_inverted) (
                      FROM ('2023-12-09') TO ('2024-03-09') INTERVAL 1 DAY,
                      FROM ('2025-02-16') TO ('2025-03-09') INTERVAL 1 DAY,
                      FROM ('2025-06-18') TO ('2025-06-20') INTERVAL 1 DAY,
                      FROM ('2026-01-01') TO ('2026-03-09') INTERVAL 1 DAY,
                      FROM ('2027-01-01') TO ('2027-02-09') INTERVAL 1 DAY
                  )
      distributed by hash(col_bigint_undef_signed_not_null_index_inverted)
      properties("enable_unique_key_merge_on_write" = "true", "replication_num" = "1");
    """

    sql """
      create table ${table2} (
      col_date_undef_signed_not_null date  not null  ,
      col_bigint_undef_signed_not_null_index_inverted bigint  not null  ,
      col_bigint_undef_signed_not_null bigint  not null  ,
      col_int_undef_signed int  null  ,
      col_int_undef_signed_index_inverted int  null  ,
      col_int_undef_signed_not_null int  not null  ,
      col_int_undef_signed_not_null_index_inverted int  not null  ,
      col_bigint_undef_signed bigint  null  ,
      col_bigint_undef_signed_index_inverted bigint  null  ,
      col_date_undef_signed date  null  ,
      col_date_undef_signed_index_inverted date  null  ,
      col_date_undef_signed_not_null_index_inverted date  not null  ,
      col_varchar_10__undef_signed varchar(10)  null  ,
      col_varchar_10__undef_signed_index_inverted varchar(10)  null  ,
      col_varchar_10__undef_signed_not_null varchar(10)  not null  ,
      col_varchar_10__undef_signed_not_null_index_inverted varchar(10)  not null  ,
      col_varchar_1024__undef_signed varchar(1024)  null  ,
      col_varchar_1024__undef_signed_index_inverted varchar(1024)  null  ,
      col_varchar_1024__undef_signed_not_null varchar(1024)  not null  ,
      col_varchar_1024__undef_signed_not_null_index_inverted varchar(1024)  not null  ,
      pk int,
      INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
      INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,
      INDEX col_bigint_undef_signed_index_inverted_idx (`col_bigint_undef_signed_index_inverted`) USING INVERTED,
      INDEX col_bigint_undef_signed_not_null_index_inverted_idx (`col_bigint_undef_signed_not_null_index_inverted`) USING INVERTED,
      INDEX col_date_undef_signed_index_inverted_idx (`col_date_undef_signed_index_inverted`) USING INVERTED,
      INDEX col_date_undef_signed_not_null_index_inverted_idx (`col_date_undef_signed_not_null_index_inverted`) USING INVERTED,
      INDEX col_varchar_10__undef_signed_index_inverted_idx (`col_varchar_10__undef_signed_index_inverted`) USING INVERTED,
      INDEX col_varchar_10__undef_signed_not_null_index_inverted_idx (`col_varchar_10__undef_signed_not_null_index_inverted`) USING INVERTED,
      INDEX col_varchar_1024__undef_signed_index_inverted_idx (`col_varchar_1024__undef_signed_index_inverted`) USING INVERTED,
      INDEX col_varchar_1024__undef_signed_not_null_index_inverted_idx (`col_varchar_1024__undef_signed_not_null_index_inverted`) USING INVERTED
      ) engine=olap
      UNIQUE KEY(col_date_undef_signed_not_null, col_bigint_undef_signed_not_null_index_inverted, col_bigint_undef_signed_not_null)
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
      distributed by hash(col_bigint_undef_signed_not_null_index_inverted)
      properties("enable_unique_key_merge_on_write" = "true", "replication_num" = "1");
    """

    sql """
      insert into ${table1}(pk,col_int_undef_signed,col_int_undef_signed_index_inverted,col_int_undef_signed_not_null,col_int_undef_signed_not_null_index_inverted,col_bigint_undef_signed,col_bigint_undef_signed_index_inverted,col_bigint_undef_signed_not_null,col_bigint_undef_signed_not_null_index_inverted,col_date_undef_signed,col_date_undef_signed_index_inverted,col_date_undef_signed_not_null,col_date_undef_signed_not_null_index_inverted,col_varchar_10__undef_signed,col_varchar_10__undef_signed_index_inverted,col_varchar_10__undef_signed_not_null,col_varchar_10__undef_signed_not_null_index_inverted,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_index_inverted,col_varchar_1024__undef_signed_not_null,col_varchar_1024__undef_signed_not_null_index_inverted) values (0,null,null,-10,-4,null,-8106433477491953829,-4802033018054435794,-5992411272414375722,'2023-12-10',null,'2024-01-19','2024-01-31','z',null,'g','o','r','e','p','j'),(1,null,-10,-10,9,null,-3660727480492065155,-2430926070148262363,-8719629382169261564,'2024-02-18','2025-02-18','2024-01-17','2024-01-31',null,'u','x','v','q',null,'i','w'),(2,null,6,3,4,7891191188234077318,2933716459221707048,-4258365664012872126,1078080062306377748,'2024-01-08',null,'2024-01-17','2023-12-18','i','b','f','m','e','d','v','f'),(3,9,7,-4,7,1220960226029024856,600673830462718207,6718338686818032732,-6190146023502832604,'2023-12-10','2023-12-10','2026-01-18','2023-12-10','a','r','s','g','r','i','b','d'),(4,2,-4,0,3,4185656264931585853,-5446372030713804479,-2608688930263553532,7869697967238665597,'2023-12-17','2024-02-18','2027-01-16','2023-12-15','y','q','z','r','s','m','a','h'),(5,8,6,6,1,-8057732588204386175,124582684071544652,4924742069387850579,-6637019824717901400,'2026-02-18','2023-12-18','2023-12-16','2024-01-19','j','y','t','b','l','c','a','i'),(6,3,-10,-10,9,-4023637318153148253,-2754393710366104080,-4712211419378518394,1855617570954714963,'2023-12-17','2024-02-18','2023-12-13','2027-01-09','m','x','p','t','s',null,'v','i'),(7,0,1,0,4,-5738014211668090731,-4683807563366365488,4787935554862968046,-5707801126253427853,'2023-12-10',null,'2025-06-18','2024-01-31','o','i','x','e','g','p','b','e'),(8,5,9,-10,-4,8416614078324884879,-495871618892611782,6517914945274107496,8086180684219698525,null,'2024-01-31','2023-12-16','2025-02-18','x',null,'v','c','q','z','i','h'),(9,1,-4,6,0,6157222804272078211,null,7992455355011438973,2635760727535496419,'2027-01-16','2023-12-18','2026-02-18','2023-12-14','m',null,'f','j','q',null,'l','p'),(10,1,4,9,3,-8536056250277879925,-8776432751990006562,8659539140666792075,-1797050225890110276,'2024-01-19','2025-02-18','2025-06-18','2023-12-10',null,'q','r','y','q','d','z','n'),(11,5,5,5,8,7224857378439572292,null,-9211767255224850856,881261003788149987,'2025-06-18','2024-01-17','2025-06-18','2025-02-18','x','o','g','f','s','b','z','z'),(12,5,-4,4,5,-462885203250818344,-6725301283266210242,-2917565685077389443,4654195353607267791,null,'2026-01-18','2024-01-09','2023-12-12','h','o','b','e','k',null,'o','q'),(13,2,0,7,5,-1953845528235077886,null,-1279716872768706296,5842288040722020479,null,'2023-12-16','2026-02-18','2023-12-09','s','r','f','c',null,'z','g','k'),(14,2,-4,4,-10,null,5431959970190124551,-3889957853385593850,-6121961635999277994,'2023-12-16','2025-06-18','2024-02-18','2023-12-10','g','q','z','j','g',null,'z','m'),(15,2,-10,-10,6,5439710353658982650,-4745847205095636748,-3627773213232785120,-6842410121072907914,'2023-12-19','2023-12-17','2023-12-10','2023-12-20','l',null,'d','o','u','b','k','q'),(16,4,null,7,-10,7811504772046995231,null,1770274235545960383,-8316425581002911063,null,'2023-12-11','2023-12-09','2025-06-18',null,'x','r','r','i','n','v','n'),(17,0,2,5,9,6727837024300139179,-778546542168175928,3455871215459760311,-4818691639390190003,'2024-01-19','2023-12-18','2025-02-17','2023-12-15',null,'f','k','c','f','w','q','v'),(18,5,-10,7,3,372004067976950191,1333800556703061816,2460652982150058280,-775316964581246846,null,'2024-01-17','2024-01-08','2025-02-17','o','r','l','b','j',null,'v','m'),(19,3,1,-10,3,-7784550107566735698,-8769834912612044169,1227511711437014365,-917749956744409154,'2024-01-31','2023-12-09','2024-02-18','2023-12-17','x','v','n','a',null,null,'m','o'),(20,9,-10,6,8,6255764142315507625,8739912244400256665,-2022085638103022141,6727105792412963692,'2026-01-18',null,'2023-12-16','2023-12-13','x','v','w','i','f','c','m','a'),(21,null,2,6,8,5943462376538127768,-6523070194436763884,2685916925490255899,-3664895298528491314,'2026-01-18','2023-12-10','2024-02-18','2024-02-18','g','r','n','h','g','l','k','j'),(22,2,-10,6,3,null,6140769122302771788,-5548487630277477546,5596564631485394070,'2023-12-17','2024-01-08','2024-01-09','2023-12-20','n','e','a','f','s','c','j','d'),(23,1,-10,-10,9,null,null,-8808158262524712053,-5335989453431031723,'2025-02-17','2023-12-12','2023-12-19','2026-01-18','f','o','a','f','w','k','t','g'),(24,-4,8,1,0,null,null,-1275549395060339347,5679808686975038166,'2026-02-18','2027-01-16','2025-02-18','2024-01-31','l','j','n','k',null,'e','q','d'),(25,2,null,-10,-10,-8673711102141939319,-2437235951522598119,-8836027704811375592,-762164198433467231,'2023-12-16','2024-01-19','2023-12-09','2024-01-19',null,'q','n','v',null,'e','j','m'),(26,null,0,4,4,-1127900356697248362,-352577873301987588,-429841398838910532,-7430782513212097196,'2023-12-16','2027-01-16','2025-06-18','2023-12-14','k','k','q','m','g','k','v','g'),(27,6,2,6,6,1126624924773634810,5801493949200181136,7830431182669843011,3696475878283701131,'2025-06-18',null,'2023-12-19','2023-12-10','s','q','k','j','l','o','y','k'),(28,9,1,-4,0,3333902147190483796,1012342138746595542,-2210154985724347963,-2534571922116710037,'2023-12-17','2023-12-12','2026-01-18','2023-12-18',null,'h','v','f','q','i','u','g'),(29,-4,-10,-10,-4,527509824391545264,-3652701503802335353,7999732248984552681,5743605366690330816,'2024-02-18',null,'2023-12-13','2023-12-19','l','c','w','g','v','a','e','j'),(30,2,9,3,5,3460832819499900201,null,-50239379625812774,2366814074545878197,'2023-12-18',null,'2023-12-18','2025-06-18',null,'s','b','n',null,'q','p','k'),(31,8,-10,-4,-4,-8059429112768327505,-6058507868321849453,-7290464822078467306,1977388257912283891,'2023-12-09','2023-12-20','2027-01-09','2023-12-13',null,'x','r','f','d','a','j','u'),(32,-10,3,7,6,-7559416125790713860,7626272583421550140,-7880165181669084133,3629921338222096940,'2025-02-17','2023-12-10','2024-02-18','2025-02-17',null,'f','t','q','v',null,'f','k'),(33,2,9,6,8,-4803091768405631061,-2317039993377096498,-1560020472949763679,-7758811838660539969,'2023-12-15','2024-01-31','2023-12-18','2023-12-12',null,'q','h','n','i','x','k','j'),(34,-4,-4,-4,0,1739090941830920904,-5339061187915777636,-7789794655033291131,-6235754378903829010,'2023-12-11','2027-01-09','2024-01-17','2025-06-18','f','k','m','n','m','c','p','a'),(35,7,6,-4,0,1728636166115379651,-7148291417315078305,6088623650330528552,5347878802282663293,'2023-12-20','2024-02-18','2025-02-18','2023-12-14',null,'g','l','t','f','r','o','z'),(36,2,9,-4,8,1146322196215181128,5270284680616892535,-472915687936844670,7598938472677921191,'2023-12-14','2024-01-17','2024-01-31','2026-02-18','g',null,'u','t','k','y','w','j'),(37,null,9,6,7,null,5708159522372944306,1590905005642959048,-3057084566170710938,'2025-06-18','2023-12-10','2023-12-16','2027-01-16','j','z','n','p','u','s','h','t'),(38,-4,-10,8,3,-5796434163230401738,-7130029882926549210,-1881329918715240705,-4298008427694582358,'2024-01-19','2027-01-16','2023-12-15','2023-12-20','b','k','s','t','t',null,'d','o'),(39,null,6,-4,-4,-5278757319722466171,null,328375824735772365,6369318777614676040,'2025-02-18',null,'2023-12-13','2024-01-19',null,'g','b','w','v','g','y','s'),(40,6,-10,-4,1,-6489645123067002445,-5502433471984282037,-5496598068665512181,-8973390036072978130,'2023-12-09','2024-02-18','2026-02-18','2026-02-18','c','m','d','l','f',null,'y','t'),(41,5,-4,9,1,null,null,7615485278964285138,266103423570383798,'2026-01-18','2025-06-18','2027-01-16','2024-02-18','h','g','n','w','v',null,'d','t'),(42,-10,-4,0,-4,4108565496193527959,null,2855942170213154109,-4503151074280279597,'2024-01-19','2027-01-09','2024-01-31','2026-01-18','q','r','c','n','f',null,'n','o'),(43,7,-10,7,1,null,3168808972110291881,-9021190639446017198,8383069847613511331,'2026-01-18','2024-02-18','2023-12-20','2024-01-08','o','t','i','y','c',null,'p','a'),(44,null,1,2,-4,-7716050566778897524,null,-5335513780667741467,6460293410615600105,'2024-02-18','2027-01-16','2023-12-09','2023-12-19','j',null,'o','u','m','f','d','r'),(45,5,-10,-10,-10,null,1770715116574871679,556243169425942999,1149153569981590533,'2026-01-18','2023-12-14','2023-12-11','2023-12-13','g','d','p','l','g',null,'o','y'),(46,-10,null,-10,-10,1058936912367135138,-1401849194921540708,4160813589004799051,1727031440015983904,'2024-02-18','2025-06-18','2023-12-16','2026-01-18','u','s','p','i','x','u','w','w'),(47,3,4,-10,3,7045177902217111465,5916226467184753164,-4871554468966160327,-2083910134922418070,'2023-12-12','2025-02-18','2023-12-13','2024-02-18','b','r','o','l','t','k','b','z'),(48,-4,-4,4,9,-6672037024099049115,-6661406554267690368,-6948401236141344277,-6946111456164759453,'2023-12-13','2023-12-14','2024-01-09','2025-02-17','v','h','w','k',null,'f','i','z'),(49,8,-4,2,3,-7001498594087950207,3653436167229140100,3409429992219169994,-1841100771255644074,'2024-01-31',null,'2023-12-17','2024-01-19','d','a','a','o','r','l','e','w');
    """

    sql """
      insert into ${table2}(pk,col_int_undef_signed,col_int_undef_signed_index_inverted,col_int_undef_signed_not_null,col_int_undef_signed_not_null_index_inverted,col_bigint_undef_signed,col_bigint_undef_signed_index_inverted,col_bigint_undef_signed_not_null,col_bigint_undef_signed_not_null_index_inverted,col_date_undef_signed,col_date_undef_signed_index_inverted,col_date_undef_signed_not_null,col_date_undef_signed_not_null_index_inverted,col_varchar_10__undef_signed,col_varchar_10__undef_signed_index_inverted,col_varchar_10__undef_signed_not_null,col_varchar_10__undef_signed_not_null_index_inverted,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_index_inverted,col_varchar_1024__undef_signed_not_null,col_varchar_1024__undef_signed_not_null_index_inverted) values (0,null,-10,1,2,-8075743792621141285,2734662917640882181,-3048315532213395347,8307248236739598635,'2027-01-16','2027-01-09','2025-06-18','2023-12-16','c','b','a','j','l',null,'j','q'),(1,3,6,5,5,6170388652161438133,822385282229125679,-9178021298456309629,-6448072632821879129,'2027-01-16','2024-02-18','2023-12-13','2026-02-18','d',null,'a','a',null,null,'q','b'),(2,-10,6,-4,8,5106550897626486199,-7135891550140816230,1036885892899682804,-63702068912664967,'2023-12-10','2026-01-18','2027-01-09','2026-02-18','e','o','f','t','z','v','j','t'),(3,8,9,-4,6,6712563041809771009,3722720734478299385,-4423043381871171611,-4432689578579260465,null,null,'2024-01-31','2023-12-09','m','v','m','p','i','y','n','x'),(4,7,-10,0,-10,null,1956752487907158854,-9061913373866619240,-7154949508077367730,'2023-12-19','2024-01-09','2023-12-16','2025-06-18','r','i','p','e','o',null,'f','i'),(5,-10,5,7,0,-6769925219445953647,5389188870599631934,-8663613684842857693,1162827325537966708,'2027-01-09','2024-01-08','2023-12-09','2023-12-11','r','d','t','b','e','l','s','h'),(6,-4,9,-10,-10,980967602513427,8455516262103018733,7272008947950545870,7813183182124818981,'2027-01-16','2024-02-18','2023-12-19','2023-12-10','e',null,'f','i',null,'m','q','h'),(7,5,9,0,1,-894682406494524686,2736941564233970858,4821050615487490027,-6144488446615481879,'2023-12-17','2023-12-10','2023-12-19','2024-02-18','x','j','l','x','a','p','u','r'),(8,9,1,-4,9,1482311970461610062,2332390989356796509,7798596972359705263,-797201522486146130,'2024-01-31','2023-12-10','2023-12-20','2023-12-10','d','s','x','f','o','d','a','x'),(9,-4,-4,-10,0,2684932411715931139,-8016529642093142936,-4256509885451347999,-8435248776445601672,'2024-02-18','2023-12-11','2024-01-17','2023-12-18','k',null,'r','h','e','w','z','z'),(10,4,0,-4,-4,-8571078188599908037,79929345862953934,-2268494408285567221,-7583920346203626596,'2025-02-18','2023-12-20','2023-12-16','2023-12-17','n','p','y','l','i','g','f','w'),(11,3,null,2,2,-1283632942836846925,null,-6303513886522662174,223995116756045964,'2024-01-17','2023-12-10','2024-01-09','2026-02-18',null,'e','h','k','t','i','f','l'),(12,-4,9,4,1,null,1521894768878204871,-442513424365647703,-4691670042696413990,'2027-01-09','2027-01-09','2023-12-18','2024-01-17','i','o','o','k','j','x','z','k'),(13,5,null,-4,-10,null,7202729657164949274,-187903138846428463,-8180419741518785047,'2024-02-18','2023-12-11','2023-12-09','2023-12-16','i',null,'l','b','s','s','e','f'),(14,null,0,4,4,-2616812201351418269,-6616226601080134448,-2929390900009725246,-8435530463480535020,'2023-12-13','2023-12-16','2025-02-18','2023-12-13','m','f','v','b','v','c','s','w'),(15,-10,null,2,6,null,-4031938246184421125,-734757013062495621,-5908890312058710584,'2024-02-18','2024-01-08','2023-12-17','2024-02-18','k',null,'a','f',null,'x','a','n'),(16,-4,3,-10,-10,null,-4559648458917878793,-3328100736643893394,3706694143765734729,'2025-02-17','2023-12-11','2025-02-18','2025-06-18','u','s','f','d','e','i','t','v'),(17,-10,5,7,-4,386514636147964659,null,-6824067400776793730,-932552576271736690,'2024-02-18','2025-06-18','2026-01-18','2023-12-16','w','g','o','z',null,null,'f','y'),(18,8,null,2,8,2445051121787207631,-8050057011556708447,-6536806493416327846,2640089136718143595,'2024-02-18','2023-12-16','2024-01-31','2024-02-18','w','q','q','y','g','r','k','i'),(19,-4,2,8,-10,3489428937435046370,null,8944066613840677976,-7419158180755571261,'2023-12-17','2024-02-18','2023-12-14','2025-02-17','t','b','r','e','h',null,'a','h'),(20,-4,null,9,-4,null,-1216805390610573337,3888352152462054487,5055668582608032908,'2023-12-09','2024-02-18','2026-02-18','2024-01-09','b','y','e','w','m','f','p','m'),(21,-4,null,6,0,2994470385252943818,null,-12379632095488396,152553367538571826,'2023-12-17','2023-12-18','2023-12-19','2027-01-09','v','p','o','j','v','z','v','y'),(22,-10,5,4,3,null,-4054919843675242653,-4860366343909254635,1919940173053442305,null,'2023-12-19','2024-02-18','2024-01-31','q','r','w','l','z','x','m','x'),(23,9,-4,8,1,null,-4852288446181060602,-8593690134625771395,813041083453661777,'2023-12-10',null,'2025-06-18','2025-06-18','w','v','s','a','w','g','o','w'),(24,2,-10,-10,-10,-2832742164813829158,-2143652086659867547,1307233024503335978,5406843843219987566,'2023-12-13','2024-01-08','2023-12-19','2025-06-18','t','h','n','t','j','y','x','g'),(25,-10,-4,2,7,-8683095377999059961,-6071284485360022401,3354393451874282781,-6341234020173954640,'2023-12-16','2023-12-17','2023-12-17','2027-01-09','l','g','k','s','x','n','n','q'),(26,9,7,2,-4,null,-8101961380551938349,-2630870189586913738,-4378525921622223456,'2026-02-18','2023-12-18','2027-01-09','2024-02-18','y','w','q','q','g','y','p','j'),(27,-10,1,-4,5,null,null,3297067028998411728,-7959693798336554301,'2027-01-16','2023-12-11','2023-12-15','2024-01-08','f','s','n','r','b','s','m','c'),(28,-10,0,3,2,5300452983031564117,null,2670779198155267377,5141197967343361168,'2024-01-17','2024-02-18','2026-02-18','2023-12-16','x',null,'v','a','z','v','m','k'),(29,5,6,8,2,null,null,3308086802751767526,169444577778942255,'2024-02-18',null,'2024-02-18','2027-01-09','z',null,'c','t','j','g','g','y'),(30,1,2,6,0,null,-1807216529029872584,-7555655864402759412,-6901163130617879304,'2023-12-09','2025-06-18','2024-01-09','2024-01-08',null,'w','j','c','z','f','h','x'),(31,null,4,0,5,7646373264427293940,1924592384362164836,-2874232051694862894,-7565291660307476088,'2023-12-14','2023-12-15','2025-06-18','2023-12-16','x','x','u','k','c','x','g','h'),(32,4,-10,5,-4,6145487557420096320,-482505174176527589,-7641704177823029118,8768829005926742336,'2027-01-09','2024-02-18','2024-01-17','2023-12-18','j','r','n','l','d','q','u','m'),(33,1,6,8,9,1850516374084242155,-6425949972949800728,6812365901574385949,8888094662734722781,'2025-06-18','2023-12-12','2025-02-18','2023-12-16','o','t','u','l','i','r','y','c'),(34,8,-10,9,1,2554317470521671455,null,3913836404319031027,1636278690783139427,'2023-12-16',null,'2023-12-17','2024-02-18','s','c','h','i',null,'d','f','d'),(35,8,-10,8,8,-3955711013464262214,5794982999814159321,314804909714134617,3544405689567540217,'2023-12-11','2023-12-11','2024-02-18','2027-01-16','f',null,'o','z','d','b','a','l'),(36,6,0,4,-10,null,-6765231039070013190,-5320402308335333960,3863002573152315803,'2023-12-13','2023-12-20','2025-02-18','2026-01-18','x','b','f','q','j',null,'g','p'),(37,4,-4,-4,-10,-9133718546048958753,-4518648403722436914,6100623509516681987,5708468593883020550,'2027-01-16','2023-12-16','2025-02-18','2025-02-18','s','i','c','h','z','o','k','o'),(38,-4,-10,-10,3,-7277594415800045941,4754497399857696314,3692929062168805852,3229421779218773432,'2027-01-09','2024-02-18','2025-02-18','2023-12-10','q',null,'x','b','z','l','r','o'),(39,4,8,1,-10,-8486468519634660502,null,-5599945230213727076,-3582482653407843949,'2023-12-16','2024-02-18','2025-06-18','2023-12-20','j','e','f','t','l','h','z','p'),(40,null,-4,-4,5,2480752007767543434,-4582947854790528662,3525297402575445055,2725424937375643618,'2023-12-19','2024-01-08','2027-01-09','2024-02-18',null,null,'v','t','g',null,'p','j'),(41,5,7,0,6,-410956214004308564,-1315695093500136014,3074944742322686852,-552688938740890673,'2025-02-17','2025-02-17','2024-02-18','2025-02-18','g','n','l','a','g','l','q','r'),(42,-10,null,-4,6,null,-5826842537949562093,7405068433598442956,-4366194334634247111,'2024-02-18','2023-12-09','2023-12-15','2023-12-18',null,'y','a','j','w','h','z','s'),(43,5,-10,1,6,-6484672808506095643,3722822631849361343,6307914284735008096,-5566766606129171015,null,'2023-12-20','2023-12-16','2023-12-15','d',null,'u','v','c','x','s','w'),(44,-10,-10,6,-4,null,6020329264654220829,7407603179849980276,-7595054584489583336,'2023-12-12',null,'2024-01-09','2025-06-18','p',null,'z','v','x',null,'c','b'),(45,-10,0,7,-10,-104892770433573241,662897028748152203,-3245502728946948267,5984260501449364544,'2023-12-14','2023-12-15','2024-02-18','2026-01-18','p',null,'i','p',null,'w','e','r'),(46,7,6,-4,6,8290392416638950158,-1169191183479044386,-1033067828885497304,7793163963274363865,'2024-02-18','2025-02-18','2027-01-16','2026-01-18',null,null,'n','s','n','t','h','x'),(47,8,null,6,6,7425697086646161265,-4949658717911474283,-1208946395949357200,-2777923163230804781,'2026-01-18','2027-01-16','2023-12-10','2023-12-18','k','n','y','s','f',null,'n','u'),(48,8,6,-4,6,-2305920794373084098,3862419411320814710,-3247489921688599586,6929777802997199035,'2023-12-11','2023-12-13','2026-02-18','2023-12-16','d','q','r','i','f','g','r','t'),(49,-10,-10,1,9,null,5055114889707700008,2871587922102250603,2191458422402058839,'2025-02-17','2025-06-18','2023-12-13','2024-01-19','r','q','k','r','k',null,'h','j');
    """

    try {
        sql "sync"

        qt_sql """
          select
            table1.col_varchar_1024__undef_signed_not_null as field1
          from
            ${table1} as table1
            right join ${table2} as table2 on (
              table2.col_date_undef_signed_index_inverted = table1.col_date_undef_signed_index_inverted
            )
          where
            not (
              (
                table2.`col_date_undef_signed_not_null_index_inverted` in (
                  '2027-01-16',
                  '2023-12-17',
                  '2024-02-18',
                  null,
                  '2000-10-18',
                  '2023-12-14',
                  '2023-12-18'
                )
              )
              and table2.`col_date_undef_signed_not_null_index_inverted` < '2025-06-18'
            )
          group by
            field1
          order by
            field1
          limit
            10000;
        """
    } finally {
    }
}
