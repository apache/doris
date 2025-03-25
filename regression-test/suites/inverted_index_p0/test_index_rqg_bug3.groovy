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
suite("test_index_rqg_bug3", "test_index_rqg_bug3"){
    def table1 = "test_index_rqg_bug3"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE ${table1} (
      `col_int_undef_signed_not_null` INT NOT NULL,
      `col_date_undef_signed_not_null` DATE NOT NULL,
      `col_bigint_undef_signed_not_null_index_inverted` BIGINT NOT NULL,
      `col_bigint_undef_signed_not_null` BIGINT NOT NULL,
      `col_int_undef_signed` INT NULL,
      `col_int_undef_signed_index_inverted` INT NULL,
      `col_int_undef_signed_not_null_index_inverted` INT NOT NULL,
      `col_bigint_undef_signed` BIGINT NULL,
      `col_bigint_undef_signed_index_inverted` BIGINT NULL,
      `col_date_undef_signed` DATE NULL,
      `col_date_undef_signed_index_inverted` DATE NULL,
      `col_date_undef_signed_not_null_index_inverted` DATE NOT NULL,
      `col_varchar_10__undef_signed` VARCHAR(10) NULL,
      `col_varchar_10__undef_signed_index_inverted` VARCHAR(10) NULL,
      `col_varchar_10__undef_signed_not_null` VARCHAR(10) NOT NULL,
      `col_varchar_10__undef_signed_not_null_index_inverted` VARCHAR(10) NOT NULL,
      `col_varchar_1024__undef_signed` VARCHAR(1024) NULL,
      `col_varchar_1024__undef_signed_index_inverted` VARCHAR(1024) NULL,
      `col_varchar_1024__undef_signed_not_null` VARCHAR(1024) NOT NULL,
      `col_varchar_1024__undef_signed_not_null_index_inverted` VARCHAR(1024) NOT NULL,
      `pk` INT NULL,
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
    ) ENGINE=OLAP
    UNIQUE KEY(`col_int_undef_signed_not_null`, `col_date_undef_signed_not_null`, `col_bigint_undef_signed_not_null_index_inverted`, `col_bigint_undef_signed_not_null`)
    DISTRIBUTED BY HASH(`col_bigint_undef_signed_not_null`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    insert into ${table1}(pk,col_int_undef_signed,col_int_undef_signed_index_inverted,col_int_undef_signed_not_null,col_int_undef_signed_not_null_index_inverted,col_bigint_undef_signed,col_bigint_undef_signed_index_inverted,col_bigint_undef_signed_not_null,col_bigint_undef_signed_not_null_index_inverted,col_date_undef_signed,col_date_undef_signed_index_inverted,col_date_undef_signed_not_null,col_date_undef_signed_not_null_index_inverted,col_varchar_10__undef_signed,col_varchar_10__undef_signed_index_inverted,col_varchar_10__undef_signed_not_null,col_varchar_10__undef_signed_not_null_index_inverted,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_index_inverted,col_varchar_1024__undef_signed_not_null,col_varchar_1024__undef_signed_not_null_index_inverted) values (0,null,null,6,-10,686522353588051391,null,-4348126053228614825,-6032791270525051561,'2025-06-18','2023-12-20','2024-02-18','2024-02-18',null,'o','o','x',null,null,'q','o'),(1,-4,-4,2,-4,-2326501514208488669,-8144130583099259882,6094773265621719520,-4389392644235126205,'2024-01-09','2023-12-19','2024-01-31','2024-02-18',null,null,'c','n',null,'k','p','y'),(2,-10,-4,-4,-10,-5761419801766661970,null,1575077084160592390,-6748295140918895470,'2023-12-09','2024-01-31','2023-12-11','2024-01-08','j',null,'m','k','p','s','s','u'),(3,2,null,3,6,7058901979703960583,-8791856880716210018,9136811854525385821,-584135196107353733,'2024-02-18','2023-12-14','2024-01-19','2024-02-18','e','h','i','b','x','a','h','d'),(4,6,-4,4,0,2965210993977402598,null,1623398766611349339,-719530465607559880,'2024-02-18','2024-01-31','2023-12-12','2027-01-16','c','p','h','o','t','l','t','k'),(5,-4,-10,7,-4,null,-4312428052005262901,1664741259066823538,-6520957605791451399,'2024-02-18','2024-01-19','2027-01-09','2024-01-31','f','d','q','k',null,'u','n','x'),(6,-10,0,4,-4,-4719935591389099252,-8933690584562378263,1354434296669241202,2684538782485650790,'2023-12-16','2026-01-18','2024-01-08','2025-06-18',null,null,'f','x','x','v','i','m'),(7,0,-10,-10,-10,-5480618991908294867,null,5334008298577672775,7173424974650158839,'2024-01-09',null,'2023-12-09','2024-02-18','y','k','x','u',null,null,'a','b'),(8,6,-4,5,8,-7188890154699493125,-1925845279956226794,-5657889190097714482,1640041513228273840,'2027-01-16',null,'2025-02-18','2023-12-20','y','x','p','w','d','j','k','d'),(9,null,-4,1,3,-2080159247468648985,-1306911382131817506,1219720796746600015,-978348523441835274,'2024-02-18','2025-06-18','2025-06-18','2025-02-18',null,'i','y','s','c',null,'m','o'),(10,2,1,-10,0,-7569151440408756108,null,1393963672572074196,-3822268135049166532,'2024-01-08','2027-01-16','2023-12-13','2024-02-18','s',null,'q','z',null,'u','b','v'),(11,9,8,-10,7,-4419333711868488787,5670069993430301249,-5101280938951897282,7291919080934920934,'2027-01-09','2025-02-18','2024-01-17','2026-01-18','u','c','d','r',null,'m','r','p'),(12,9,8,7,9,-697217673128595873,-2415793798160514654,-1909943068043737865,5844073448689265407,'2024-01-17','2025-02-17','2023-12-17','2023-12-14','a','z','v','o','q','u','z','h'),(13,-10,6,-10,1,null,null,-6933219731543784592,-4745778751501231119,'2023-12-20',null,'2026-01-18','2026-01-18',null,'r','w','c','k',null,'t','e'),(14,6,4,-10,-10,null,377907223978572611,-7447442174599113505,4949011330273779695,'2023-12-17','2024-02-18','2026-01-18','2024-01-08','g','w','u','k',null,'m','g','d'),(15,null,-4,-10,3,-5441857898369120483,-2001300041828347883,4385022502994073333,6762545521805735020,'2024-01-17','2023-12-11','2023-12-15','2023-12-10','p','u','p','i','i','c','j','v'),(16,8,1,-10,3,7499177078109776887,8002215544264694167,-4914597203639379766,7611185654075676834,null,null,'2024-01-08','2023-12-17','e','h','q','t',null,null,'p','d'),(17,-4,-10,3,6,null,null,4596273190394276006,-3248366019937329149,'2024-01-09','2023-12-19','2023-12-20','2023-12-11',null,null,'i','f',null,'t','q','a'),(18,0,-4,-10,1,null,-2000849949571150330,7208571222986345744,2598345170237057509,'2024-01-09','2023-12-09','2024-02-18','2024-01-19','a','f','w','o','b','y','q','f'),(19,7,7,-4,0,null,5717592572856392823,-8128856226419623044,-7534868883394863810,'2023-12-20','2023-12-09','2023-12-10','2023-12-12','r','t','y','d','l','a','y','v'),(20,2,null,0,2,null,-6905288165492491017,1934258578152616096,-1388806210542225140,null,null,'2024-01-19','2026-01-18','p',null,'p','g','u','b','i','c'),(21,null,null,-10,-4,1698759627767041241,null,-6613269394014189122,1915677852069340594,'2023-12-18','2024-01-31','2025-02-18','2024-01-08','e','g','l','h',null,null,'v','n'),(22,-10,-10,-4,5,-3720952595350369266,1539673860923570193,5089313038468606351,262016952853919148,'2023-12-15','2025-02-18','2024-01-19','2024-01-08','d','g','d','e','l',null,'m','g'),(23,-10,1,-10,7,null,-4884323809040291936,-4428424779275301738,-3325468851678420401,'2023-12-19','2027-01-16','2026-02-18','2024-01-09','z','v','v','v','r','d','j','y'),(24,4,8,3,-10,1026316126533561197,-8966784351064986909,496857885215447340,-6148636280121789215,'2024-02-18','2025-06-18','2026-02-18','2025-02-18','f',null,'j','k',null,'s','i','q'),(25,5,5,4,6,8574091287090543865,null,-773937635554104337,6026917236758217609,'2026-02-18','2027-01-09','2023-12-12','2024-01-19','s','p','t','d','t','l','u','m'),(26,0,-10,-10,9,-2429694321063869458,null,8908961259233183763,6894623222255264210,'2024-01-17','2023-12-14','2023-12-11','2023-12-09','w','o','l','g','m','r','h','i'),(27,5,0,2,-10,7748161344545453064,null,3244053576839674045,-7948008233666340932,'2024-02-18','2023-12-20','2024-01-08','2023-12-14','n','a','r','q','c','y','q','u'),(28,4,-4,5,4,null,2204997326584988589,7997961660331289189,8763906081360257030,'2025-06-18','2023-12-18','2023-12-16','2023-12-16','k','i','d','t','y','c','o','a'),(29,0,5,-4,4,null,null,7562098503580472041,929168144988769048,'2026-01-18','2023-12-11','2023-12-10','2024-01-31','p','d','j','j','j','h','f','p'),(30,5,-10,-4,4,3945007524910600918,null,-8466778503120120353,-9169615505661358433,'2023-12-13','2024-01-19','2023-12-16','2023-12-10',null,'p','g','d','e','e','r','u'),(31,5,4,8,6,-7544567449016691208,-7026985677386241453,-2698203866546802012,-8383194363413620107,'2024-01-09','2027-01-09','2025-02-18','2025-02-18','f','t','g','n','r','i','p','o'),(32,-4,null,7,-10,-5468978947503379654,-5676001133436456624,-5328902013300281884,2338117992866157501,'2023-12-20','2023-12-15','2023-12-20','2024-01-08','f','z','y','t','j','c','e','x'),(33,-10,6,-10,-10,6715916167220457165,-3864032264700366706,7115861918737510196,-937991761308321600,'2025-02-18','2024-02-18','2023-12-19','2024-02-18','x',null,'t','x','h','o','p','v'),(34,-4,8,9,-10,-4718823602932239136,-3633212199616285968,-5190227402771860745,5545611345261203982,'2024-01-08','2026-01-18','2024-02-18','2023-12-16','f','t','n','h',null,'y','e','u'),(35,-4,3,-10,8,null,7722389449895645140,-4965839022248115530,6494405496923526511,'2023-12-10','2024-02-18','2026-02-18','2024-01-09','u','t','a','t','w',null,'h','w'),(36,5,6,-10,0,null,null,84960662585385706,2611830596442233539,null,'2026-01-18','2023-12-15','2026-01-18','b','t','p','b','g','g','z','k'),(37,-10,-10,3,-10,null,-5462312886211329186,-2793882411087244594,7564921654378090880,'2025-06-18','2027-01-09','2027-01-16','2023-12-09','n','k','l','z','y','i','o','c'),(38,4,null,-10,3,null,2065313248357950073,2398686393322288278,-5793325226082177083,'2023-12-14','2024-01-17','2023-12-12','2024-01-31','m',null,'n','c','g','f','r','m'),(39,5,1,9,0,-2901110266746457515,-7419676417711330947,5568223068212783910,-8443206843568552423,'2023-12-20','2023-12-15','2024-02-18','2024-01-17','j',null,'x','m','c','u','j','a'),(40,-4,5,-4,-4,3686987810771014559,4528672918224579415,-531153650185309112,-4795413900154192584,'2023-12-12','2024-01-19','2024-01-31','2024-01-19','m','o','k','p','v','s','f','c'),(41,null,6,-10,-10,1371451390800312645,-945321182848207621,-8418988114521301883,-8987180461079691062,'2024-01-09','2023-12-10','2023-12-19','2023-12-12','s','i','x','u','h','e','q','y'),(42,-10,null,2,1,null,-2863490765313461654,3110048825870954129,-2547950560699735251,'2025-06-18','2024-01-08','2023-12-10','2023-12-10','d','y','d','h','t','o','t','w'),(43,3,0,4,1,-7282895869404970499,5532011705197458854,-4502369753730677912,-3032934141238479600,'2023-12-18','2024-02-18','2023-12-19','2026-02-18','w',null,'m','n','g',null,'j','q'),(44,null,null,2,9,5430716729257430348,null,-8208477558683957238,-7953995265596299120,'2023-12-18','2023-12-18','2023-12-11','2025-06-18','w','w','a','u','k','k','j','q'),(45,6,-4,-10,1,-8903356732633014005,null,2532821444113211740,-2346292639048659545,'2024-01-08','2023-12-12','2025-02-18','2023-12-19','v','b','k','e','i','q','h','l'),(46,7,-10,-10,-10,null,-6646527298990960109,-7898216427196445987,-1558528416630681469,null,'2027-01-09','2024-02-18','2025-02-17','k','d','o','n','h','g','x','p'),(47,3,6,3,7,-6864291355591117572,4024432796468900765,-6272917113481022986,-1984131539617763529,'2024-01-17','2024-01-17','2025-06-18','2025-06-18','c','p','t','y','i','c','y','i'),(48,9,null,1,-4,null,-7244007199905240117,8657019614874868097,-492287318340969091,'2024-02-18','2023-12-11','2024-01-09','2027-01-09',null,null,'m','i','t','k','r','x'),(49,1,null,1,5,8407263822602373073,-3275760834800206047,-2117832965174816037,5807219087033669504,'2023-12-13','2024-01-19','2023-12-16','2024-02-18','r','n','n','o','r','g','j','q');
    """
    sql """ set enable_common_expr_pushdown = true; """
    qt_select_bug_1 """
    SELECT col_int_undef_signed_not_null, col_date_undef_signed_not_null
     FROM ${table1} AS table1
     WHERE (
             NOT (
                 (
                    table1.`col_int_undef_signed_not_null` is NULL
                 )
                 OR table1.col_varchar_1024__undef_signed_index_inverted IN ('h', 'j')
             )
            OR table1.`col_date_undef_signed_not_null` IN ('2024-02-18')
        )
     ORDER BY col_int_undef_signed_not_null, col_date_undef_signed_not_null;    """

}
