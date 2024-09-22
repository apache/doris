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


suite("test_need_read_data", "p0"){
    def indexTbName1 = "test_need_read_data"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      create table ${indexTbName1} (
      col_int_undef_signed_not_null int  not null  ,
      col_bigint_undef_signed_not_null_index_inverted bigint  not null  ,
      col_bigint_undef_signed_not_null bigint  not null  ,
      col_int_undef_signed int  null  ,
      col_int_undef_signed_index_inverted int  null  ,
      col_int_undef_signed_not_null_index_inverted int  not null  ,
      col_bigint_undef_signed bigint  null  ,
      col_bigint_undef_signed_index_inverted bigint  null  ,
      col_date_undef_signed date  null  ,
      col_date_undef_signed_index_inverted date  null  ,
      col_date_undef_signed_not_null date  not null  ,
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
      UNIQUE KEY(col_int_undef_signed_not_null, col_bigint_undef_signed_not_null_index_inverted, col_bigint_undef_signed_not_null)
      PARTITION BY             RANGE(col_int_undef_signed_not_null) (
                      PARTITION p0 VALUES LESS THAN ('4'),
                      PARTITION p1 VALUES LESS THAN ('6'),
                      PARTITION p2 VALUES LESS THAN ('7'),
                      PARTITION p3 VALUES LESS THAN ('8'),
                      PARTITION p4 VALUES LESS THAN ('10'),
                      PARTITION p5 VALUES LESS THAN ('1147483647'),
                      PARTITION p100 VALUES LESS THAN ('2147483647')
                  )
      distributed by hash(col_bigint_undef_signed_not_null)
      properties("enable_unique_key_merge_on_write" = "true", "replication_num" = "1");
    """

    try {
        sql """ insert into ${indexTbName1}(pk,col_int_undef_signed,col_int_undef_signed_index_inverted,col_int_undef_signed_not_null,col_int_undef_signed_not_null_index_inverted,col_bigint_undef_signed,col_bigint_undef_signed_index_inverted,col_bigint_undef_signed_not_null,col_bigint_undef_signed_not_null_index_inverted,col_date_undef_signed,col_date_undef_signed_index_inverted,col_date_undef_signed_not_null,col_date_undef_signed_not_null_index_inverted,col_varchar_10__undef_signed,col_varchar_10__undef_signed_index_inverted,col_varchar_10__undef_signed_not_null,col_varchar_10__undef_signed_not_null_index_inverted,col_varchar_1024__undef_signed,col_varchar_1024__undef_signed_index_inverted,col_varchar_1024__undef_signed_not_null,col_varchar_1024__undef_signed_not_null_index_inverted) values (0,null,4,4,-10,-2806067520539703493,-6601893530499828925,-7112891289289392099,-4883682982770875774,'2024-02-18','2025-02-18','2026-02-18','2025-06-18','s',null,'p','h','m','v','p','y'),(1,null,null,-4,5,null,5288406618687277699,2255222069393083918,-7885696364844049115,null,'2023-12-12','2025-02-18','2025-02-17','r','u','f','q','n','x','q','b'),(2,1,null,3,-10,4359919727878435225,3857327739249958938,-1608704987438726460,3650073478443923028,'2025-06-18','2024-02-18','2023-12-11','2023-12-13','s','p','x','u','e','k','l','c'),(3,4,-4,-4,9,null,-2952515504097288835,-5248236183608833050,-959402810989602748,null,'2024-01-09','2023-12-12','2023-12-11','i','f','r','m','y','j','z','h'),(4,3,-10,-10,-10,7595845588678689951,1807745217759831665,-3269384278721380385,-2626336667443528939,'2024-01-31','2023-12-13','2024-02-18','2023-12-15','c','b','q','a','k',null,'c','q'),(5,2,3,-10,1,4845076071359725577,-2081520813928284051,7965382457542866166,-1263489703824567045,'2023-12-20','2025-02-18','2025-02-17','2023-12-12',null,'p','v','k','s',null,'c','y'),(6,-4,7,-4,1,null,-312662439708631344,5986577765852616616,4340850184105875353,'2023-12-16','2024-01-17','2024-01-19','2026-02-18','s','q','w','z','m','a','v','z'),(7,5,-10,5,4,2838843073924070168,1039278568099501661,-2970064881715473209,-3546456947974778751,'2023-12-15','2024-01-31','2023-12-18','2024-01-08','m','t','t','y','y',null,'l','l'),(8,null,1,7,4,8007492376294713910,994564705132157298,-4772736062894230873,7998340640624222727,'2023-12-13','2027-01-09','2024-02-18','2024-01-19','t',null,'p','n','j','l','g','c'),(9,8,5,2,0,-3365354435906005141,6104736481079857939,-9027968242311553543,8197347709168535456,'2023-12-20','2024-02-18','2023-12-10','2024-02-18','t',null,'p','h','b','e','w','a'),(10,4,5,9,3,null,-4270500778304447716,7975155833293678874,-5070901586921449703,'2024-01-19','2024-01-19','2027-01-09','2024-02-18','d','b','o','p','b','r','f','q'),(11,5,-4,3,-4,9029358127154882937,-6951089794937682659,2630099804275038168,-5951780849025883849,null,null,'2024-01-17','2023-12-18','k','b','s','t','z','w','r','y'),(12,3,-4,1,-4,309499821905665998,null,9039699521215326765,1922704068231658563,'2023-12-17','2024-02-18','2025-06-18','2027-01-09','m','z','l','l','c',null,'h','a'),(13,4,null,7,1,-8088552747885790045,null,4290463296755479391,-143636852574500943,'2023-12-15','2024-02-18','2023-12-11','2023-12-16',null,'o','j','r',null,'l','p','z'),(14,null,3,8,-4,-2114077344204837001,null,-2621506620475509311,3516417267200208461,'2023-12-20','2024-01-09','2023-12-18','2024-01-31','q','d','u','y','k','o','l','o'),(15,5,0,3,1,null,469795947256142816,-3769981987932056248,-5475689888751280017,'2025-06-18',null,'2024-02-18','2025-06-18','y','c','r','o','y','p','o','x'),(16,-4,5,6,2,-5508267849003968462,3862598879377665395,8797778903170766625,383525525986717298,'2024-02-18','2023-12-11','2024-01-17','2023-12-17','s','c','m','c',null,null,'s','o'),(17,-4,6,6,0,-1057012716454277307,-8435107454217223314,-120847043370486063,-7398709528414815558,'2027-01-16','2024-02-18','2025-02-17','2024-01-17',null,'f','u','d','l','a','h','r'),(18,2,6,1,-10,null,339922889780584305,8187645881248196657,9163411866066071700,'2023-12-14','2024-01-08','2024-01-31','2023-12-13','u',null,'y','a','v','v','u','z'),(19,8,-4,9,-10,null,null,-5043486900779665508,6743373354123723422,'2023-12-19','2023-12-14','2024-01-17','2023-12-14','u','t','z','o','b','n','y','t'),(20,-4,7,-4,0,-14388658393529797,null,4788467486005193064,545093836722695629,'2025-06-18','2024-02-18','2027-01-09','2023-12-09','u','b','u','r','e',null,'f','o'),(21,-4,-4,4,8,2245289792981551579,-4485687312935149586,-6812240256635951381,8434108874554436761,'2023-12-09','2023-12-16','2026-02-18','2023-12-09',null,'k','v','q','r',null,'z','e'),(22,-4,3,3,1,-1218928283971104430,null,4325982422669856960,-3538203895099667724,'2027-01-16',null,'2024-02-18','2023-12-19',null,null,'v','e','u','n','q','h'),(23,-4,0,7,7,null,null,-8073893843395705054,-5272967641252019267,'2024-01-17','2027-01-09','2024-01-19','2023-12-14','n','q','o','t','e',null,'w','u'),(24,-10,3,-10,5,null,-8586767268780407575,3995596016735621711,429810224315638782,'2025-06-18','2026-02-18','2023-12-18','2023-12-14','m','p','k','e',null,'l','m','j'),(25,9,null,8,9,-7406209357285295571,8508527424784069010,-6700207760307684345,-6778419266053660893,'2024-01-31','2023-12-15','2025-06-18','2023-12-20',null,null,'t','f','b','k','e','b'),(26,-4,7,-10,1,549615970930886740,-8537815265488791461,9105177114666776852,808842453982447397,'2023-12-14','2026-02-18','2025-06-18','2023-12-18','z','g','v','b','m','s','b','r'),(27,6,8,-4,0,2363364623910847992,2435555377853278383,-7059658388492780701,1808647950247830538,'2023-12-14','2023-12-14','2027-01-09','2025-06-18',null,null,'o','q','i','g','i','u'),(28,-4,-4,0,-10,2837109330712127287,3258333757532106070,-5486400602525569363,7454355268455905775,'2023-12-18','2024-01-09','2025-06-18','2024-01-17',null,'e','h','j',null,'u','a','y'),(29,7,-4,5,-4,-3944687198069417525,7676519542662933720,6921050061010821518,8686366976901132291,'2026-01-18','2024-01-17','2024-02-18','2024-01-08',null,'a','d','y','i','l','r','h'),(30,-4,null,4,7,null,3744030733185485721,-6094790559118439736,-5022434353993604500,'2023-12-09','2023-12-20','2025-06-18','2023-12-14','u','a','o','m','c','n','o','z'),(31,1,4,2,9,-6750407042384384034,-2505324657561681875,5301019286095013646,3139887746721886789,null,'2023-12-15','2024-01-08','2023-12-17','q','q','u','x','b','m','z','p'),(32,-4,null,9,9,null,-1729390457612771468,6708980039726745192,-7293963935049205901,'2024-02-18','2023-12-17','2023-12-13','2023-12-11','d',null,'v','m','e','f','w','y'),(33,4,5,-10,9,3535708249298387360,-8975616986595863107,-862904611806478134,-3482957162935231780,'2024-02-18','2023-12-20','2023-12-19','2025-02-18','b','h','c','v','s','b','e','e'),(34,-4,null,-4,0,null,-2891436930426307984,-1704292540467538099,-8564095007543756456,'2023-12-13','2027-01-09','2025-02-18','2026-02-18',null,null,'f','t','w','s','z','h'),(35,-10,5,8,8,-5214808208417488395,null,-7973404656651152899,-7504002864628497355,'2023-12-19','2023-12-10','2025-02-18','2023-12-12','v','x','f','t','q','x','t','n'),(36,5,5,6,-10,null,-5034216029789478132,7055943879035021136,3694646370699507306,'2027-01-09','2024-01-19','2025-06-18','2025-06-18',null,'s','t','d',null,'w','r','i'),(37,-4,-4,4,8,-8353588562907627230,-4962760661862938335,-6383251065296502747,-3956983090211228295,'2023-12-11','2024-01-08','2023-12-16','2024-02-18','u','j','w','g','w','r','c','f'),(38,-10,-10,-4,-4,null,null,247919645069562206,8288875655774835875,'2025-02-17','2026-01-18','2023-12-11','2026-02-18','u','m','y','a','k',null,'d','g'),(39,2,9,4,0,-202685904718522032,5896989546064098495,5695022708672839468,-7757738794882960533,'2027-01-09','2023-12-15','2027-01-16','2023-12-16','c','u','l','z',null,'d','x','h'),(40,null,null,7,7,4835497339758608794,-5034982165921306189,548692450913004773,-5189451515582531492,'2024-01-19','2024-01-19','2024-02-18','2023-12-11','w',null,'m','b','w','u','t','u'),(41,5,0,6,3,-4294233837078291532,null,-4951306529377186523,-6918657392711546234,'2023-12-15','2023-12-15','2023-12-09','2024-01-09',null,'p','c','b','a','i','j','n'),(42,null,-4,-10,9,-1890221339559069903,-9174324348086863583,-7768565663714277414,1583589820059033801,'2024-01-19','2024-01-31','2026-02-18','2024-02-18','i','x','z','z',null,'s','n','h'),(43,null,8,4,6,null,-3040322546432413315,-7308614762246284846,-7065674754425766028,'2023-12-20','2024-02-18','2025-02-17','2024-02-18','z','z','l','l','a','j','p','f'),(44,8,-10,8,7,3533075847821605226,-8953779906693032030,-4566442548311033242,-6944000050952871345,'2025-02-18','2023-12-19','2023-12-16','2024-01-31','i','h','n','u','j','y','r','g'),(45,8,-4,6,-4,null,null,-6687031592408843506,-2644229079223766810,'2023-12-12','2025-02-18','2023-12-12','2024-01-09',null,'y','j','b','p','l','c','c'),(46,4,-4,-10,-4,4827922355005130144,2170052469942214303,-1634163889160521502,2246460621190812016,'2023-12-15','2023-12-11','2024-01-08','2025-02-18',null,null,'t','x','n','i','e','l'),(47,null,4,8,7,null,-7852983538684806346,-8869292859054504183,-7739200759355598281,null,'2023-12-17','2025-06-18','2024-01-17','i','r','m','s',null,'m','j','r'),(48,-10,7,9,0,-4878175313227689694,6741975426332123319,-384904760880798675,3671673134248536674,null,'2023-12-15','2023-12-09','2025-02-18','w','y','w','b','z','s','l','g'),(49,3,-4,8,-10,1403956186045671649,null,6070888386247696636,6864331793839321007,null,'2024-01-08','2027-01-16','2026-01-18','p','z','b','i','t','u','q','f'); """

        sql "sync"

        qt_sql """ SELECT COUNT( *) AS field1, col_int_undef_signed AS field2 FROM ${indexTbName1} WHERE( col_date_undef_signed_not_null_index_inverted == '2024-01-01' OR day( col_date_undef_signed_not_null_index_inverted ) !=0 ) GROUP BY field2 ORDER BY field2; """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    def indexTbName2 = "test_need_read_data_2"

    sql "DROP TABLE IF EXISTS ${indexTbName2}"

    sql """
      create table ${indexTbName2} (
          a datetime not null,
          b varchar not null,
          INDEX idx_inverted_b (`b`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        COMMENT ''
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into ${indexTbName2} values
            ('2024-06-03 15:16:49.266678','shanghai'),
            ('2024-06-02 15:16:49.266678','shenzhen'),
            ('2024-06-01 15:16:49.266678','beijing'),
            ('2024-06-13 15:16:49.266678','beijing'),
            ('2024-06-14 15:16:49.266678','beijing'),
            ('2024-06-15 15:16:49.266678','shanghai'),
            ('2024-06-16 15:16:49.266678','tengxun'),
            ('2024-06-17 15:16:49.266678','tengxun2')
    """

    qt_sql1 """ select  COUNT(1)  from  ${indexTbName2}  WHERE  a  >=  '2024-06-15  00:00:00'  AND  b  =  'tengxun2'  and    `b`  match  'tengxun2'  ; """
    qt_sql2 """ select  *  from  ${indexTbName2}  WHERE  a  >=  '2024-06-15  00:00:00'  AND  b  =  'tengxun2'  and    `b`  match  'tengxun2'  ; """
    qt_sql3 """ select  COUNT(1)  from  ${indexTbName2}  WHERE  a  >=  '2024-06-15  00:00:00'  AND  b  like  '%tengxun%'  and    `b`  match  'tengxun2'  ; """
    qt_sql4 """ select  *  from  ${indexTbName2}  WHERE  a  >=  '2024-06-15  00:00:00'  AND  b  like  '%tengxun%'  and    `b`  match  'tengxun2'  ; """
}
