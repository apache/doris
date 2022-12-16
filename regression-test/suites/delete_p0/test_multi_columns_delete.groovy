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

suite("test_multi_columns_delete") {
    def tableName = "test_multi_columns_delete"

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} ( id INT NULL COMMENT "",name VARCHAR(500) NULL COMMENT "",gender VARCHAR(500) NULL COMMENT "",remark VARCHAR(500) NULL COMMENT "" ) ENGINE=OLAP DUPLICATE KEY(id, name) DISTRIBUTED BY HASH(id, name) BUCKETS 4 PROPERTIES ( "replication_allocation" = "tag.location.default: 1","in_memory" = "false","storage_format" = "V2");"""

    sql """ INSERT INTO ${tableName} VALUES (1125,'mus','f','xDavis'),(1338,'mos','f','kDiaz'),(1443,'mus','f','enim_unde_quae'),(1564,'mos','f','quasi'),(1745,'mus','m','sit_et_quidem'),(1898,'mos','m','est_sit'),(2256,'mus','f','SusanHolmes'),(2316,'mus','f','jSpencer'),(2448,'mus','f','impedit_exercitationem_nobis'),(2493,'mus','f','aut_id'),(2768,'mos','f','dolor_neque'),(2813,'mos','f','SeanHill'),(2834,'mus','f','xMorrison'),(2894,'mos','m','incidunt'),(2933,'mos','m','CharlesFisher'),(2978,'mus','m','fTaylor'),(3017,'mos','m','RaymondCruz'),(3059,'mos','m','autem'),(3366,'mus','m','doloremque_vel'),(3433,'mus','m','odio_voluptatem'),(3878,'mos','m','ut_animi_necessitatibus'),(3986,'mus','m','ut'),(4510,'mos','m','NicoleBarnes'),(4551,'mos','f','libero_maxime'),(1225,'mus','m','ArthurBarnes'),(1412,'mus','f','JessicaWoods'),(1413,'mus','m','NancyCarroll'),(1496,'mos','m','RalphHicks'),(1515,'mus','m','et'),(1706,'mos','f','JesseBryant'),(1736,'mos','f','PhilipChavez'),(1951,'mos','f','aWilson'),(2290,'mos','m','JamesHernandez'),(2398,'mus','m','provident_consequuntur'),(2618,'mus','f','quas_expedita_nulla'),(2726,'mus','m','PatrickJackson'),(2778,'mos','f','oAndrews'),(2911,'mos','f','dolore'),(2962,'mus','f','4Stevens'),(3280,'mus','m','quaerat'),(3383,'mus','m','qui_molestiae_consequatur'),(3438,'mus','f','odio_eos'),(3724,'mus','f','voluptatem_non_eum'),(3979,'mos','f','ToddRomero'),(4016,'mos','f','gBlack'),(4029,'mos','m','uWright'),(4138,'mos','f','MariaPatterson'),(4214,'mus','f','ipsa'),(4265,'mus','f','EvelynWebb'),(4379,'mus','f','hWheeler'),(4436,'mus','m','aNelson'),(1315,'mos','m','doloribus'),(1995,'mus','m','rerum'),(2093,'mus','m','PeterFuller'),(2151,'mos','f','qGarrett'),(2271,'mus','f','cEvans'),(2422,'mus','f','MargaretHunt'),(2691,'mus','m','0Alexander'),(2917,'mus','m','id'),(2920,'mus','f','eos_voluptas_minima'),(3037,'mos','f','gFowler'),(3074,'mos','m','1Stanley'),(3159,'mos','f','aut'),(3340,'mos','f','KarenFuller'),(3564,'mus','f','cum_maiores'),(3623,'mos','f','CynthiaGarrett'),(3895,'mus','m','bWatkins'),(3992,'mos','f','hJames'),(4055,'mos','f','RogerThomas'),(4291,'mus','f','FredJohnson'),(4326,'mos','f','AndrewRodriguez'),(1302,'mus','f','sed_qui_aut'),(1312,'mus','f','qBoyd'),(1418,'mus','m','KeithRogers'),(1553,'mus','f','molestiae'),(1596,'mus','f','KennethJohnston'),(1618,'mus','m','veniam_rem'),(1822,'mus','f','5Fernandez'),(1916,'mus','f','uDiaz'),(2194,'mos','m','tHayes'),(2334,'mus','f','GregorySchmidt'),(2363,'mos','f','itaque_minima_placeat'),(2370,'mos','m','et_velit_in'),(2589,'mos','f','CherylBailey'),(2606,'mus','f','SaraBerry'),(2650,'mus','f','VirginiaVasquez'),(2883,'mus','m','quia_quae_repudiandae'),(2904,'mus','f','TerryRivera'),(3338,'mos','m','VictorOlson'),(3393,'mus','m','itaque_eum'),(3602,'mus','f','wWells'),(3635,'mus','m','mPorter'),(3697,'mus','m','CharlesRobinson'),(3936,'mos','f','facere_corrupti_velit'),(4158,'mos','f','in_nostrum'),(4212,'mus','m','JonathanStanley'),(4231,'mus','f','lSchmidt'),(4259,'mos','f','voluptas_culpa'),(4492,'mos','f','excepturi_eum'),(4555,'mus','f','9Powell'); """
    sql """ DELETE FROM ${tableName} WHERE name = 'mos' AND gender = 'm'; """

    qt_sql """SELECT * FROM ${tableName} WHERE name = 'mus' AND id = 1302;"""
}
