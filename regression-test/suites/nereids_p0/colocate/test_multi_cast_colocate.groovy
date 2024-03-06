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

// this suite is for creating table with timestamp datatype in defferent
// case. For example: 'year' and 'Year' datatype should also be valid in definition

suite("test_multi_cast_colocate") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'

    sql """
        drop table if exists test_multi_cast_colocate
    """

    sql """
        create table test_multi_cast_colocate (
            `c1` int,
            `c2` varchar(10)   ,
            `pk` int
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into test_multi_cast_colocate(pk,c1,c2) values (0,null,'m'),(1,1,"me"),(2,null,'p'),(3,null,"and"),(4,7,'v'),(5,3,"what"),(6,5,'j'),(7,null,"he's"),(8,null,'q'),(9,8,"like"),(10,null,"me"),(11,null,"from"),(12,4,'r'),(13,null,"now"),(14,null,'q'),(15,2,"me"),(16,6,'y'),(17,4,"on"),(18,null,'d'),(19,7,"really"),(20,2,"for"),(21,null,'a'),(22,9,'j'),(23,null,"tell"),(24,6,'f'),(25,7,'n'),(26,3,'g'),(27,null,"on"),(28,0,'l'),(29,2,'n'),(30,null,"her"),(31,null,"got"),(32,8,'i'),(33,7,'g'),(34,null,"from"),(35,null,'j'),(36,null,"did"),(37,null,'v'),(38,9,"could"),(39,null,"if"),(40,null,"no"),(41,1,"me"),(42,3,"good"),(43,8,"i"),(44,9,'e'),(45,null,"here"),(46,3,'t'),(47,null,"some"),(48,5,"he"),(49,6,"there"),(50,3,'v'),(51,null,"will"),(52,6,'y'),(53,null,"were"),(54,8,'h'),(55,null,"mean"),(56,7,'k'),(57,null,"they"),(58,1,'j'),(59,3,'f'),(60,null,"been"),(61,null,'u'),(62,null,"out"),(63,null,'w'),(64,4,"back"),(65,null,'c'),(66,0,"well"),(67,null,"didn't"),(68,3,'w'),(69,0,'f'),(70,null,"get"),(71,null,"is"),(72,null,"her"),(73,2,'m'),(74,null,'n'),(75,null,"we"),(76,8,"say"),(77,null,"no"),(78,null,'m'),(79,null,'r'),(80,2,"to"),(81,null,'u'),(82,0,"in"),(83,null,"yeah"),(84,8,"in"),(85,4,"here"),(86,null,"had"),(87,3,'b'),(88,3,"all"),(89,null,"that"),(90,null,"this"),(91,7,"know"),(92,4,'c'),(93,null,"be"),(94,null,"his"),(95,4,"they"),(96,null,'y'),(97,null,'j'),(98,9,'r'),(99,5,"who");
    """

    sql """
        sync
    """

    qt_test """
        WITH cte1 AS (
            SELECT t1.`pk`
            FROM test_multi_cast_colocate AS t1 LEFT SEMI
                JOIN test_multi_cast_colocate AS alias1 ON t1.`pk` = alias1.`pk`
        )
        SELECT cte1.`pk` AS pk1
        FROM cte1
            FULL OUTER JOIN cte1 AS alias2 ON cte1.`pk` = alias2.`pk`
        ORDER BY pk1
    """
}
