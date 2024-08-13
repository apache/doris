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

suite("distribution_expr") {
    multi_sql """
        drop table if exists table_100_undef_partitions2_keys3_properties4_distributed_by52;
        drop table if exists table_6_undef_partitions2_keys3_properties4_distributed_by53;
        drop table if exists table_7_undef_partitions2_keys3_properties4_distributed_by5;
	drop table if exists table_8_undef_partitions2_keys3_properties4_distributed_by5;

        create table table_100_undef_partitions2_keys3_properties4_distributed_by52 (
	`pk` int,
	`col_int_undef_signed` int   ,
	`col_varchar_10__undef_signed` varchar(10)   ,
	`col_varchar_1024__undef_signed` varchar(1024) MAX   
	) engine=olap
	AGGREGATE KEY(pk, col_int_undef_signed, col_varchar_10__undef_signed)
	distributed by hash(pk) buckets 10
	properties("replication_num" = "1");
	insert into table_100_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_varchar_10__undef_signed,col_varchar_1024__undef_signed) values (0,null,'k',"mean"),(1,null,"I'll","who"),(2,null,"i",'v'),(3,null,"really",'w'),(4,null,"when",'e'),(5,9,"what","who"),(6,5,"been","like"),(7,null,"and","ok"),(8,3,'t',"then"),(9,null,'y',"up"),(10,8,'b',"think"),(11,0,"if",'l'),(12,null,"there",'q'),(13,null,"out",'q'),(14,3,'c','h'),(15,2,"out","yes"),(16,null,'t',"think"),(17,2,'a',"did"),(18,null,'j','a'),(19,7,'p',"with"),(20,null,'x','v'),(21,null,"I'll",'f'),(22,9,'i',"this"),(23,4,'l',"or"),(24,3,"oh","have"),(25,null,"go",'g'),(26,null,"did","been"),(27,null,'x',"what"),(28,2,"see",'b'),(29,null,'c','d'),(30,null,'b',"some"),(31,null,"didn't","out"),(32,6,"that's","did"),(33,7,"will","got"),(34,null,'w','h'),(35,null,"the",'d'),(36,4,'k',"good"),(37,null,'u','m'),(38,null,"for",'c'),(39,8,"good","on"),(40,1,'d',"will"),(41,null,"ok",'t'),(42,null,"see",'a'),(43,null,"mean","something"),(44,4,"did","be"),(45,3,'k',"been"),(46,4,'t',"yes"),(47,null,"but","think"),(48,null,'b',"some"),(49,null,'o',"like"),(50,null,"on","there"),(51,0,'q','u'),(52,0,"a",'s'),(53,6,'d',"yes"),(54,7,"that's",'e'),(55,3,"been",'f'),(56,null,"tell",'y'),(57,null,'m','v'),(58,null,"i","get"),(59,2,"why",'v'),(60,3,'g',"for"),(61,null,'x',"if"),(62,null,"can","did"),(63,null,"i",'t'),(64,3,"who","I'll"),(65,1,'x',"if"),(66,9,"he","a"),(67,0,"get",'h'),(68,0,"don't","some"),(69,null,'r',"with"),(70,3,'i','j'),(71,null,"can't",'v'),(72,2,"ok",'j'),(73,null,'e',"what"),(74,null,'w',"in"),(75,8,"well","mean"),(76,null,'z','s'),(77,9,'d','z'),(78,9,"oh","you"),(79,null,'k','c'),(80,2,"know","I'll"),(81,null,"say","had"),(82,null,'x',"about"),(83,9,"a","me"),(84,1,"be",'a'),(85,7,"the",'t'),(86,null,'t',"been"),(87,null,"not","are"),(88,null,"how",'m'),(89,2,'w',"will"),(90,null,"what","i"),(91,1,"will","we"),(92,null,'l','o'),(93,null,"all",'o'),(94,null,'i',"me"),(95,null,'e','l'),(96,6,'q',"you're"),(97,9,"your",'g'),(98,null,"okay",'o'),(99,7,"my",'v');


	create table table_6_undef_partitions2_keys3_properties4_distributed_by53 (
	`pk` int,
	`col_varchar_10__undef_signed` varchar(10)   ,
	`col_int_undef_signed` int   ,
	`col_varchar_1024__undef_signed` varchar(1024)   
	) engine=olap
	DUPLICATE KEY(pk, col_varchar_10__undef_signed)
	distributed by hash(pk) buckets 10
	properties("replication_num" = "1");
	insert into table_6_undef_partitions2_keys3_properties4_distributed_by53(pk,col_int_undef_signed,col_varchar_10__undef_signed,col_varchar_1024__undef_signed) values (0,0,"think","she"),(1,null,"was",'r'),(2,8,'g',"i"),(3,9,'s',"he's"),(4,4,"they",'n'),(5,null,"time","really");
	
	create table table_7_undef_partitions2_keys3_properties4_distributed_by5 (
	`col_int_undef_signed` int/*agg_type_placeholder*/   ,
	`col_varchar_10__undef_signed` varchar(10)/*agg_type_placeholder*/   ,
	`col_varchar_1024__undef_signed` varchar(1024)/*agg_type_placeholder*/   ,
	`pk` int/*agg_type_placeholder*/
	) engine=olap
	distributed by hash(pk) buckets 10
	properties("replication_num" = "1");
	insert into table_7_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_varchar_10__undef_signed,col_varchar_1024__undef_signed) values (0,7,'y','g'),(1,null,'t',"can"),(2,8,"would",'l'),(3,null,"will","he"),(4,null,'k',"I'll"),(5,null,'m',"ok"),(6,null,'s',"that");
	
	create table table_8_undef_partitions2_keys3_properties4_distributed_by5 (
	`col_int_undef_signed` int/*agg_type_placeholder*/   ,
	`col_varchar_10__undef_signed` varchar(10)/*agg_type_placeholder*/   ,
	`col_varchar_1024__undef_signed` varchar(1024)/*agg_type_placeholder*/   ,
	`pk` int/*agg_type_placeholder*/
	) engine=olap
	distributed by hash(pk) buckets 10
	properties("replication_num" = "1");
	insert into table_8_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_varchar_10__undef_signed,col_varchar_1024__undef_signed) values (0,4,'d',"she"),(1,3,"okay",'e'),(2,null,'s',"as"),(3,null,"you","up"),(4,3,'f','q'),(5,null,'f','s'),(6,null,'h',"time"),(7,8,'o',"i");

        set enable_nereids_distribute_planner=true;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        set enable_local_shuffle=true;
        set force_to_local_shuffle=true;
        """

	explain {
                sql """
                SELECT *
                FROM
                       (SELECT alias3.`pk` from table_100_undef_partitions2_keys3_properties4_distributed_by52 AS alias4 INNER JOIN
                       table_6_undef_partitions2_keys3_properties4_distributed_by53 AS alias3
                       ON alias3.`pk` = alias4.`pk`
                       WHERE  (alias3.`pk` < alias4.`pk` OR alias3.`pk` <= 4 )
                       ) tmp2
                INNER JOIN[shuffle]
                       (select alias1.pk from table_7_undef_partitions2_keys3_properties4_distributed_by5 AS alias1
                       LEFT JOIN table_8_undef_partitions2_keys3_properties4_distributed_by5 AS alias2
                       ON alias1.`col_varchar_10__undef_signed` = alias2.`col_varchar_1024__undef_signed`) tmp1
                ON tmp1 . `pk` = tmp2 . `pk`;
                    """
	        contains "BUCKET_SHUFFLE"
	        contains "distribute expr lists: pk[#22]"
	        contains "distribute expr lists: pk[#11]"
    	}

        multi_sql """
	    drop table if exists baseall;
	    drop table if exists test;
	    CREATE TABLE IF NOT EXISTS `baseall` (
                `k1` tinyint(4) null comment ""
            ) engine=olap
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3 properties("replication_num" = "1");

	    CREATE TABLE IF NOT EXISTS `test` (
                `k1` tinyint(4) null comment ""
            ) engine=olap
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3 properties("replication_num" = "1");
	    
	    insert into baseall values (1);
	    insert into baseall values (2);
	    insert into baseall values (3);
	    insert into test values (1);
	    insert into test values (2);
	    insert into test values (3);

            set enable_nereids_distribute_planner=true;
            set enable_pipeline_x_engine=true;
            set disable_join_reorder=true;
            set enable_local_shuffle=true;
            set force_to_local_shuffle=true;
    	"""

	explain {
                sql """
		select tmp.k1 from baseall d join (select a.k1 as k1 from baseall b join test a on (a.k1=b.k1)) tmp on tmp.k1 = d.k1;
                """
    	        contains "COLOCATE"
    	        contains "distribute expr lists: k1[#5]"
    	        contains "distribute expr lists: k1[#4]"
        }
}
