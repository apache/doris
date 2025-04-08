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

suite("bucket_shuffle_to_right") {
    multi_sql """
        set enable_nereids_distribute_planner=true;
        set disable_join_reorder=true;
        
        drop table if exists table_20_undef_partitions2_keys3_properties4_distributed_by53;
        create table table_20_undef_partitions2_keys3_properties4_distributed_by53 (
                                                                                       `pk` int,
                                                                                       `col_varchar_10__undef_signed` varchar(10)   ,
                                                                                       `col_int_undef_signed` int   ,
                                                                                       `col_varchar_1024__undef_signed` varchar(1024)
        ) engine=olap
        DUPLICATE KEY(pk, col_varchar_10__undef_signed)
        
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into table_20_undef_partitions2_keys3_properties4_distributed_by53(pk,col_int_undef_signed,col_varchar_10__undef_signed,col_varchar_1024__undef_signed) values (0,null,"at",'b'),(1,null,'e',"your"),(2,6,'h',"look"),(3,3,'z',"to"),(4,6,'q',"did"),(5,7,"come",'g'),(6,null,'x',"his"),(7,null,'w','s'),(8,null,"don't",'l'),(9,null,"and","know"),(10,null,'q','c'),(11,null,'u','w'),(12,9,'c','x'),(13,null,"my","or"),(14,null,'a','i'),(15,null,"look",'u'),(16,2,"were","be"),(17,null,"is",'k'),(18,null,'c',"her"),(19,null,"but",'x');
        
        
        drop table if exists table_23_undef_partitions2_keys3_properties4_distributed_by52;
        create table table_23_undef_partitions2_keys3_properties4_distributed_by52 (
                                                                                       `pk` int,
                                                                                       `col_int_undef_signed` int   ,
                                                                                       `col_varchar_10__undef_signed` varchar(10)   ,
                                                                                       `col_varchar_1024__undef_signed` varchar(1024) MIN
        ) engine=olap
        AGGREGATE KEY(pk, col_int_undef_signed, col_varchar_10__undef_signed)
        
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into table_23_undef_partitions2_keys3_properties4_distributed_by52(pk,col_int_undef_signed,col_varchar_10__undef_signed,col_varchar_1024__undef_signed) values (0,null,"to","so"),(1,null,'u','j'),(2,null,"say","would"),(3,null,'t',"to"),(4,0,"your",'q'),(5,1,'w',"if"),(6,null,"right",'p'),(7,7,'h',"her"),(8,6,"that",'v'),(9,5,'k',"as"),(10,null,"know","did"),(11,9,"to",'q'),(12,null,"look","don't"),(13,9,"say",'v'),(14,null,'m','j'),(15,9,"i","want"),(16,4,"then","why"),(17,null,"something",'p'),(18,2,'i',"for"),(19,5,"for",'q'),(20,6,"he",'r'),(21,null,'n',"didn't"),(22,null,'n','x');
        
        
        drop table if exists table_100_undef_partitions2_keys3_properties4_distributed_by5;
        create table table_100_undef_partitions2_keys3_properties4_distributed_by5 (
                                                                                       `col_int_undef_signed` int/*agg_type_placeholder*/   ,
                                                                                       `col_varchar_10__undef_signed` varchar(10)/*agg_type_placeholder*/   ,
                                                                                       `col_varchar_1024__undef_signed` varchar(1024)/*agg_type_placeholder*/   ,
                                                                                       `pk` int/*agg_type_placeholder*/
        ) engine=olap
        
        
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
        insert into table_100_undef_partitions2_keys3_properties4_distributed_by5(pk,col_int_undef_signed,col_varchar_10__undef_signed,col_varchar_1024__undef_signed) values (0,null,"when","yes"),(1,null,"do",'i'),(2,1,"all","didn't"),(3,null,"don't","who"),(4,9,"your","it"),(5,5,'n','c'),(6,0,"up","it's"),(7,9,'d','a'),(8,3,"yeah",'v'),(9,null,'r','s'),(10,5,'s','n'),(11,null,'w','l'),(12,null,'k',"she"),(13,1,"from","what"),(14,1,'t',"at"),(15,null,"something",'s'),(16,null,'q',"his"),(17,3,'k','d'),(18,6,"you",'m'),(19,null,"something","could"),(20,8,'e','d'),(21,null,"I'm",'j'),(22,8,"get","can"),(23,5,'b',"but"),(24,3,'f','q'),(25,8,"who",'z'),(26,null,"was","her"),(27,5,'q','l'),(28,1,"here","about"),(29,null,'i',"for"),(30,null,"that","ok"),(31,null,'l',"from"),(32,9,"my",'o'),(33,6,'k',"one"),(34,null,"or","yeah"),(35,null,'q',"going"),(36,7,'x','g'),(37,null,'b',"one"),(38,null,'w','l'),(39,9,'q',"that"),(40,8,'x',"the"),(41,1,"was",'y'),(42,9,"his","you"),(43,null,"with","okay"),(44,0,"with",'f'),(45,2,'c',"as"),(46,null,"yes",'l'),(47,null,'a',"yeah"),(48,2,'c','b'),(49,5,'f','j'),(50,null,'k','j'),(51,4,"don't",'b'),(52,1,"that's",'u'),(53,null,"had",'z'),(54,null,"didn't",'o'),(55,null,'t','v'),(56,null,"this",'o'),(57,null,'b','p'),(58,7,'u',"good"),(59,null,"something",'a'),(60,2,"back",'w'),(61,null,"got","he"),(62,null,'i',"back"),(63,5,"why","be"),(64,0,'m',"he's"),(65,null,'j','e'),(66,null,"from",'z'),(67,null,'y','i'),(68,null,"he's",'l'),(69,5,"him",'b'),(70,2,'b','b'),(71,1,'w','u'),(72,8,'n',"he's"),(73,4,"say","you"),(74,null,'y',"as"),(75,null,"he",'h'),(76,5,'k','y'),(77,null,"not","is"),(78,3,"on","you're"),(79,null,'a','r'),(80,null,"just","that"),(81,8,'k',"as"),(82,null,'y',"to"),(83,2,"we","yes"),(84,7,'j',"look"),(85,2,'v','d'),(86,null,"me",'u'),(87,8,"okay",'u'),(88,4,"not",'p'),(89,3,'f',"he's"),(90,null,'g',"what"),(91,3,'l','o'),(92,null,"I'm",'i'),(93,null,"been","back"),(94,null,'w','b'),(95,null,"okay","who"),(96,1,"got","all"),(97,null,"your",'c'),(98,null,'i','u'),(99,null,"ok","they");
        """

    order_qt_bucket_shuffle_to_right """
        select alias1 . `col_int_undef_signed` AS field1
        from table_100_undef_partitions2_keys3_properties4_distributed_by5 as alias3
        right outer join
        (
            select alias1 . `pk`, alias1 . `col_int_undef_signed`
            from table_23_undef_partitions2_keys3_properties4_distributed_by52 as alias2
            right outer join
            table_20_undef_partitions2_keys3_properties4_distributed_by53 as alias1
            on  alias1 . `pk` = alias2 . `col_int_undef_signed`
        ) alias1 ON alias1 . `pk` = alias3 . `pk`
        WHERE alias1 . `pk` >= 0;
        """
}
