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

suite("test_nestloop_join_nullable") {
    multi_sql '''
    drop table if exists table_20_undef_partitions2_keys3_properties4_distributed_by510;
    drop table if exists table_20_undef_partitions2_keys3_properties4_distributed_by512;

    create table table_20_undef_partitions2_keys3_properties4_distributed_by510 (
    col_int_undef_signed int/*agg_type_placeholder*/    ,
    col_int_undef_signed2 int/*agg_type_placeholder*/    ,
    col_float_undef_signed float/*agg_type_placeholder*/    ,
    col_int_undef_signed3 int/*agg_type_placeholder*/    ,
    col_int_undef_signed4 int/*agg_type_placeholder*/    ,
    col_int_undef_signed5 int/*agg_type_placeholder*/    ,
    pk int/*agg_type_placeholder*/
    ) engine=olap
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into table_20_undef_partitions2_keys3_properties4_distributed_by510(pk,col_int_undef_signed,col_int_undef_signed2,col_float_undef_signed,col_int_undef_signed3,col_int_undef_signed4,col_int_undef_signed5) values (19,-538797,111,-2357243,88,null,null),(18,11035,null,-7629271,null,2400941,-12250),(17,119,8360350,null,-101,-57,-2825979),(16,93,-5,101,81,-57,null),(15,24421,3576066,null,-8351025,3094515,null),(14,-32,-53,-117,null,408662,69),(13,-28788,null,-3424,58,-16,5885342),(12,4378160,54,-14810,17886,-30200,-10060),(11,77,-43,null,-35,null,23),(10,-2333750,-18428,58,1504,-6105795,31),(9,30258,-158232,8149676,null,null,-1923047),(8,-30473,-29077,null,-9975,-60,null),(7,1447364,-17722,-5366279,-4911,3433200,null),(6,8,21708,-61,null,null,null),(5,-7131891,-15453,13205,4832507,-4572942,-57),(4,-61,2924909,-3996268,-63,-8108590,11731),(3,-902463,12,null,-6,8136,23281),(2,null,null,null,4533188,-26763,85),(1,-27575,null,null,null,-9,-42),(0,17583,-121,null,null,-79,-22443);

    create table table_20_undef_partitions2_keys3_properties4_distributed_by512 (
    pk int,
    col_int_undef_signed int    ,
    col_int_undef_signed2 int    ,
    col_float_undef_signed float    ,
    col_int_undef_signed3 int    ,
    col_int_undef_signed4 int    ,
    col_int_undef_signed5 int    
    ) engine=olap
    DUPLICATE KEY(pk)
    distributed by hash(pk) buckets 10
    properties("replication_num" = "1");
    insert into table_20_undef_partitions2_keys3_properties4_distributed_by512(pk,col_int_undef_signed,col_int_undef_signed2,col_float_undef_signed,col_int_undef_signed3,col_int_undef_signed4,col_int_undef_signed5) values (19,null,-122,-4279753,-7311927,89,null),(18,-35,8334250,1537805,-112,-21552,1527847),(17,24212,null,null,62,-26157,43),(16,-3845324,null,-11824,5033341,null,121),(15,-4642469,null,-35,null,5146485,9832),(14,7780660,null,-3021705,-29468,105,-6163),(13,null,46,null,104,-20633,44),(12,7629243,null,-107,3637456,82,-39),(11,106,37,25249,null,2622107,null),(10,null,-13728,10270,-26,13731,92),(9,19752,58,null,-4083,1569134,473442),(8,null,-14922,-22053,null,1953,79),(7,-18243,null,null,-62,-87,-2355558),(6,-120,518201,null,null,-50,null),(5,5201336,5855553,-5913148,-8576,35,-1471873),(4,null,-30894,-24418,31495,null,-48),(3,-860330,7609530,73,6729250,-91,7695),(2,3009668,null,null,-26,2992,null),(1,-38,28,14143,null,-13643,-126),(0,-1141940,-50,null,7458207,66,null);
    '''

    qt_select '''
        SELECT
        FIRST_VALUE(t2.col_float_undef_signed) OVER (
            ORDER BY
                t1.col_int_undef_signed4,
                (
                    t2.col_int_undef_signed4 + t1.col_int_undef_signed4
                ),
                t2.col_int_undef_signed4,
                t1.col_int_undef_signed3,
                t1.col_int_undef_signed5,
                abs(t1.col_int_undef_signed),
                t1.col_float_undef_signed,
                t2.col_int_undef_signed
        ),
        LAST_VALUE(t2.col_int_undef_signed5) OVER (
            ORDER BY
                (
                    t2.col_float_undef_signed * t2.col_int_undef_signed
                ),
                t2.col_int_undef_signed5,
                1,
                t2.col_float_undef_signed,
                atan2(
                    t2.col_int_undef_signed3,
                    t1.col_int_undef_signed3
                ),
                t2.col_int_undef_signed,
                t1.col_float_undef_signed,
                t2.col_float_undef_signed
        ),
        RANK() OVER (
            PARTITION BY (t1.col_int_undef_signed2 * 80954088951981892)
            ORDER BY
                6,
                ln(t2.col_int_undef_signed3),
                t2.col_float_undef_signed,
                t2.col_int_undef_signed,
                (
                    t1.col_int_undef_signed4 - t2.col_int_undef_signed4
                ),
                t2.col_float_undef_signed,
                t1.col_int_undef_signed5,
                t2.col_int_undef_signed
        ),
        (
            t2.col_int_undef_signed5 - t1.col_int_undef_signed2
        ),
        (
            t1.col_int_undef_signed2 - t2.col_int_undef_signed5
        )
    from
        table_20_undef_partitions2_keys3_properties4_distributed_by510 t1
        RIGHT JOIN table_20_undef_partitions2_keys3_properties4_distributed_by512 t2 ON t1.col_int_undef_signed5 = t2.col_int_undef_signed3
        OR t1.col_int_undef_signed3 = t1.col_int_undef_signed3
        AND t2.col_int_undef_signed2 = t2.col_int_undef_signed2
    order by
        1,
        2,
        3,
        4,
        5;
    '''
}