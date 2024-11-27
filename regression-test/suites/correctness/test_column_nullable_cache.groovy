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

suite("test_column_nullable_cache") {
    sql """
        drop table if exists test_column_nullable_cache;
    """
     sql """
        CREATE TABLE `test_column_nullable_cache` (
            `col_int_undef_signed2` int NULL,
            `col_int_undef_signed` int NULL,
            `col_int_undef_signed3` int NULL,
            `col_int_undef_signed4` int NULL,
            `pk` int NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`col_int_undef_signed2`)
            DISTRIBUTED by RANDOM BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
     """

    sql """
    insert into test_column_nullable_cache
        (pk,col_int_undef_signed,col_int_undef_signed2,col_int_undef_signed3,col_int_undef_signed4) 
        values (0,3,7164641,5,8),(1,null,3916062,5,6),(2,1,5533498,0,9),(3,7,2,null,7057679),(4,1,0,7,7),
        (5,null,4,2448564,1),(6,7531976,7324373,9,7),(7,3,1,1,3),(8,6,8131576,9,-1793807),(9,9,2,4214547,9),
        (10,-7299852,5,1,3),(11,7,3,-1036551,5),(12,-6108579,84823,4,1229534),(13,-1065629,5,4,null),(14,null,8072633,3328285,2),
        (15,2,7,6,6),(16,8,5,-4582103,1),(17,5,-4677722,-2379367,4),(18,-7807532,-6686732,0,5329341),
        (19,8,7,-4013246,-7013374),(20,0,2,9,2),(21,7,2383333,5,4),(22,5844611,2,2,0),(23,0,4756185,0,-5612039),
        (24,6,4878754,608172,0),(25,null,7858692,7,-6704206),(26,7,-1697597,6,9),(27,9,-7021349,3,-3094786),
        (28,2,2830915,null,8),(29,4133633,489212,5,9),(30,6,-3346211,3668768,2),(31,1,4862070,-5066405,0),(32,9,6,7,8),
        (33,2,null,4,2),(34,1,2893430,-3282825,5),(35,2,3,4,2),(36,4,-3418732,6,1263819),(37,5,4,-6342170,6),(99,9,2,8,null);
    """

    qt_test1 """
        select * from test_column_nullable_cache where col_int_undef_signed3 IS NULL and col_int_undef_signed3 = col_int_undef_signed3;
    """

    qt_test2 """
        select count(*) from test_column_nullable_cache where col_int_undef_signed3 IS NULL and col_int_undef_signed3 = col_int_undef_signed3;
    """
}
