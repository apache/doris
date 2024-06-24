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

suite("test_compress_varchar") {
    sql "drop table if exists compress_varchar;"
    sql """
        create table compress_varchar(rowid int, vc1 varchar(1), vc2 varchar(2),
        vc3 varchar(3),vc4 varchar(4),vc5 varchar(5),vc6 varchar(6),vc7 varchar(7),
        vc8 varchar(8),vc9 varchar(9),vc10 varchar(10),vc11 varchar(11),vc12 varchar(12),
        vc13 varchar(13),vc14 varchar(14),vc15 varchar(15))
        distributed by hash(rowid) buckets 5 properties('replication_num'='1');
    """
    sql """
        insert into compress_varchar values(1,'a','ab','abc','abcd','abcde','abcdef','abcdefg','abcdefgh','abcdefghi','abcdefghij','abcdefghijk','abcdefghijkl','abcdefghijklm','abcdefghijklmn','abcdefghijklmno');
    """
    sql """
        insert into compress_varchar values(2,'','','','','','','','','','','','','','','');
    """
    sql """
        insert into compress_varchar values(3,'b','b','b','b','b','b','b','b','b','b','b','b','b','b','b');
    """
    qt_select_1 """
        select vc1, compress_as_smallint(vc1), decompress_varchar(compress_as_smallint(vc1)) = vc1 from compress_varchar order by rowid;
    """
    qt_select_2 """
        select vc2, compress_as_int(vc2), decompress_varchar(compress_as_int(vc2)) = vc2 from compress_varchar order by rowid;
    """
    qt_select_3 """
        select vc3, compress_as_int(vc3), decompress_varchar(compress_as_int(vc3)) = vc3 from compress_varchar order by rowid;
    """
    qt_select_4 """
        select vc4, compress_as_bigint(vc4), decompress_varchar(compress_as_bigint(vc4)) =vc4 from compress_varchar order by rowid;
    """
    qt_select_5 """
        select vc5, compress_as_bigint(vc5), decompress_varchar(compress_as_bigint(vc5)) = vc5 from compress_varchar order by rowid;
    """
    qt_select_6 """
        select vc6, compress_as_bigint(vc6), decompress_varchar(compress_as_bigint(vc6)) = vc6 from compress_varchar order by rowid;
    """
    qt_select_7 """
        select vc7, compress_as_bigint(vc7), decompress_varchar(compress_as_bigint(vc7)) = vc7 from compress_varchar order by rowid;
    """
    qt_select_8 """
        select vc8, compress_as_largeint(vc8), decompress_varchar(compress_as_largeint(vc8)) = vc8 from compress_varchar order by rowid;
    """
    qt_select_9 """
        select vc9, compress_as_largeint(vc9), decompress_varchar(compress_as_largeint(vc9)) = vc9 from compress_varchar order by rowid;
    """
    qt_select_10 """
        select vc10, compress_as_largeint(vc10), decompress_varchar(compress_as_largeint(vc10)) = vc10 from compress_varchar order by rowid;
    """
    qt_select_11 """
        select vc11, compress_as_largeint(vc11), decompress_varchar(compress_as_largeint(vc11)) = vc11 from compress_varchar order by rowid;
    """
    qt_select_12 """
        select vc12, compress_as_largeint(vc12), decompress_varchar(compress_as_largeint(vc12)) = vc12 from compress_varchar order by rowid;
    """
    qt_select_13 """
        select vc13, compress_as_largeint(vc13), decompress_varchar(compress_as_largeint(vc13)) = vc13 from compress_varchar order by rowid;
    """
    qt_select_14 """
        select vc14, compress_as_largeint(vc14), decompress_varchar(compress_as_largeint(vc14)) = vc14 from compress_varchar order by rowid;
    """
    qt_select_15 """
        select vc15, compress_as_largeint(vc15), decompress_varchar(compress_as_largeint(vc15)) = vc15 from compress_varchar order by rowid;
    """
    qt_select_16 """
        select decompress_varchar(cast('8321' as SMALLINT))
    """
    qt_select_17 """
        select decompress_varchar(compress_as_smallint('')) == '';
    """
    qt_select_18 """
        select decompress_varchar(compress_as_smallint('\0')) == '\0';
    """

    test{
        sql """
            select compress_as_largeint("abcdefghijklmnop");
        """
        exception "String is too long to compress"
    }

    test{
        sql """
            select decompress_varchar(2);
        """
        exception "Invalid input of function decompress_varchar"
    }
}