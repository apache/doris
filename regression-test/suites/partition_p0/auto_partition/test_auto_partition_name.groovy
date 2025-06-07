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

suite("test_auto_partition_name") {
    sql "drop table if exists test_list_name1"
    sql """
        CREATE TABLE `test_list_name1` (
            `str` varchar
        )
        DUPLICATE KEY(`str`)
        AUTO PARTITION BY LIST (`str`)
        ()
        DISTRIBUTED BY HASH(`str`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql ''' insert into test_list_name1 values ("Beijing"), ("XXX"), ("xxx"), ("Abc"), (null), ("您好"), ("   "),
    (""), ("X"), ("NULL"), ("a,b,c,"), ("---###___a#b!c!!"), ("\\b\\c\\d\\a\\e\\f"),
    ("Beijing#Beijing@Beijing!Beijing"), ("Line1\nLine2"), ('12345'), ('-9876'), ('x;y;z'), ('p|q|r'),
    ('C:\\\\Windows\\\\'), ('Carriage\rReturn'), ('accenté'), ('😊😂👍'), ('!@#$%^&*()') '''
    def part1 = sql " show partitions from test_list_name1; "
    def resultSet1 = part1.collect { it[1] }.toSet()
    assertEquals(resultSet1, 
        ["p0", "p8cdaef6", "paccente97", "pX", "pAbc3", "pX1", "pxxx3", "pNULL4", "pBeijing7", "pCarriagedReturn15", "p_2d98765", "p2020203", "px3by3bz5", "p1f60ade0a1f602de021f44ddc4d6", "p_2d2d2d2323235f5f5fa23b21c212116", "pXXX3", "pp7cq7cr5", "pBeijing23Beijing40Beijing21Beijing31", "pC3a5cWindows5c11", "pa2cb2cc2c6", "p60a8597d2", "p123455", "pLine1aLine211", "p21402324255e262a282910"]
        as Set
    )
    def row1 = (sql " select count() from test_list_name1 ")[0][0]
    assertEquals(row1, part1.size())

    sql "drop table if exists test_list_name2"
    sql """
        CREATE TABLE `test_list_name2` (
            `str1` varchar,
            `str2` varchar
        )
        DUPLICATE KEY(`str1`)
        AUTO PARTITION BY LIST (`str1`, `str2`)
        ()
        DISTRIBUTED BY HASH(`str1`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql '''
    INSERT INTO test_list_name2 VALUES ('123','123'), ('1231','23'), ('12','31,23'), ('1','231,23'),
        ('123,123','456'), ('1231,23','456'), (',123','23'), ('123,','23'), ('123',',23'), ('123','23,'),
        (',,',','), (',,',',,'), ('',',123'), (',',''), (NULL,','), (',',NULL), ('"a,b"','c,d'),
        ('\\,\\,','\\\\,'), ('你好,世界','😊,😂'), ('', ''), (' ', ' '), ('', ' '), ('1231,231','23'),
        ('123','1231,231'), ('12,3123',''), ('','12,3123')
    '''
    def part2 = sql " show partitions from test_list_name2; "
    def resultSet2 = part2.collect { it[1] }.toSet()
    assertEquals(resultSet2, 
        ["p2c10", "p1233232c3", "p12312c2374563", "p2c1234232", "p2c2c22c1", "p2c2c22c2c2", "p0122c31237", "p1232c12374563", "p201201", "p122312c235", "p12331233", "p112312c236", "p12314232", "p1232c4232", "p12312c2318232", "p22a2cb225c2cd3", "p2c1X", "p4f60597d2c4e16754c51f60ade0a2c1f602de025", "pX2c1", "p00", "p0201", "p122c312370", "p02c1234", "p2c2c25c2c2", "p12332c233", "p123312312c2318"]
        as Set)
    def row2 = (sql " select count() from test_list_name2 ")[0][0]
    assertEquals(row2, part2.size())

    sql "drop table if exists test_list_name3"
    sql """
        CREATE TABLE `test_list_name3` (
            `str` varchar
        )
        DUPLICATE KEY(`str`)
        AUTO PARTITION BY LIST (`str`)
        ()
        DISTRIBUTED BY HASH(`str`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "use_simple_auto_partition_name" = "true"
        );
    """
    sql ''' insert into test_list_name3 values ("Beijing"), ("XXX"), ("xxx"), ("Abc"), (null), ("您好"), ("   "),
    (""), ("X"), ("NULL"), ("a,b,c,"), ("---###___a#b!c!!"), ("\\b\\c\\d\\a\\e\\f"),
    ("Beijing#Beijing@Beijing!Beijing"), ("Line1\nLine2"), ('12345'), ('-9876'), ('x;y;z'), ('p|q|r'),
    ('C:\\\\Windows\\\\'), ('Carriage\rReturn'), ('accenté'), ('😊😂👍'), ('!@#$%^&*()') '''
    def part3 = sql " show partitions from test_list_name3; "
    def resultSet3 = part3.collect { it[1] }.toSet()
    assertEquals(resultSet3,
        ["p0", "p8cdaef6", "paccente97", "pX", "pAbc3", "pX1", "pxxx3", "pNULL4", "pBeijing7", "pCarriagedReturn15", "p_2d98765", "p2020203", "px3by3bz5", "p1f60ade0a1f602de021f44ddc4d6", "p_2d2d2d2323235f5f5fa23b21c212116", "pXXX3", "pp7cq7cr5", "pBeijing23Beijing40Beijing21Beijing31", "pC3a5cWindows5c11", "pa2cb2cc2c6", "p60a8597d2", "p123455", "pLine1aLine211", "p21402324255e262a282910"]
        as Set
    )
    def row3 = (sql " select count() from test_list_name3 ")[0][0]
    assertEquals(row3, part3.size())

    sql "drop table if exists test_list_name4"
    sql """
        CREATE TABLE `test_list_name4` (
            `str1` varchar,
            `str2` varchar
        )
        DUPLICATE KEY(`str1`)
        AUTO PARTITION BY LIST (`str1`, `str2`)
        ()
        DISTRIBUTED BY HASH(`str1`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "use_simple_auto_partition_name" = "true"
        );
    """
    sql '''
    INSERT INTO test_list_name4 VALUES ('123','123'), ('1231','23'), ('12','31,23'), ('1','231,23'),
        ('123,123','456'), ('1231,23','456'), (',123','23'), ('123,','23'), ('123',',23'), ('123','23,'),
        (',,',','), (',,',',,'), ('',',123'), (',',''), (NULL,','), (',',NULL), ('"a,b"','c,d'),
        ('\\,\\,','\\\\,'), ('你好,世界','😊,😂'), ('', ''), (' ', ' '), ('', ' '), ('1231,231','23'),
        ('123','1231,231'), ('12,3123',''), ('','12,3123')
    '''
    def part4 = sql " show partitions from test_list_name4; "
    def resultSet4 = part4.collect { it[1] }.toSet()
    assertEquals(resultSet4, 
        ["p2c10", "p1233232c3", "p12312c2374563", "p2c1234232", "p2c2c22c1", "p2c2c22c2c2", "p0122c31237", "p1232c12374563", "p201201", "p122312c235", "p12331233", "p112312c236", "p12314232", "p1232c4232", "p12312c2318232", "p22a2cb225c2cd3", "p2c1X", "p4f60597d2c4e16754c51f60ade0a2c1f602de025", "pX2c1", "p00", "p0201", "p122c312370", "p02c1234", "p2c2c25c2c2", "p12332c233", "p123312312c2318"]
        as Set)
    def row4 = (sql " select count() from test_list_name4 ")[0][0]
    assertEquals(row4, part4.size())

    // test simple range partition name.
    def test_partition_name_length = { interval, length ->
        def part = []
        def part_names = []
        sql "drop table if exists test_interval"
        sql """
            CREATE TABLE test_interval (
                `TIME_STAMP` datetimev2 NOT NULL
            )
            auto partition by range (date_trunc(`TIME_STAMP`, '${interval}'))()
            DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "use_simple_auto_partition_name" = "true"
            );
        """
        sql " insert into test_interval values ('2022-12-14'), ('2022-01-15'), ('2022-07-26'), ('2000-02-29'), ('2015-09-18'); "
        part = sql " show partitions from test_interval; "
        part_names = part.collect{it[1]}.sort()
        logger.info("${interval}: ${part_names}")

        part_names.each { element ->
            assertEquals(element.size(), length)
        }
    }
    test_partition_name_length("year", 5)
    test_partition_name_length("quarter", 7)
    test_partition_name_length("month", 7)
    test_partition_name_length("week", 9)
    test_partition_name_length("day", 9)
    test_partition_name_length("hour", 11)
    test_partition_name_length("minute", 13)
    test_partition_name_length("second", 15)

    def show_result = (sql "show create table test_interval").toString()
    assertTrue(show_result.contains("""\"use_simple_auto_partition_name\" = \"true\""""), show_result)


    // length exceed limit
    sql "drop table if exists `long_value1`"
    sql """
        CREATE TABLE `long_value1` (
            `str` varchar not null
        )
        DUPLICATE KEY(`str`)
        AUTO PARTITION BY LIST (`str`)
        ()
        DISTRIBUTED BY HASH(`str`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    test{
        sql """insert into `long_value1` values ("jwklefjklwehrnkjlwbfjkwhefkjhwjkefhkjwehfkjwehfkjwehfkjbvkwebconqkcqnocdmowqmosqmojwnqknrviuwbnclkmwkj");"""
        def exception_str = isGroupCommitMode() ? "s length is over limit of 50." : "Partition name's length is over limit of 50."
        exception exception_str
    }

    sql "drop table if exists `long_value2`"
    sql """
        CREATE TABLE `long_value2` (
            `i1` largeint not null,
            `i2` largeint not null
        )
        DUPLICATE KEY(`i1`)
        AUTO PARTITION BY LIST (`i1`, `i2`)
        ()
        DISTRIBUTED BY HASH(`i1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    test{
        sql """insert into `long_value2` values ("111111111111111111111111111111111111", "111111111111111111111111111111111111");"""
        def exception_str = isGroupCommitMode() ? "s length is over limit of 50." : "Partition name's length is over limit of 50."
        exception exception_str
    }
}
