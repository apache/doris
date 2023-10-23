suite("test_create_table_like") {

    sql """DROP TABLE IF EXISTS decimal_test"""
    sql """CREATE TABLE decimal_test
        (
            `name` varchar COMMENT "1m size",
            `id` SMALLINT COMMENT "[-32768, 32767]",
            `timestamp0` decimal null comment "c0",
            `timestamp1` decimal(38, 0) null comment "c1",
            `timestamp2` decimal(10, 1) null comment "c2",
            `timestamp3` decimalv3(38, 0) null comment "c3",
            `timestamp4` decimalv3(8, 3) null comment "c4",
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')"""
    qt_show_create_table """show create table decimal_test"""

    sql """DROP TABLE IF EXISTS decimal_test_like"""
    sql """CREATE TABLE decimal_test_like LIKE decimal_test"""
    qt_show_create_table_like """show create table decimal_test_like"""
}