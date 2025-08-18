suite("test_temp_table_with_mtmv", "p0") {
    def dbName = "tmp_db_test_temp_table_with_mtmv"
    def tableName = "t_test_temp_table_with_mtmv"
    def mtmvName = "mtmv_test_temp_table_with_mtmv"

    sql "drop database if exists ${dbName}"
    sql "create database ${dbName}"
    sql "use ${dbName}"

    // FIXME: this case only work if replication num is not 1
    sql """create table ${tableName}(a int, b int, c int, d int) properties("replication_num" = "1") """
    sql """insert into ${tableName} values(1,2,3,4)"""
    sql """create materialized view ${mtmvName} BUILD IMMEDIATE REFRESH COMPLETE as select * from ${tableName} """
    sql """create temporary table ${tableName}(a int) properties("replication_num" = "1") """
    sql """insert into ${tableName} values(5)"""

    // drop db should success
    sql("drop database ${dbName}")
}
