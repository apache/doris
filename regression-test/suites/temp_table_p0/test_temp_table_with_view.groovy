suite("test_temp_table_with_view", "p0") {
    def tableName = "t_test_temp_table_with_view"
    def viewName = "v_test_temp_table_with_view"
    sql("""create table ${tableName}(a int) properties("replication_num" = "1") """)
    sql("create view ${viewName} as select * from ${tableName}")
    sql("""create temporary table ${tableName}(a int, b int, c int, d int) properties("replication_num" = "1") """)

    // following query should success
    sql("select * from ${viewName}")
}
