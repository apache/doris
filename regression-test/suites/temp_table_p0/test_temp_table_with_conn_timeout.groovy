suite("test_temp_table_with_conn_timeout", "p0") {
    String db = context.config.getDbNameByFile(context.file)
    def tableName = "t_test_temp_table_with_conn_timeout"
    String tempTableFullName
    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
        sql"use ${db}"
        sql """create temporary table ${tableName}(id int) properties("replication_num" = "1") """
        def show_result = sql_return_maparray("show data")

        show_result.each {  row ->
            if (row.TableName.contains(tableName)) {
                tempTableFullName = row.TableName
            }
        }
        assert tempTableFullName != null

        // set session variable for a short connection timeout
        sql "set interactive_timeout=5"
        sql "set wait_timeout=5"

        sleep(10*1000)
    }

    // temp table should not exist after session exit
    def tables = sql_return_maparray("show data")
    assert tables.find { it.TableName == tempTableFullName } == null
}
