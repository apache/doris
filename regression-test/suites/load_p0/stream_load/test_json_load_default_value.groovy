suite("test_json_load_default_value", "p0") { 
    // define a sql table
    def testTable = "test_json_load_default_value"

    def create_test_table = {testTablex ->
        // multi-line sql
        sql """ DROP TABLE IF EXISTS ${testTable} """
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTable} (
                        id INT NOT NULL DEFAULT '0',
                        country VARCHAR(32) NULL DEFAULT 'default_country',
                        city VARCHAR(32) NULL DEFAULT 'default_city',
                        code BIGINT DEFAULT '1111')
                        DISTRIBUTED BY RANDOM BUCKETS 10
                        PROPERTIES("replication_num" = "1");
                        """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 1)
        assertTrue(result[0][0] == 0, "Create table should update 0 rows")
    }
        
    def load_json_data = {strip_flag, read_flag, format_flag, json_paths, file_name ->
        // load the json data
        streamLoad {
            table testTable
            
            // set http request header params
            set 'strip_outer_array', strip_flag
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            set 'jsonpaths', json_paths
            file file_name
        }
    }

    // case1: import simple json lack one column
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table.call(testTable)
        load_json_data.call('true', '', 'json', '', 'simple_json.json')
        sql "sync"
        qt_select1 "select * from ${testTable} order by id"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case2: import json lack one column of rows
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table.call(testTable)
        load_json_data.call('true', '', 'json', '', 'simple_json2_lack_one_column.json')
        sql "sync"
        qt_select2 "select * from ${testTable} order by id"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}