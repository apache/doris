// execute sql and ignore result
sql "show databases"

// execute sql and get result, outer List denote rows, inner List denote columns in a single row
List<List<Object>> tables = sql "show tables"

// assertXxx() will invoke junit5's Assertions.assertXxx() dynamically
assertTrue(tables.size() >= 0) // test rowCount >= 0

// syntax error
try {
    sql "a b c d e"
    throw new IllegalStateException("Should be syntax error")
} catch (java.sql.SQLException t) {
    assertTrue(true)
}

def testTable = "test_sql_action1"

try {
    sql "DROP TABLE IF EXISTS ${testTable}"

    // multi-line sql
    def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${testTable} (
                            id int
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                          "replication_num" = "1"
                        ) 
                        """

    // DDL/DML return 1 row and 1 column, the only value is update row count
    assertTrue(result1.size() == 1)
    assertTrue(result1[0].size() == 1)
    assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

    def result2 = sql "INSERT INTO test_sql_action1 values(1), (2), (3)"
    assertTrue(result2.size() == 1)
    assertTrue(result2[0].size() == 1)
    assertTrue(result2[0][0] == 3, "Insert should update 3 rows")
} finally {
    /**
     * try_xxx(args) means:
     *
     * try {
     *    return xxx(args)
     * } catch (Throwable t) {
     *     // do nothing
     *     return null
     * }
     */
    try_sql("DROP TABLE IF EXISTS ${testTable}")

    // you can see the error sql will not throw exception and return
    try {
        def errorSqlResult = try_sql("a b c d e f g")
        assertTrue(errorSqlResult == null)
    } catch (Throwable t) {
        assertTrue(false, "Never catch exception")
    }
}

// order_sql(sqlStr) equals to sql(sqlStr, isOrder=true)
// sort result by string dict
def list = order_sql """
                select 2
                union all
                select 1
                union all
                select null
                union all
                select 15
                union all
                select 3
                """

assertEquals(null, list[0][0])
assertEquals(1, list[1][0])
assertEquals(15, list[2][0])
assertEquals(2, list[3][0])
assertEquals(3, list[4][0])