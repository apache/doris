test {
    sql "abcdefg"
    // check exception message contains
    exception "errCode = 2, detailMessage = Syntax error"
}

test {
    sql """
            select *
            from (
                select 1 id
                union all
                select 2
            ) a
            order by id"""

    // multi check condition

    // check return 2 rows
    rowNum 2
    // execute time must <= 5000 millisecond
    time 5000
    // check result, must be 2 rows and 1 column, the first row is 1, second is 2
    result(
        [[1], [2]]
    )
}

test {
    sql "a b c d e f g"

    // other check will not work because already declared a check callback
    exception "aaaaaaaaa"

    // callback
    check { result, exception, startTime, endTime ->
        // assertXxx() will invoke junit5's Assertions.assertXxx() dynamically
        assertTrue(exception != null)
    }
}

test {
    sql  """
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

    check { result, ex, startTime, endTime ->
        // same as order_sql(sqlStr)
        result = sortRows(result)

        assertEquals(null, result[0][0])
        assertEquals(1, result[1][0])
        assertEquals(15, result[2][0])
        assertEquals(2, result[3][0])
        assertEquals(3, result[4][0])
    }
}