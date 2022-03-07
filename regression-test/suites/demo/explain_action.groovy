explain {
    sql("select 100")

    // contains("OUTPUT EXPRS:<slot 0> 100\n") && contains("PARTITION: UNPARTITIONED\n")
    contains "OUTPUT EXPRS:<slot 0> 100\n"
    contains "PARTITION: UNPARTITIONED\n"
}

explain {
    sql("select 100")

    // contains(" 100\n") && !contains("abcdefg") && !("1234567")
    contains " 100\n"
    notContains "abcdefg"
    notContains "1234567"
}

explain {
    sql("select 100")
    // simple callback
    check { explainStr -> explainStr.contains("abcdefg") || explainStr.contains(" 100\n") }
}

explain {
    sql("a b c d e")
    // callback with exception and time
    check { explainStr, exception, startTime, endTime ->
        // assertXxx() will invoke junit5's Assertions.assertXxx() dynamically
        assertTrue(exception != null)
    }
}