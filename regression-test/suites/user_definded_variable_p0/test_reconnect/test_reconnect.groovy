suite("test_reconnect", "description") {

    // 1. Set the value of variable @A to 3 before starting execution
    sql """set @A = 3"""

    // 2. Disconnect the current connection
    disconnect()

    // 3. Reconnect and execute select @A to query the value of the variable
    def result = connect(user = 'root', password = '', url = context.config.jdbcUrl) {
        sql 'select @A'
    }

    // 4. Check if the result is NULL
    test {
        assert result[0][0] == null, "Variable @A should be NULL"
    }
}