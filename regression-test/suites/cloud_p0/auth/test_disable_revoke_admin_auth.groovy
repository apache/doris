suite("test_disable_revoke_admin_auth", "cloud_auth") {
    def user = "regression_test_cloud_revoke_admin_user"
    sql """drop user if exists ${user}"""

    sql """create user ${user} identified by 'Cloud12345' default role 'admin'"""

    sql "sync"

    try {
        result = sql """revoke 'admin' from 'admin'""";
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    try {
        result = connect(user = "${user}", password = 'Cloud12345', url = context.config.jdbcUrl) {
             sql """
                revoke 'admin' from 'admin'
             """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Unsupported operation"), e.getMessage())
    }

    result = sql """revoke 'admin' from ${user}"""
    assertEquals(result[0][0], 0)

    sql """drop user if exists ${user}"""
}
