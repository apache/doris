suite("smoke_test_grant_revoke_stage_to_user", "smoke") {
    if (context.config.cloudVersion != null && !context.config.cloudVersion.isEmpty()
            && compareCloudVersion(context.config.cloudVersion, "3.0.0") >= 0) {
        log.info("case: smoke_test_grant_revoke_stage_to_user, cloud version ${context.config.cloudVersion} bigger than 3.0.0, skip".toString());
        return
    }
    def user1 = "regression_test_user1"
    def stage1 = "test_stage_1"
    def role = "admin"
    try_sql("DROP USER if exists ${user1}")

    def fail1 = try_sql """
        GRANT USAGE_PRIV ON STAGE ${stage1} TO ${user1};
    """
    // ERROR 1105 (HY000): errCode = 2, detailMessage = user 'default_cluster:user1'@'%' does not exist
    assertEquals(fail1, null)

    sql """CREATE USER '${user1}' IDENTIFIED BY 'Cloud123456' DEFAULT ROLE '${role}'"""

    def succ1 = try_sql """
        GRANT USAGE_PRIV ON STAGE ${stage1} TO ${user1};
    """
    // OK
    assertEquals(succ1.size(), 1)

    sql "sync"

    def result1 = connect(user=user1, password='Cloud123456', url=context.config.jdbcUrl) {
        sql "sync"
        def sg = try_sql """show grants"""
        assertEquals(sg.size(), 1)
    }

    def succ3 = try_sql """
        REVOKE USAGE_PRIV ON STAGE ${stage1} FROM ${user1};
    """
    assertEquals(succ3.size(), 1)

    def succ4 = try_sql """
        DROP USER if exists ${user1}
    """
    assertEquals(succ3.size(), 1)
}
