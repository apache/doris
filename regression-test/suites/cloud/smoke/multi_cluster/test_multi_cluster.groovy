suite("smoke_test_multi_cluster", "smoke") {
    // smoke prepare env, create 2 clusters(smoke_test_cluster_01, smoke_test_cluster_02)
    def cluster1 = "smoke_test_cluster_01"
    def cluster2 = "smoke_test_cluster_02"
    List<List<Object>> result = sql "show clusters"

    if (result.size() != 2) {
        // if not smoke env, just return. such as regression case run daily doesn't have $cluster1 and $cluster2
        logger.info("clusters size not eq 2, not smoke env")
        return
    }

    if (!result.stream().map{it[0]}.toList().containsAll("$cluster1", "$cluster2")) {
        // if not smoke env, just return. such as regression case run daily doesn't have $cluster1 and $cluster2
        logger.info("clusters not have '$cluster1' and '$cluster2'")
        return
    }

    assertTrue(result.size() == 2)

    // 1. test switch cluster
    sql "use @$cluster1"
    result  = sql "show clusters"
    for (row : result) {
        println row
        if(row[0] == "$cluster1") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    sql "use @$cluster2"
    result  = sql "show clusters"
    for (row : result) {
        println row
        if(row[0] == "$cluster2") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    // 2. test default cluster
    def user1 = "smoke_test_cloud_multi_cluster_user1"
    sql """drop user if exists ${user1}"""

    sql """create user ${user1} identified by 'Cloud12345'"""
    // for use default_cluster:regression_test
    sql """GRANT SELECT_PRIV on *.*.* to ${user1}"""
    sql """GRANT USAGE_PRIV ON CLUSTER '${cluster1}' TO '${user1}'"""
    // set default cluster2 to user1
    sql "SET PROPERTY FOR '$user1' 'default_cloud_cluster' = '$cluster2'"

    result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        sql "show clusters"
    }

    for (row : result) {
        println row
        if(row[0] == "$cluster2") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    sql """drop user if exists ${user1}"""

    // 3. test multi cluster read、write
    sql """
        drop table if exists test_table_smoke_multi_cluster
    """

    sql """
        CREATE TABLE test_table_smoke_multi_cluster (
            class INT,
            id INT,
            score INT SUM
        )
        AGGREGATE KEY(class, id)
        DISTRIBUTED BY HASH(class) BUCKETS 16
    """

    sql """use @$cluster1"""
    sql """
        insert into test_table_smoke_multi_cluster values (1, 2, 3);
    """
    sql """use @$cluster2"""
    qt_sql """select * from test_table_smoke_multi_cluster"""

    sql """
        insert into test_table_smoke_multi_cluster values (4, 5, 6);
    """
    sql """use @$cluster1"""
    qt_sql """select * from test_table_smoke_multi_cluster"""

    // 4. test rename cluster and read、write

    sql """
        drop table if exists test_table_smoke_multi_cluster
    """
}
