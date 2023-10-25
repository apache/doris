suite("test_mysql_agg", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "mysql_catalog_agg_resource";
        String catalog_name = "mysql_catalog_agg";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String SID = "doris_test";
        String test_insert = "TEST_INSERT";
        String inDorisTable = "doris_in_tb";

        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists ${resource_name}"""

        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="root",
                    "password"="123456",
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/${SID}?useSSL=false",
                    "driver_url" = "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/data_lake_driver/mysql-connector-java-5.1.47.jar",
                    "driver_class" = "com.mysql.jdbc.Driver"
        );"""

        sql """switch ${catalog_name}"""

        sql """use ${catalog_name}.doris_test"""


        order_qt_0  """select count(*) from ${catalog_name}.${ex_db_name}.testImplementCount order by 1 limit 1;"""

        order_qt_1  """select count(v_bigint) from ${catalog_name}.${ex_db_name}.testImplementCount limit 1;"""

        order_qt_2  """select count(v_double) from ${catalog_name}.${ex_db_name}.testImplementCount limit 1;"""

        order_qt_3  """select count(distinct v_bigint) from ${catalog_name}.${ex_db_name}.testImplementCount limit 1;"""

        order_qt_4  """select count(1) from ${catalog_name}.${ex_db_name}.testImplementCount where v_bigint=20000000000 limit 1;"""

        order_qt_5  """select count(v_bigint) from ${catalog_name}.${ex_db_name}.testImplementCount where v_double=28.26 limit 1;"""

        order_qt_6  """select sum(v_bigint) from ${catalog_name}.${ex_db_name}.testImplementCount  limit 1;"""

        order_qt_7  """select sum(v_double) from ${catalog_name}.${ex_db_name}.testImplementCount  limit 1;"""

        order_qt_8  """select sum(distinct v_double) from ${catalog_name}.${ex_db_name}.testImplementCount  limit 1;"""

        order_qt_9  """select sum(distinct v_bigint) from ${catalog_name}.${ex_db_name}.testImplementCount  limit 1;"""

        order_qt_10  """select sum(v_bigint) from ${catalog_name}.${ex_db_name}.testImplementCount where v_double=15.7 limit 1;"""

    }
}
