suite("test_mysql_PredicatePushdown", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "mysql_catalog_resource_PredicatePushdown";
        String catalog_name = "mysql_catalog_PredicatePushdown";
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

        order_qt_1 """SELECT regionkey, nationkey, name FROM ${catalog_name}.${ex_db_name}.test_nation WHERE name = 'ROMANIA' limit 1;"""

        order_qt_2 """SELECT regionkey, nationkey, name FROM ${catalog_name}.${ex_db_name}.test_nation WHERE name BETWEEN 'POLAND' AND 'RPA' limit 1;"""

        order_qt_3 """SELECT regionkey, nationkey, name FROM ${catalog_name}.${ex_db_name}.test_nation WHERE name = 'romania' limit 1;"""

        order_qt_4 """SELECT regionkey, nationkey, name FROM ${catalog_name}.${ex_db_name}.test_nation WHERE nationkey = 19 limit 1;"""

        order_qt_5 """SELECT regionkey, nationkey, name FROM ${catalog_name}.${ex_db_name}.test_nation WHERE nationkey BETWEEN 18.5 AND 19.5 limit 1;"""

        order_qt_6 """select * from ${catalog_name}.${ex_db_name}.test_date_type where d_01 = DATE '1992-09-29' limit 1;"""

        order_qt_8 """SELECT * FROM (SELECT regionkey, sum(nationkey) FROM ${catalog_name}.${ex_db_name}.test_nation GROUP BY regionkey) as sub WHERE regionkey = 3 limit 1;"""

        order_qt_9 """SELECT regionkey, sum(nationkey) FROM ${catalog_name}.${ex_db_name}.test_nation GROUP BY regionkey HAVING sum(nationkey) = 77 order  by regionkey limit 1;"""

        // The doris and mysql query performance is inconsistent from_base64
        // order_qt_7 """SELECT x, y FROM ${catalog_name}.${ex_db_name}.binary_test WHERE y = from_base64('AFCBhLrkidtNTZcA9Ru3hw==') limit 1;"""

    }
}
