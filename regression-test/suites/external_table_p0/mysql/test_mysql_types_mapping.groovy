suite("test_mysql_types_mapping", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "mysql_catalog_resource";
        String catalog_name = "test_mysql_types_mapping";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
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

        // order_qt_1 """select * from ${catalog_name}.${ex_db_name}.test_bit_type order by 1 limit 1;"""


        order_qt_2 """select * from ${catalog_name}.${ex_db_name}.test_boolean_type order by 1 limit 1;"""


        order_qt_3 """select * from ${catalog_name}.${ex_db_name}.test_tinyint_type order by 1 limit 1;"""


        order_qt_4 """select * from ${catalog_name}.${ex_db_name}.test_smallint_type order by 1 limit 1;"""


        order_qt_5 """select * from ${catalog_name}.${ex_db_name}.test_integer_type order by 1 limit 1;"""


        order_qt_6 """select * from ${catalog_name}.${ex_db_name}.test_varchar2_type order by 1 limit 1;"""


        order_qt_7 """select * from ${catalog_name}.${ex_db_name}.test_char_type order by 1 limit 1;"""


        order_qt_8 """select * from ${catalog_name}.${ex_db_name}.test_char1_type order by 1 limit 1;"""


        order_qt_9 """select * from ${catalog_name}.${ex_db_name}.test_decimal_type order by 1 limit 1;"""

        order_qt_11 """select * from ${catalog_name}.${ex_db_name}.test_date_type order by 1 limit 1;"""


        order_qt_14 """select * from ${catalog_name}.${ex_db_name}.test_real_type order by 1 limit 1;"""


        order_qt_15 """select * from ${catalog_name}.${ex_db_name}.test_Unsigned_type order by 1 limit 1;"""


        order_qt_16 """select * from ${catalog_name}.${ex_db_name}.test_enum order by 1 limit 1;"""


        order_qt_17 """show create table ${catalog_name}.${ex_db_name}.test_enum;"""

        order_qt_18 """select * from ${catalog_name}.${ex_db_name}.test_binary_type order by 1 limit 1;"""

        order_qt_19 """select * from ${catalog_name}.${ex_db_name}.test_bit_type order by 1 limit 1;"""


        order_qt_12 """select * from ${catalog_name}.${ex_db_name}.test_json_type;"""

        order_qt_13 """select * from ${catalog_name}.${ex_db_name}.test_json1_type;"""


    }
}
