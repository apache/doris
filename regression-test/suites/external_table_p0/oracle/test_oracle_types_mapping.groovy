suite("test_oracle_types_mapping", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "test_oracle_types_mapping_resource";
        String catalog_name = "test_oracle_types_mapping_catalog";
        String ex_db_name = "DORIS_TEST";
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String SID = "XE";

        sql """drop catalog if exists ${catalog_name} """
        // sql """drop resource if exists ${resource_name}"""

        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/data_lake_driver/ojdbc6.jar",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""

        sql """switch ${catalog_name}"""

        sql """use DORIS_TEST"""

        sql """INSERT INTO ${catalog_name}.${ex_db_name}.TCC_AND_EMOJI
                VALUES
                (
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '😂',
                    '😂',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '😂',
                    '😂',
                    '攻殻機動隊',
                    '😂',
                    '攻殻機動隊',
                    '😂',
                    '攻殻機動隊',
                    '😂',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '😂',
                    '😂',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '😂',
                    '😂',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '😂',
                    '😂',
                    '攻殻機動隊',
                    '攻殻機動隊',
                    '攻殻機動隊'
                )
            """


        order_qt_1 """select * from ${catalog_name}.${ex_db_name}.TEST_NUMERICAL_TYPE"""

        order_qt_2 """select * from ${catalog_name}.${ex_db_name}.TEST_VACHAR_TYPE"""

        order_qt_3 """select * from ${catalog_name}.${ex_db_name}.TEST_NVACHAR_2_TYPE"""

        order_qt_5 """select * from ${catalog_name}.${ex_db_name}.TEST_CHAR_TYPE_01"""

        order_qt_6  """select * from ${catalog_name}.${ex_db_name}.TEST_CHAR_TYPE_02"""

        order_qt_8 """select * from ${catalog_name}.${ex_db_name}.TEST_DECIMAL_TYPE_01"""


        order_qt_11 """select * from ${catalog_name}.${ex_db_name}.TEST_NUMBER_TYPE"""

        // This case has some problems under jdbc
        //order_qt_16  """select * from ${catalog_name}.${ex_db_name}.TCC_AND_EMOJI"""


        //blob type is not currently supported
        //order_qt_12 """select * from ${catalog_name}.${ex_db_name}.TEST_BLOB_TYPE"""

        //this return here is variable
        //order_qt_13  """select * from ${catalog_name}.${ex_db_name}.TEST_RAW_TYPE"""

        //order_qt_14 """select * from ${catalog_name}.${ex_db_name}.TEST_DATE_TYPE"""

        //order_qt_15 """select * from ${catalog_name}.${ex_db_name}.TEST_TIMESTAMP_TYPE"""

        //clob type is not currently supported
        //order_qt_4 """select * from ${catalog_name}.${ex_db_name}.test_clob_type"""

        order_qt_9 """select * from ${catalog_name}.${ex_db_name}.TEST_DECIMAL_TYPE_02"""

        order_qt_10 """select * from ${catalog_name}.${ex_db_name}.TEST_DECIMAL_2_TYPE"""


    }
}
