suite("test_oracle_read_and_write", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "test_oracle_read_and_write_resource";
        String catalog_name = "test_oracle_read_and_write_catalog";
        String ex_db_name = "DORIS_TEST";
        String ex_table_name = "NATION_READ_AND_WRITE";
        String ex_table_view_name = "NATION_READ_AND_WRITE_VIEW";
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

        order_qt_1 """select * from ${catalog_name}.${ex_db_name}.${ex_table_name} order by n_nationkey limit 1"""

        order_qt_2 """select * from ${catalog_name}.${ex_db_name}.${ex_table_name} where n_nationkey > 6  order by n_nationkey limit 1"""

        order_qt_3 """select * from ${catalog_name}.${ex_db_name}.${ex_table_name} where n_nationkey > 6 and N_REGIONKEY<3  order by n_nationkey limit 1"""

        order_qt_4 """select * from ${catalog_name}.${ex_db_name}.${ex_table_view_name}  order by n_nationkey limit 1"""

        UUID uuid = UUID.randomUUID();
        String uuid_str = uuid.toString();

        // insert into
        sql """INSERT INTO ${catalog_name}.${ex_db_name}.${ex_table_name} VALUES (55,'$uuid_str', 10,'INSERT SUCCESS');"""

        def res = sql """select count(*) from ${catalog_name}.${ex_db_name}.${ex_table_name} WHERE N_NAME='$uuid_str';"""

        assertEquals(1,res[0][0])

        order_qt_7 """select * from ${catalog_name}.${ex_db_name}.${ex_table_name} where n_nationkey in (6, 7)  order by n_nationkey limit 1"""

        order_qt_8 """select * from ${catalog_name}.${ex_db_name}.${ex_table_name} where n_nationkey between 6 and 7  order by n_nationkey limit 1"""

        order_qt_9 """select count(*) from ${catalog_name}.${ex_db_name}.${ex_table_name} where n_nationkey between 6 and 7"""

        order_qt_10 """select sum(n_nationkey) from ${catalog_name}.${ex_db_name}.${ex_table_name} where n_nationkey between 6 and 7"""

        order_qt_10 """select distinct n_nationkey from ${catalog_name}.${ex_db_name}.${ex_table_name} where n_nationkey between 6 and 7"""

        order_qt_11 """select count(distinct n_nationkey) from ${catalog_name}.${ex_db_name}.${ex_table_name} where n_nationkey between 6 and 7"""


    }
}
