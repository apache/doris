suite("test_mysql_write", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "mysql_catalog_write_resource";
        String catalog_name = "mysql_catalog_write";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String SID = "doris_test";
        String test_insert = "TEST_INSERT";
        String inDorisTable = "doris_in_tb";
        String ex_table_name = "nation_load"
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

        UUID uuid = UUID.randomUUID();
        String uuid_str = uuid.toString();
        // insert into
        sql """INSERT INTO ${catalog_name}.${ex_db_name}.${ex_table_name} VALUES (55,'$uuid_str', 10,'INSERT SUCCESS');"""

        def res = sql """select count(*) from ${catalog_name}.${ex_db_name}.${ex_table_name} WHERE N_NAME='$uuid_str';"""
        assertEquals(1,res[0][0])

        UUID uuid1 = UUID.randomUUID();
        String uuid_str1 = uuid1.toString();
        sql   """INSERT INTO ${catalog_name}.${ex_db_name}.test_view  VALUES (65,'$uuid_str1', 20,'INSERT VIEW SUCCESS');"""
        def res_view = sql """select count(*) from ${catalog_name}.${ex_db_name}.test_view WHERE N_NAME='$uuid_str1'"""
        log.info(res_view.toString())
        assertEquals(1,res_view[0][0])

        order_qt_3  """SELECT test FROM ${catalog_name}.${ex_db_name}.test_insert_unicode WHERE test = 'aa';"""

        order_qt_4  """SELECT test FROM ${catalog_name}.${ex_db_name}.test_insert_unicode WHERE test = 'bé';"""

        order_qt_5  """SELECT test FROM ${catalog_name}.${ex_db_name}.test_insert_unicode WHERE test > 'ba';"""

        order_qt_6  """SELECT test FROM ${catalog_name}.${ex_db_name}.test_insert_unicode WHERE test < 'ba';"""

        order_qt_7  """SELECT test FROM ${catalog_name}.${ex_db_name}.test_insert_unicode WHERE test = 'ba';"""

        order_qt_9  """SELECT count(DISTINCT t_varchar) FROM ${catalog_name}.${ex_db_name}.test_insert_unicode1;"""

        order_qt_10  """SELECT count(DISTINCT t_char) FROM ${catalog_name}.${ex_db_name}.test_insert_unicode1;"""

        order_qt_11  """SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM ${catalog_name}.${ex_db_name}.test_insert_unicode1;"""

        //order_qt_13  """insert into ${catalog_name}.${ex_db_name}.test_date2_type values('-0001-01-01','-1582-10-04',NULL,'23:59:59');"""

        //order_qt_14  """insert into ${catalog_name}.${ex_db_name}.test_date1_type values('-2019-03-18 10:01:17.987','-2018-10-28 03:33:33.333333',NULL, '-2018-04-01 02:13:55');"""

    }
}
