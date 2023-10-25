suite("test_oracle_load", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "test_oracle_load_resource";
        String catalog_name = "test_oracle_load_catalog";
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String SID = "XE";
        String ex_db_name = "DORIS_TEST";
        String ex_table_name = "NATION_LOAD"
        String internal_db_name = "internal_test_oracle_load";
        String internal_table_name = "internal_nation";

        sql """drop catalog if exists ${catalog_name}"""
        // sql """drop resource if exists ${resource_name}"""
        sql """drop database if exists ${internal_db_name}"""
        sql """create database if not exists ${internal_db_name}"""
        sql """
               CREATE TABLE ${internal_db_name}.${internal_table_name} (
                  `n_nationkey` int(11) NOT NULL,
                  `n_name`      varchar(25) NOT NULL,
                  `n_regionkey` int(11) NOT NULL,
                  `n_comment`   varchar(152) NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`N_NATIONKEY`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                )
            """

        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/data_lake_driver/ojdbc6.jar",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""

        sql """switch ${catalog_name}"""

        sql """show databases;"""

        sql """use DORIS_TEST"""

        def ex_num = sql  """select count(*) from ${catalog_name}.${ex_db_name}.${ex_table_name}"""

        def res = sql """insert into internal.${internal_db_name}.${internal_table_name} select * from ${catalog_name}.${ex_db_name}.${ex_table_name}"""
        log.info(res.toString())
        def res_01 = sql  """select count(*) from internal.${internal_db_name}.${internal_table_name}"""
        assertEquals(ex_num[0][0],res_01[0][0])



        def res_02 = sql """insert into ${catalog_name}.${ex_db_name}.${ex_table_name}  select * from internal.${internal_db_name}.${internal_table_name}"""
        log.info(res_02.toString())

        def internal_external_sum= res_01[0][0] + ex_num[0][0]
        def res_03 = sql """select count(*) from ${catalog_name}.${ex_db_name}.${ex_table_name}"""
        assertEquals(internal_external_sum, res_03[0][0])

        def internal_rows=res_01[0][0]
        def external_rows=res_03[0][0]
        log.info("inner table count: ${internal_rows}")
        log.info("external table count: ${external_rows}")

    }
}
