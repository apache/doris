suite("test_mysql_load", "external_docker") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "mysql_catalog_load_resource";
        String catalog_name = "mysql_catalog_load";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        String SID = "doris_test";
        String ex_table_name = "test_load"
        String internal_db_name = "doris_test";
        String internal_table_name = "in_test_load";


        sql """drop catalog if exists ${catalog_name} """
        //sql """drop resource if exists ${resource_name}"""

        sql """drop database if exists ${internal_db_name}"""
        sql """create database if not exists ${internal_db_name}"""
        sql """use ${internal_db_name}"""
        sql """drop table if exists ${internal_table_name}"""
        sql """CREATE TABLE ${internal_table_name} (
              x varchar(50),
              y varchar(50)
            ) ENGINE=OLAP
            DUPLICATE KEY(x)
            DISTRIBUTED BY HASH(x) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

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
        
        def ex_num = sql  """select count(*) from ${catalog_name}.${ex_db_name}.${ex_table_name}"""
        log.info(ex_num.toString())
        def res = sql """insert into internal.${internal_db_name}.${internal_table_name} select * from ${catalog_name}.${ex_db_name}.${ex_table_name};"""
        log.info(res.toString())

        def res_01 = sql  """select count(*) from internal.${internal_db_name}.${internal_table_name}"""
        log.info(res_01.toString())
        assertEquals(ex_num[0][0],res_01[0][0])


        // def res_02 = sql """insert into internal.${internal_db_name}.${internal_table_name} values('asdfs','fadfag')"""
        sql """insert into ${catalog_name}.${ex_db_name}.${ex_table_name} select * from internal.${internal_db_name}.${internal_table_name}"""

        def res_02 = sql """select count(*) from internal.${internal_db_name}.${internal_table_name};"""

        def internal_external_sum= 2 * ex_num[0][0]
        log.info(internal_external_sum.toString())


        def res_03 = sql """select count(*) from ${catalog_name}.${ex_db_name}.${ex_table_name};"""
        log.info(res_03.toString())
        assertEquals(internal_external_sum, res_03[0][0])

        def internal_rows=res_02[0][0]
        def external_rows=res_03[0][0]
        log.info("inner table rows count: ${internal_rows}")
        log.info("external table rows count: ${external_rows}")


    }
}
