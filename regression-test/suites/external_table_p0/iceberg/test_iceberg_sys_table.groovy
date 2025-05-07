// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_iceberg_sys_table", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_systable_ctl"
    String db_name = "test_iceberg_systable_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name}"""

    sql """drop database if exists ${db_name} force"""
    sql """create database ${db_name}"""
    sql """use ${db_name}"""

    sql """ create table test_iceberg_systable_tbl1 (id int) """
    sql """insert into test_iceberg_systable_tbl1 values(1);"""
    qt_sql_tbl1 """select * from test_iceberg_systable_tbl1"""
    qt_sql_tbl1_systable """select count(*) from test_iceberg_systable_tbl1\$snapshots"""
    List<List<Object>> res1 = sql """select * from test_iceberg_systable_tbl1\$snapshots"""
    List<List<Object>> res2 = sql """select * from iceberg_meta(
        "table" = "${catalog_name}.${db_name}.test_iceberg_systable_tbl1",
        "query_type" = "snapshots");
    """
    assertEquals(res1.size(), res2.size());
    for (int i = 0; i < res1.size(); i++) {
        for (int j = 0; j < res1[i].size(); j++) {
            assertEquals(res1[i][j], res2[i][j]);
        }
    }

    String snapshot_id = String.valueOf(res1[0][1]);
    qt_sql_tbl1_systable_where """
        select count(*) from test_iceberg_systable_tbl1\$snapshots where snapshot_id=${snapshot_id};
    """
    qt_tbl1_systable_where2 """select count(*) from iceberg_meta(
        "table" = "${catalog_name}.${db_name}.test_iceberg_systable_tbl1",
        "query_type" = "snapshots") where snapshot_id="${snapshot_id}";
    """

    sql """insert into test_iceberg_systable_tbl1 values(2);"""
    order_qt_sql_tbl12 """select * from test_iceberg_systable_tbl1"""
    qt_sql_tbl1_systable2 """select count(*) from test_iceberg_systable_tbl1\$snapshots"""

    qt_desc_systable1 """desc test_iceberg_systable_tbl1\$snapshots"""
    qt_desc_systable2 """desc ${db_name}.test_iceberg_systable_tbl1\$snapshots"""
    qt_desc_systable3 """desc ${catalog_name}.${db_name}.test_iceberg_systable_tbl1\$snapshots"""

    String user = "test_iceberg_systable_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    sql """create database if not exists internal.regression_test"""
    sql """grant select_priv on internal.regression_test.* to ${user}""" 
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
              sql """
                 select committed_at, snapshot_id, parent_id, operation from iceberg_meta(
                                             "table" = "${catalog_name}.${db_name}.test_iceberg_systable_tbl1",
                                             "query_type" = "snapshots");
              """
              exception "denied"
        }
        test {
              sql """
                 select committed_at, snapshot_id, parent_id, operation from ${catalog_name}.${db_name}.test_iceberg_systable_tbl1\$snapshots
              """
              exception "denied"
        }
    }
    sql """grant select_priv on ${catalog_name}.${db_name}.test_iceberg_systable_tbl1 to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """
           select committed_at, snapshot_id, parent_id, operation from iceberg_meta(
                                       "table" = "${catalog_name}.${db_name}.test_iceberg_systable_tbl1",
                                       "query_type" = "snapshots");
        """
        sql """select committed_at, snapshot_id, parent_id, operation from ${catalog_name}.${db_name}.test_iceberg_systable_tbl1\$snapshots"""
    }
    try_sql("DROP USER ${user}")

    // tbl2
    sql """ create table test_iceberg_systable_tbl2 (id int) """
    sql """insert into test_iceberg_systable_tbl2 values(2);"""
    qt_sql_tbl2 """select * from test_iceberg_systable_tbl2"""

    sql """ create table test_iceberg_systable_tbl3 (id int) """
    sql """insert into test_iceberg_systable_tbl3 values(3);"""
    qt_sql_tbl3 """select * from test_iceberg_systable_tbl3"""

    // drop db with tables
    test {
        sql """drop database ${db_name}"""
        exception """is not empty"""
    }

    // drop db froce with tables
    sql """drop database ${db_name} force"""

    // refresh catalog
    sql """refresh catalog ${catalog_name}"""
    // should be empty
    test {
        sql """show tables from ${db_name}"""
        exception "Unknown database"
    }

    // table should be deleted
    qt_test1 """
        select * from s3(
            "uri" = "s3://warehouse/wh/${db_name}/test_iceberg_systable_tbl1/data/*.parquet",
            "s3.endpoint"="http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "format" = "parquet",
            "use_path_style" = "true"
        )
    """
    qt_test2 """
        select * from s3(
            "uri" = "s3://warehouse/wh/${db_name}/test_iceberg_systable_tbl1/data/*.parquet",
            "s3.endpoint"="http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "format" = "parquet",
            "use_path_style" = "true"
        )
    """
    qt_test3 """
        select * from s3(
            "uri" = "s3://warehouse/wh/${db_name}/test_iceberg_systable_tbl1/data/*.parquet",
            "s3.endpoint"="http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "format" = "parquet",
            "use_path_style" = "true"
        )
    """
}
