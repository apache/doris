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

suite("iceberg_query_tag_branch", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_query_tag_branch"

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


    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """ use test_db;""" 

    qt_branch_1  """ select * from tag_branch_table@branch(b1) order by c1;""" 
    qt_branch_2  """ select * from tag_branch_table@branch(b2) order by c1 ;""" 
    qt_branch_3  """ select * from tag_branch_table@branch(b3) order by c1 ;""" 
    qt_branch_4  """ select * from tag_branch_table@branch('name'='b1') order by c1 ;""" 
    qt_branch_5  """ select * from tag_branch_table@branch('name'='b2') order by c1 ;""" 
    qt_branch_6  """ select * from tag_branch_table@branch('name'='b3') order by c1 ;""" 

    qt_tag_1  """ select * from tag_branch_table@tag(t1) order by c1 ;""" 
    qt_tag_2  """ select * from tag_branch_table@tag(t2) order by c1 ;""" 
    qt_tag_3  """ select * from tag_branch_table@tag(t3) order by c1 ;""" 
    qt_tag_4  """ select * from tag_branch_table@tag('name'='t1') order by c1 ;""" 
    qt_tag_5  """ select * from tag_branch_table@tag('name'='t2') order by c1 ;""" 
    qt_tag_6  """ select * from tag_branch_table@tag('name'='t3') order by c1 ;""" 

    qt_version_1  """ select * from tag_branch_table for version as of 'b1' order by c1 ;""" 
    qt_version_2  """ select * from tag_branch_table for version as of 'b2' order by c1 ;""" 
    qt_version_3  """ select * from tag_branch_table for version as of 'b3' order by c1 ;""" 
    qt_version_4  """ select * from tag_branch_table for version as of 't1' order by c1 ;""" 
    qt_version_5  """ select * from tag_branch_table for version as of 't2' order by c1;""" 
    qt_version_6  """ select * from tag_branch_table for version as of 't3' order by c1 ;""" 

}
