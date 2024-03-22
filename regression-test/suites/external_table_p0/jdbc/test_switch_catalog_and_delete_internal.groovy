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

suite("test_switch_catalog_and_delete_internal") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // 0.create internal db and table
        String db = context.config.getDbNameByFile(new File(context.file))
        sql "drop table if exists test_switch_catalog_and_delete_internal"
        sql """
        create table test_switch_catalog_and_delete_internal(pk int, a int, b int) distributed by hash(pk) buckets 10
        properties('replication_num' = '1'); 
        """

        sql """
        insert into test_switch_catalog_and_delete_internal values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6);
        """
        // 1.create catalog
        String catalog_name = "test_switch_catalog_and_delete_internal_catalog"
        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""
        // 2.switch catalog/ refresh
        sql "switch test_switch_catalog_and_delete_internal_catalog"
        sql "refresh catalog test_switch_catalog_and_delete_internal_catalog"
        // 3.delete table
        sql "delete from internal.${db}.test_switch_catalog_and_delete_internal;"
        // 4.select table
        qt_test "select * from internal.maldb.test_switch_catalog_and_delete_internal;"
    }

}