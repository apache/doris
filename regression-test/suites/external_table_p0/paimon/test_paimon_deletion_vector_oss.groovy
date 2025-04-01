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

suite("test_paimon_deletion_vector_oss", "p0,external,doris,external_docker,external_docker_doris") {

    logger.info("start paimon test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    try {
        String catalog_name = "test_paimon_deletion_vector_oss"
        String aliYunAk = context.config.otherConfigs.get("aliYunAk")
        String aliYunSk = context.config.otherConfigs.get("aliYunSk")
        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type" = "paimon",
            "paimon.catalog.type"="filesystem",
            "warehouse" = "oss://doris-regression-bj/regression/paimon1",
            "oss.access_key"="${aliYunAk}",
            "oss.secret_key"="${aliYunSk}",
            "oss.endpoint"="oss-cn-beijing.aliyuncs.com"
        );"""

        sql """use `${catalog_name}`.`db1`"""

        def test_cases = { String force ->
            sql """ set force_jni_scanner=${force} """
            qt_1 """select count(*) from deletion_vector_orc;"""
            qt_2 """select count(*) from deletion_vector_parquet;"""
            qt_3 """select count(*) from deletion_vector_orc where id > 2;"""
            qt_4 """select count(*) from deletion_vector_parquet where id > 2;"""
            qt_5 """select * from deletion_vector_orc where id > 2 order by id;"""
            qt_6 """select * from deletion_vector_parquet where id > 2 order by id;"""
        }

        test_cases("false")
        test_cases("true")

    } finally {
        sql """set force_jni_scanner=false"""
    }

}