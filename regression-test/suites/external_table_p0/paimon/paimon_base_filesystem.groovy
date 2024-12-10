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

suite("paimon_base_filesystem", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    try {
        String catalog_obs = "paimon_base_filesystem_paimon_obs"
        String catalog_oss = "paimon_base_filesystem_paimon_oss"
        String catalog_cos = "paimon_base_filesystem_paimon_cos"
        String catalog_cosn = "paimon_base_filesystem_paimon_cosn"
        String aliYunAk = context.config.otherConfigs.get("aliYunAk")
        String aliYunSk = context.config.otherConfigs.get("aliYunSk")
        String hwYunAk = context.config.otherConfigs.get("hwYunAk")
        String hwYunSk = context.config.otherConfigs.get("hwYunSk")
        String txYunAk = context.config.otherConfigs.get("txYunAk")
        String txYunSk = context.config.otherConfigs.get("txYunSk")

        def obs = """select * from ${catalog_obs}.paimon_obs_db.paimon_obs_table order by k"""
        def oss = """select * from ${catalog_oss}.paimon_oss_db.paimon_oss_table order by k"""
        def cos = """select * from ${catalog_cos}.paimon_cos_db.paimon_cos_table order by k"""
        def cosn = """select * from ${catalog_cosn}.paimon_cosn_db.paimon_cosn_table order by k"""


        sql """drop catalog if exists ${catalog_obs};"""
        sql """drop catalog if exists ${catalog_oss};"""
        sql """drop catalog if exists ${catalog_cos};"""
        sql """drop catalog if exists ${catalog_cosn};"""

        sql """
            create catalog if not exists ${catalog_cos} properties (
                "type" = "paimon",
                "warehouse" = "cos://doris-build-1308700295/regression/paimon",
                "cos.access_key" = "${txYunAk}",
                "cos.secret_key" = "${txYunSk}",
                "cos.endpoint" = "cos.ap-beijing.myqcloud.com"
            );
        """
        sql """
            create catalog if not exists ${catalog_cosn} properties (
                "type" = "paimon",
                "warehouse" = "cosn://doris-build-1308700295/regression/paimon",
                "cosn.access_key" = "${txYunAk}",
                "cosn.secret_key" = "${txYunSk}",
                "cosn.endpoint" = "cos.ap-beijing.myqcloud.com"
            );
        """
        sql """
            create catalog if not exists ${catalog_oss} properties (
                "type" = "paimon",
                "warehouse" = "oss://doris-regression-bj/regression/paimon",
                "oss.access_key"="${aliYunAk}",
                "oss.secret_key"="${aliYunSk}",
                "oss.endpoint"="oss-cn-beijing.aliyuncs.com"
            );
        """
        sql """
            create catalog if not exists ${catalog_obs} properties (
                "type" = "paimon",
                "warehouse" = "obs://doris-build/piamon",
                "obs.access_key"="${hwYunAk}",
                "obs.secret_key"="${hwYunSk}",
                "obs.endpoint"="obs.cn-north-4.myhuaweicloud.com"
            );
        """
        logger.info("catalog " + catalog_obs + " created")
        logger.info("catalog " + catalog_oss + " created")
        logger.info("catalog " + catalog_cos + " created")
        logger.info("catalog " + catalog_cosn + " created")

        sql """set force_jni_scanner=false"""
        qt_oss oss
        qt_obs obs
        qt_cos cos
        qt_cosn cosn

        sql """set force_jni_scanner=true"""
        qt_oss oss
        qt_obs obs
        qt_cos cos
        qt_cosn cosn

    } finally {
        sql """set force_jni_scanner=false"""
    }
}

