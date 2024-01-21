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

suite("paimon_base_filesystem", "p2,external,paimon,external_remote,external_remote_paimon") {
    String enabled = context.config.otherConfigs.get("enableExternalPaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_cos = "paimon_cos"
        String catalog_oss = "paimon_oss"
        String ak = context.config.otherConfigs.get("aliYunAk")
        String sk = context.config.otherConfigs.get("aliYunSk")

        String s3ak = getS3AK()
        String s3sk = getS3SK()

        def cos = """select c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c18 from ${catalog_cos}.zd.all_table order by c18"""
        def oss = """select * from ${catalog_oss}.paimonossdb1.test_tableoss order by a"""

        sql """drop catalog if exists ${catalog_cos};"""
        sql """drop catalog if exists ${catalog_oss};"""
        sql """
            create catalog if not exists ${catalog_cos} properties (
                "type" = "paimon",
                "warehouse" = "cosn://paimon-1308700295/paimoncos",
                "cos.access_key" = "${s3ak}",
                "cos.secret_key" = "${s3sk}",
                "cos.endpoint" = "cos.ap-beijing.myqcloud.com"
            );
        """
        sql """
            create catalog if not exists ${catalog_oss} properties (
                "type" = "paimon",
                "warehouse" = "oss://paimon-zd/paimonoss",
                "oss.endpoint"="oss-cn-beijing.aliyuncs.com",
                "oss.access_key"="${ak}",
                "oss.secret_key"="${sk}"
            );
        """
        logger.info("catalog " + catalog_cos + " created")
        logger.info("catalog " + catalog_oss + " created")

        sql """set force_jni_scanner=false"""
        qt_c1 cos
        qt_c2 oss

        sql """set force_jni_scanner=true"""
        qt_c3 cos
        qt_c4 oss

        sql """set force_jni_scanner=false"""
    }
}

