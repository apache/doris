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

suite("test_paimon_dlf_catalog", "p2,external,paimon,external_remote,external_remote_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    try {
        String catalog = "test_paimon_dlf_catalog"
        String uid = context.config.otherConfigs.get("dlf_uid")
        String region = context.config.otherConfigs.get("dlf_region")
        String catalog_id = context.config.otherConfigs.get("dlf_catalog_id")
        String access_key = context.config.otherConfigs.get("dlf_access_key")
        String secret_key = context.config.otherConfigs.get("dlf_secret_key")


        sql """drop catalog if exists ${catalog};"""
        sql """
            create catalog if not exists ${catalog} properties (
            "type" = "paimon",
            "paimon.catalog.type" = "dlf",
            "warehouse" = "oss://selectdb-qa-datalake-test/p2_regression_case",
            "dlf.proxy.mode" = "DLF_ONLY",
            "dlf.uid" = "${uid}",
            "dlf.region" = "${region}",
            "dlf.catalog.id" = "${catalog_id}",
            "dlf.access_key" = "${access_key}",
            "dlf.secret_key" = "${secret_key}"
            );
        """

        sql """ use ${catalog}.regression_paimon """

        sql """set force_jni_scanner=false"""
        qt_c1 """ select * from tb_simple order by id """
        sql """set force_jni_scanner=true"""
        qt_c2 """ select * from tb_simple order by id """

    } finally {
        sql """set force_jni_scanner=false"""
    }
}

