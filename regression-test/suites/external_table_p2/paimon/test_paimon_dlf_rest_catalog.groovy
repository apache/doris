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

suite("test_paimon_dlf_rest_catalog", "p2,external,paimon,external_remote,external_remote_paimon,new_catalog_property") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    try {
        String catalog = "test_paimon_dlf_rest_catalog"
        String prop= context.config.otherConfigs.get("paimonDlfRestCatalog")

        sql """drop catalog if exists ${catalog};"""
        sql """
            create catalog if not exists ${catalog} properties (
                ${prop}
            );
        """

        sql """ use ${catalog}.new_dlf_paimon_db"""

        sql """set force_jni_scanner=false"""
        qt_c1 """ select * from users_samples order by user_id """
        sql """select * from users_samples\$files;"""

        sql """set force_jni_scanner=true"""
        qt_c1 """ select * from users_samples order by user_id """
        sql """select * from users_samples\$files;"""

    } finally {
        sql """set force_jni_scanner=false"""
    }
}

