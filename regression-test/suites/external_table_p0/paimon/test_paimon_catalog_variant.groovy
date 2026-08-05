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

suite("test_paimon_catalog_variant", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalogName = "test_paimon_variant"

        sql """drop catalog if exists ${catalogName}"""
        sql """create catalog if not exists ${catalogName} properties (
                "type" = "paimon",
                "paimon.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/wh",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "s3.path.style.access" = "true"
            );"""
        sql """use `${catalogName}`.`test_paimon_spark`"""
        sql """set force_jni_scanner = true"""

        explain {
            sql "select * from variant_smoke order by id"
            contains "paimonNativeReadSplits=0/1"
        }

        order_qt_desc """desc variant_smoke"""

        order_qt_full_variant """
            select id, payload
            from variant_smoke
            order by id
        """

        order_qt_object_subpaths """
            select id,
                   cast(payload['name'] as string),
                   cast(payload['age'] as int),
                   cast(payload['profile']['city'] as string),
                   cast(payload['active'] as boolean)
            from variant_smoke
            order by id
        """

        order_qt_null_and_missing """
            select id,
                   payload['missing'] is null,
                   payload['not_exist'] is null
            from variant_smoke
            order by id
        """

        order_qt_root_array """
            select id,
                   cast(payload[1] as int),
                   cast(payload[2] as string),
                   cast(payload[3] as boolean),
                   cast(payload[4] as string),
                   cast(payload[5]['k'] as string)
            from variant_smoke
            where id = 3
            order by id
        """

        order_qt_subpath_predicate """
            select id, cast(payload['name'] as string)
            from variant_smoke
            where cast(payload['age'] as int) >= 20
            order by id
        """

        sql """set force_jni_scanner = false"""
    }
}
