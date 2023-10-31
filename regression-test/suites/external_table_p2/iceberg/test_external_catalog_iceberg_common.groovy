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

suite("test_external_catalog_iceberg_common", "p2,external,iceberg,external_remote,external_remote_iceberg") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_external_catalog_iceberg_partition"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """switch ${catalog_name};"""
        // test parquet format
        def q01_parquet = {
            qt_q01 """ SELECT COUNT(*) FROM (
                        SELECT l_returnflag, l_quantity, l_partkey, l_suppkey, l_discount, l_tax,
                            case
                                when l_tax <= 0.15 then '低频'
                                when l_tax <= 0.85 then '中频'
                                else '高频'
                            end
                            gr, cast(l_discount / 5 as int) * 5 as score_bins, l_comment from lineitem
                        ) as dc_1;
                    """
        }
        sql """ use `iceberg_catalog`; """
        q01_parquet()
    }
}
