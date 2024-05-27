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

suite("test_string_dict_filter", "p0,external,hive,external_docker,external_docker_hive") {
    def q_parquet = {
        qt_q01 """
        select * from test_string_dict_filter_parquet where o_orderstatus = 'F';
        """
        qt_q02 """
        select * from test_string_dict_filter_parquet where o_orderstatus = 'O';
        """
        qt_q03 """
        select * from test_string_dict_filter_parquet where o_orderstatus in ('O', 'F');
        """
        qt_q04 """
        select * from test_string_dict_filter_parquet where o_orderpriority is null;
        """
        qt_q05 """
        select * from test_string_dict_filter_parquet where o_orderpriority is not null;
        """
        qt_q06 """
        select * from test_string_dict_filter_parquet where o_orderpriority in ('5-LOW', NULL);
        """
        qt_q07 """
        select * from test_string_dict_filter_parquet where o_orderpriority in ('5-LOW') and o_orderstatus in ('O', 'F');
        """
        qt_q08 """
        select * from test_string_dict_filter_parquet where o_orderpriority in ('1-URGENT') and o_orderstatus in ('F');
        """
        qt_q09 """
        select * from test_string_dict_filter_parquet where o_orderpriority in ('1-URGENT') or o_orderstatus in ('F');
        """
        qt_q10 """
        select * from ( select IF(o_orderpriority IS NULL, 'null', o_orderpriority) AS o_orderpriority from test_string_dict_filter_parquet ) as A where o_orderpriority = 'null';
        """
        qt_q11 """
        select * from ( select IF(o_orderpriority IS NOT NULL, o_orderpriority, 'null') AS o_orderpriority from test_string_dict_filter_parquet ) as A where o_orderpriority = 'null';
        """
        qt_q12 """
        select * from ( select IFNULL(o_orderpriority, 'null') AS o_orderpriority from test_string_dict_filter_parquet ) as A where o_orderpriority = 'null';
        """
        qt_q13 """
        select * from ( select IFNULL(o_orderpriority, 'null') AS o_orderpriority from test_string_dict_filter_parquet ) as A where o_orderpriority = 'null';
        """
        qt_q14 """
        select * from ( select COALESCE(o_orderpriority, 'null') AS o_orderpriority from test_string_dict_filter_parquet ) as A where o_orderpriority = 'null';
        """
    }
    def q_orc = {
        qt_q01 """
        select * from test_string_dict_filter_orc where o_orderstatus = 'F';
        """
        qt_q02 """
        select * from test_string_dict_filter_orc where o_orderstatus = 'O';
        """
        qt_q03 """
        select * from test_string_dict_filter_orc where o_orderstatus in ('O', 'F');
        """
        qt_q04 """
        select * from test_string_dict_filter_orc where o_orderpriority is null;
        """
        qt_q05 """
        select * from test_string_dict_filter_orc where o_orderpriority is not null;
        """
        qt_q06 """
        select * from test_string_dict_filter_orc where o_orderpriority in ('5-LOW', NULL);
        """
        qt_q07 """
        select * from test_string_dict_filter_orc where o_orderpriority in ('5-LOW') and o_orderstatus in ('O', 'F');
        """
        qt_q08 """
        select * from test_string_dict_filter_orc where o_orderpriority in ('1-URGENT') and o_orderstatus in ('F');
        """
        qt_q09 """
        select * from test_string_dict_filter_orc where o_orderpriority in ('1-URGENT') or o_orderstatus in ('F');
        """
        qt_q10 """
        select * from ( select IF(o_orderpriority IS NULL, 'null', o_orderpriority) AS o_orderpriority from test_string_dict_filter_orc ) as A where o_orderpriority = 'null';
        """
        qt_q11 """
        select * from ( select IF(o_orderpriority IS NOT NULL, o_orderpriority, 'null') AS o_orderpriority from test_string_dict_filter_orc ) as A where o_orderpriority = 'null';
        """
        qt_q12 """
        select * from ( select IFNULL(o_orderpriority, 'null') AS o_orderpriority from test_string_dict_filter_orc ) as A where o_orderpriority = 'null';
        """
        qt_q13 """
        select * from ( select IFNULL(o_orderpriority, 'null') AS o_orderpriority from test_string_dict_filter_orc ) as A where o_orderpriority = 'null';
        """
        qt_q14 """
        select * from ( select COALESCE(o_orderpriority, 'null') AS o_orderpriority from test_string_dict_filter_orc ) as A where o_orderpriority = 'null';
        """
    }
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "test_string_dict_filter_${hivePrefix}"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""

            q_parquet()
            q_orc()

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

