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

suite("test_hive_orc_predicate", "p0,external,hive,external_docker,external_docker_hive") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_predicate"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`multi_catalog`"""

            qt_predicate_fixed_char1 """ select * from fixed_char_table where c = 'a';"""
            qt_predicate_fixed_char2 """ select * from fixed_char_table where c = 'a ';"""

            qt_predicate_changed_type1 """ select * from type_changed_table where id = '1';"""
            qt_predicate_changed_type2 """ select * from type_changed_table where id = '2';"""
            qt_predicate_changed_type3 """ select * from type_changed_table where id = '3';"""

            qt_predicate_null_aware_equal_in_rt """select * from table_a inner join table_b on table_a.age <=> table_b.age and table_b.id in (1,3) order by table_a.id;"""

            // use check_orc_init_sargs_success to test full acid push down
            sql """use `${catalog_name}`.`default`"""
            if (hivePrefix == "hive3") {
                sql """ set check_orc_init_sargs_success = true; """
            }
            qt_predicate_full_acid_push_down """ select * from orc_full_acid_par where value = 'BB' order by id;"""
            sql """ set check_orc_init_sargs_success = false; """

            sql """use `${catalog_name}`.`multi_catalog`"""
            qt_lazy_materialization_for_list_type """ select l from complex_data_orc where id > 2 order by id; """
            qt_lazy_materialization_for_map_type """ select m from complex_data_orc where id > 2 order by id; """
            qt_lazy_materialization_for_list_and_map_type """ select * from complex_data_orc where id > 2 order by id; """
            qt_lazy_materialization_for_list_type2 """select t_struct_nested from `${catalog_name}`.`default`.orc_all_types_t where t_int=3;"""

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
            sql """ set check_orc_init_sargs_success = false; """
        }
    }
}
