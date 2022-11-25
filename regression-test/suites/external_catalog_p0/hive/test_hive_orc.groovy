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

suite("test_hive_orc", "all_types") {
    // Ensure that all types are parsed correctly
    def select_top50 = {
        qt_select_top50 """select * from orc_all_types order by int_col desc limit 50;"""
    }

    // Ensure that the null map of all types are parsed correctly
    def count_all = {
        qt_count_all """
        select p1_col, p2_col,
        count(tinyint_col),
        count(smallint_col),
        count(int_col),
        count(bigint_col),
        count(boolean_col),
        count(float_col),
        count(double_col),
        count(string_col),
        count(binary_col),
        count(timestamp_col),
        count(decimal_col),
        count(char_col),
        count(varchar_col),
        count(date_col),
        sum(size(list_double_col)),
        sum(size(list_string_col))
        from orc_all_types group by p1_col, p2_col
        order by p1_col, p2_col;
        """
    }

    // Ensure that the SearchArgument works well: LG
    def search_lg_int = {
        qt_search_lg_int """select count(int_col) from orc_all_types where int_col > 999613702;"""
    }

    // Ensure that the SearchArgument works well: IN
    def search_in_int = {
        qt_search_in_int """select count(int_col) from orc_all_types where int_col in (999742610, 999613702);"""
    }

    // Ensure that the SearchArgument works well: MIX
    def search_mix = {
        qt_search_mix """select int_col, decimal_col, date_col from orc_all_types where int_col > 995328433 and decimal_col > 988850.7929 and date_col > date '2018-08-25';"""
    }

    // only partition column selected
    def only_partition_col = {
        qt_only_partition_col """select count(p1_col), count(p2_col) from orc_all_types;"""
    }

    def set_be_config = { flag ->
        String[][] backends = sql """ show backends; """
        assertTrue(backends.size() > 0)
        for (String[] backend in backends) {
            StringBuilder setConfigCommand = new StringBuilder();
            setConfigCommand.append("curl -X POST http://")
            setConfigCommand.append(backend[2])
            setConfigCommand.append(":")
            setConfigCommand.append(backend[5])
            setConfigCommand.append("/api/update_config?")
            String command1 = setConfigCommand.toString() + "enable_new_load_scan_node=$flag"
            logger.info(command1)
            String command2 = setConfigCommand.toString() + "enable_new_file_scanner=$flag"
            logger.info(command2)
            def process1 = command1.execute()
            int code = process1.waitFor()
            assertEquals(code, 0)
            def process2 = command2.execute()
            code = process1.waitFor()
            assertEquals(code, 0)
        }
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String hms_port = context.config.otherConfigs.get("hms_port")
            sql """admin set frontend config ("enable_multi_catalog" = "true")"""
            sql """admin set frontend config ("enable_new_load_scan_node" = "true");"""
            set_be_config.call('true')
            sql """drop catalog if exists hive"""
            sql """
            create catalog if not exists hive properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://127.0.0.1:${hms_port}'
            );
            """
            sql """use `hive`.`default`"""

            select_top50()
            count_all()
            search_lg_int()
            search_in_int()
            search_mix()
            only_partition_col()

        } finally {
            sql """ADMIN SET FRONTEND CONFIG ("enable_new_load_scan_node" = "false");"""
            set_be_config.call('false')
        }
    }
}
