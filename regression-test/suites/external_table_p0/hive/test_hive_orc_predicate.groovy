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

import groovy.json.JsonSlurper

suite("test_hive_orc_predicate", "p0,external") {
    def getProfileList = {
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfile = { id ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/api/profile/text/?query_id=$id").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfileWithToken = { token ->
        String profileId = ""
        int attempts = 0
        while (attempts < 10 && (profileId == null || profileId == "")) {
            List profileData = new JsonSlurper().parseText(getProfileList()).data.rows
            for (def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileId = profileItem["Profile ID"].toString()
                    break
                }
            }
            if (profileId == null || profileId == "") {
                Thread.sleep(300)
            }
            attempts++
        }
        assertTrue(profileId != null && profileId != "")
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }

    def extractProfileValue = { String profileText, String keyName ->
        def matcher = profileText =~ /(?m)^\s*-\s*${keyName}:\s*sum\s+(\S+),/
        return matcher.find() ? matcher.group(1).trim() : null
    }

    def assertOrcV2SargProfile = { String profileText ->
        assertTrue(profileText.contains("UseScannerV2: true"), "Profile does not use ScannerV2")
        assertTrue(profileText.contains("OrcReader"), "Profile does not contain OrcReader")
        assertTrue(extractProfileValue(profileText, "EvaluatedRowGroupCount") != null)
        assertTrue(extractProfileValue(profileText, "SelectedRowGroupCount") != null)
        assertTrue(extractProfileValue(profileText, "RowGroupsFilteredByMinMax") != null)
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    def normalizeRows = { rows ->
        rows.collect { row ->
            row.collect { value -> value == null ? null : value.toString() }
        }
    }
    def assertRepeatedRows = { expectedRow, rows ->
        def normalizedRows = normalizeRows(rows)
        assertEquals(10, normalizedRows.size())
        assertTrue(normalizedRows.every { it == expectedRow })
    }
    def assertProfiledRepeatedRows = { expectedRow, queryBody ->
        def profileToken = UUID.randomUUID().toString()
        try {
            sql """ set check_orc_init_sargs_success = true; """
            def rows = sql("""
                select * from (
                    ${queryBody}
                ) profile_query where '${profileToken}' = '${profileToken}';
            """)
            assertRepeatedRows(expectedRow, rows)
            assertOrcV2SargProfile(getProfileWithToken(profileToken))
        } finally {
            sql """ set check_orc_init_sargs_success = false; """
        }
    }
    def assertProfiledRows = { expectedRows, queryBody ->
        def profileToken = UUID.randomUUID().toString()
        try {
            sql """ set check_orc_init_sargs_success = true; """
            def rows = sql("""
                select * from (
                    ${queryBody}
                ) profile_query where '${profileToken}' = '${profileToken}';
            """)
            assertEquals(expectedRows, normalizeRows(rows))
            assertOrcV2SargProfile(getProfileWithToken(profileToken))
        } finally {
            sql """ set check_orc_init_sargs_success = false; """
        }
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_predicate"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """use `${catalog_name}`.`multi_catalog`"""
        sql """set enable_file_scanner_v2 = true;"""
        sql """set enable_orc_filter_by_min_max = true;"""
        sql """set profile_level=2;"""
        sql """set enable_profile=true;"""

        qt_predicate_fixed_char1 """ select * from fixed_char_table where c = 'a';"""
        qt_predicate_fixed_char2 """ select * from fixed_char_table where c = 'a ';"""

        qt_predicate_changed_type1 """ select * from type_changed_table where id = '1';"""
        qt_predicate_changed_type2 """ select * from type_changed_table where id = '2';"""
        qt_predicate_changed_type3 """ select * from type_changed_table where id = '3';"""

        qt_predicate_null_aware_equal_in_rt """select * from table_a inner join table_b on table_a.age <=> table_b.age and table_b.id in (1,3) order by table_a.id;"""

        assertProfiledRows([["1", null], ["3", null]], """
            select id, age from table_a where age <=> null order by id
        """)
        assertProfiledRows([["2", "18"]], """
            select id, age from table_a where age <=> 18 order by id
        """)

        sql """use `${catalog_name}`.`default`"""
        assertProfiledRepeatedRows(["3"], """
            select t_int from `${catalog_name}`.`default`.orc_all_types_t
            where t_int >= 3 and t_int < 4 order by t_int
        """)
        assertProfiledRepeatedRows(["3", "-1234567890.12345678"], """
            select t_int, t_decimal_precision_18
            from `${catalog_name}`.`default`.orc_all_types_t
            where t_decimal_precision_18 =
                    cast('-1234567890.12345678' as decimal(18,8))
            order by t_int
        """)
        assertProfiledRepeatedRows(["3", "-1234567890.12345678"], """
            select t_int, t_decimal_precision_18
            from `${catalog_name}`.`default`.orc_all_types_t
            where t_decimal_precision_18 >=
                    cast('-1234567890.12345678' as decimal(18,8))
              and t_decimal_precision_18 <
                    cast('-1234567890.12345677' as decimal(18,8))
            order by t_int
        """)
        assertProfiledRepeatedRows(["3", "2011-05-06"], """
            select t_int, t_date from `${catalog_name}`.`default`.orc_all_types_t
            where t_date = date '2011-05-06' order by t_int
        """)
        assertProfiledRepeatedRows(["3", "2011-05-06"], """
            select t_int, t_date from `${catalog_name}`.`default`.orc_all_types_t
            where t_date >= date '2011-05-06'
              and t_date < date '2011-05-07'
            order by t_int
        """)
        assertProfiledRepeatedRows(["3", "2011-05-06T07:08:09.123"], """
            select t_int, t_timestamp
            from `${catalog_name}`.`default`.orc_all_types_t
            where t_timestamp =
                    cast('2011-05-06 07:08:09.123' as datetime(6))
            order by t_int
        """)
        assertProfiledRepeatedRows(["3", "2011-05-06T07:08:09.123"], """
            select t_int, t_timestamp
            from `${catalog_name}`.`default`.orc_all_types_t
            where t_timestamp >=
                    cast('2011-05-06 07:08:09.123' as datetime(6))
              and t_timestamp <
                    cast('2011-05-06 07:08:10' as datetime(6))
            order by t_int
        """)

        sql """use `${catalog_name}`.`multi_catalog`"""
        qt_lazy_materialization_for_list_type """ select l from complex_data_orc where id > 2 order by id; """
        qt_lazy_materialization_for_map_type """ select m from complex_data_orc where id > 2 order by id; """
        qt_lazy_materialization_for_list_and_map_type """ select * from complex_data_orc where id > 2 order by id; """
        qt_lazy_materialization_for_list_type2 """select t_struct_nested from `${catalog_name}`.`default`.orc_all_types_t where t_int=3;"""

        sql """drop catalog if exists ${catalog_name}"""
    }
}
