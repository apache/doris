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

suite("test_routine_load","p0") {

    def tables = [
                  "dup_tbl_basic",
                  "uniq_tbl_basic",
                  "mow_tbl_basic",
                  "agg_tbl_basic",
                  "dup_tbl_array",
                  "uniq_tbl_array",
                  "mow_tbl_array",
                 ]

    def multiTables = [
                  "dup_tbl_basic_multi_table",
                 ]

    def jobs =   [
                  "dup_tbl_basic_job",
                  "uniq_tbl_basic_job",
                  "mow_tbl_basic_job",
                  "agg_tbl_basic_job",
                  "dup_tbl_array_job",
                  "uniq_tbl_array_job",
                  "mow_tbl_array_job",
                 ]

    def topics = [
                  "basic_data",
                  "basic_data",
                  "basic_data",
                  "basic_data",
                  "basic_array_data",
                  "basic_array_data",
                  "basic_array_data",
                 ]

    def errorTopics = [
                  "basic_data_with_errors",
                  "basic_data_with_errors",
                  "basic_data_with_errors",
                  "basic_data_with_errors",
                  "basic_array_data_with_errors",
                  "basic_array_data_with_errors",
                  "basic_array_data_with_errors",
                 ]

    def timezoneTopics = [
                  "basic_data_timezone",
                  "basic_data_timezone",
                  "basic_data_timezone",
                  "basic_data_timezone",
                  "basic_array_data_timezone",
                  "basic_array_data_timezone",
                  "basic_array_data_timezone",
                 ]

    def jsonArrayTopic = [
                 "basic_data_json",
                 "basic_data_json",
                 "basic_data_json",
                 "basic_data_json",
                 "basic_array_data_json",
                 "basic_array_data_json",
                 "basic_array_data_json",
                ]

    def jsonTopic = [
                 "basic_data_json_by_line",
                 "basic_data_json_by_line",
                 "basic_data_json_by_line",
                 "basic_data_json_by_line",
                 "basic_array_data_json_by_line",
                 "basic_array_data_json_by_line",
                 "basic_array_data_json_by_line",
                ]

    def jsonpaths = [
                    '[\"$.k00\", \"$.k01\", \"$.k02\", \"$.k03\", \"$.k04\", \"$.k05\", \"$.k06\", \"$.k07\", \"$.k08\", \"$.k09\", \"$.k10\", \"$.k11\", \"$.k12\", \"$.k13\", \"$.k14\", \"$.k15\", \"$.k16\", \"$.k17\", \"$.k18\"]',
                    '[\"$.k00\", \"$.k01\", \"$.k02\", \"$.k03\", \"$.k04\", \"$.k05\", \"$.k06\", \"$.k07\", \"$.k08\", \"$.k09\", \"$.k10\", \"$.k11\", \"$.k12\", \"$.k13\", \"$.k14\", \"$.k15\", \"$.k16\", \"$.k17\", \"$.k18\"]',
                    '[\"$.k00\", \"$.k01\", \"$.k02\", \"$.k03\", \"$.k04\", \"$.k05\", \"$.k06\", \"$.k07\", \"$.k08\", \"$.k09\", \"$.k10\", \"$.k11\", \"$.k12\", \"$.k13\", \"$.k14\", \"$.k15\", \"$.k16\", \"$.k17\", \"$.k18\"]',
                    '[\"$.k00\", \"$.k01\", \"$.k02\", \"$.k03\", \"$.k04\", \"$.k05\", \"$.k06\", \"$.k07\", \"$.k08\", \"$.k09\", \"$.k10\", \"$.k11\", \"$.k12\", \"$.k13\", \"$.k14\", \"$.k15\", \"$.k16\", \"$.k17\", \"$.k18\"]',
                    '[\"$.k00\", \"$.k01\", \"$.k02\", \"$.k03\", \"$.k04\", \"$.k05\", \"$.k06\", \"$.k07\", \"$.k08\", \"$.k09\", \"$.k10\", \"$.k11\", \"$.k12\", \"$.k13\", \"$.k14\", \"$.k15\", \"$.k16\", \"$.k17\"]',
                    '[\"$.k00\", \"$.k01\", \"$.k02\", \"$.k03\", \"$.k04\", \"$.k05\", \"$.k06\", \"$.k07\", \"$.k08\", \"$.k09\", \"$.k10\", \"$.k11\", \"$.k12\", \"$.k13\", \"$.k14\", \"$.k15\", \"$.k16\", \"$.k17\"]',
                    '[\"$.k00\", \"$.k01\", \"$.k02\", \"$.k03\", \"$.k04\", \"$.k05\", \"$.k06\", \"$.k07\", \"$.k08\", \"$.k09\", \"$.k10\", \"$.k11\", \"$.k12\", \"$.k13\", \"$.k14\", \"$.k15\", \"$.k16\", \"$.k17\"]',
                    ]

    def columns = [ 
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                  ]

    def timezoneColumns = 
                  [ 
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00=unix_timestamp('2007-11-30 10:30:19'),k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                  ]

    def multiTableJobName = [
                    "multi_table_one_data_csv",
                    "multi_table_one_data_json",
                  ]

    def formats = [
                    "csv",
                    "json",
                  ]            

    def loadedRows = [0,0,0,0,17,17,17]

    def filteredRows = [20,20,20,20,3,3,3]

    def maxFilterRatio = [1,1,1,1,0.15,0.15,0.15]

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // exec_mem_limit
    def i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "exec_mem_limit" = "5",
                        "max_batch_interval" = "5",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${topics[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state == "NEED_SCHEDULE") {
                        continue;
                    }
                    log.info("reason of state changed: ${res[0][17].toString()}".toString())
                    assertEquals(res[0][8].toString(), "RUNNING")
                    break;
                }

                def count = 0
                def tableName1 =  "routine_load_" + tableName
                while (true) {
                    def res = sql "select count(*) from ${tableName1}"
                    def state = sql "show routine load for ${jobs[i]}"
                    log.info("routine load state: ${state[0][8].toString()}".toString())
                    log.info("routine load statistic: ${state[0][14].toString()}".toString())
                    log.info("reason of state changed: ${state[0][17].toString()}".toString())
                    if (res[0][0] > 0) {
                        break
                    }
                    if (count >= 120) {
                        log.error("routine load can not visible for long time")
                        assertEquals(20, res[0][0])
                        break
                    }
                    sleep(5000)
                    count++
                }
                
                if (i <= 3) {
                    qt_sql_exec_mem_limit "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_exec_mem_limit "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // timezone
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "timezone" = "Asia/Shanghai",
                        "max_batch_interval" = "5",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${topics[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state == "NEED_SCHEDULE") {
                        continue;
                    }
                    log.info("reason of state changed: ${res[0][17].toString()}".toString())
                    assertEquals(res[0][8].toString(), "RUNNING")
                    break;
                }

                def count = 0
                def tableName1 =  "routine_load_" + tableName
                while (true) {
                    def res = sql "select count(*) from ${tableName1}"
                    def state = sql "show routine load for ${jobs[i]}"
                    log.info("routine load state: ${state[0][8].toString()}".toString())
                    log.info("routine load statistic: ${state[0][14].toString()}".toString())
                    log.info("reason of state changed: ${state[0][17].toString()}".toString())
                    if (res[0][0] > 0) {
                        break
                    }
                    if (count >= 120) {
                        log.error("routine load can not visible for long time")
                        assertEquals(20, res[0][0])
                        break
                    }
                    sleep(5000)
                    count++
                }

                if (i <= 3) {
                    qt_sql_timezone_shanghai "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_timezone_shanghai "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    //strict_mode
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "strict_mode" = "true",
                        "max_batch_interval" = "5",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${errorTopics[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state != "NEED_SCHEDULE") {
                        break;
                    }
                }

                def count = 0
                def tableName1 =  "routine_load_" + tableName
                while (true) {
                    def res = sql "select count(*) from ${tableName1}"
                    def state = sql "show routine load for ${jobs[i]}"
                    log.info("routine load state: ${state[0][8].toString()}".toString())
                    log.info("routine load statistic: ${state[0][14].toString()}".toString())
                    log.info("reason of state changed: ${state[0][17].toString()}".toString())
                    if (res[0][0] > 0) {
                        break
                    }
                    if (count >= 120) {
                        log.error("routine load can not visible for long time")
                        assertEquals(20, res[0][0])
                        break
                    }
                    sleep(5000)
                    count++
                }

                if (i <= 3) {
                    qt_sql_strict_mode "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_strict_mode "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // max_error_number
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "max_error_number" = "${filteredRows[i]}",
                        "max_batch_interval" = "5",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${errorTopics[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state == "NEED_SCHEDULE") {
                        continue;
                    }
                    log.info("reason of state changed: ${res[0][17].toString()}".toString())
                    assertEquals(res[0][8].toString(), "RUNNING")
                    break;
                }

                def count = 0
                def tableName1 =  "routine_load_" + tableName
                while (true) {
                    def res = sql "select count(*) from ${tableName1}"
                    def state = sql "show routine load for ${jobs[i]}"
                    log.info("routine load state: ${state[0][8].toString()}".toString())
                    log.info("routine load statistic: ${state[0][14].toString()}".toString())
                    log.info("reason of state changed: ${state[0][17].toString()}".toString())
                    if (res[0][0] > 0) {
                        break
                    }
                    if (count >= 120) {
                        log.error("routine load can not visible for long time")
                        assertEquals(20, res[0][0])
                        break
                    }
                    sleep(5000)
                    count++
                }

                if (i <= 3) {
                    qt_sql_max_error_number "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_max_error_number "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // max_filter_ratio
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "max_filter_ratio" = "${maxFilterRatio[i]}",
                        "max_error_number" = "1000",
                        "max_batch_interval" = "5",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${errorTopics[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state == "NEED_SCHEDULE") {
                        continue;
                    }
                    log.info("reason of state changed: ${res[0][17].toString()}".toString())
                    assertEquals(res[0][8].toString(), "RUNNING")
                    break;
                }

                def count = 0
                def tableName1 =  "routine_load_" + tableName
                while (true) {
                    def res = sql "select count(*) from ${tableName1}"
                    def state = sql "show routine load for ${jobs[i]}"
                    log.info("routine load state: ${state[0][8].toString()}".toString())
                    log.info("routine load statistic: ${state[0][14].toString()}".toString())
                    log.info("reason of state changed: ${state[0][17].toString()}".toString())
                    if (res[0][0] > 0) {
                        break
                    }
                    if (count >= 120) {
                        log.error("routine load can not visible for long time")
                        assertEquals(20, res[0][0])
                        break
                    }
                    sleep(5000)
                    count++
                }
                
                if (i <= 3) {
                    qt_sql_max_filter_ratio "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_max_filter_ratio "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // load_to_single_tablet
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "load_to_single_tablet" = "true",
                        "max_batch_interval" = "1",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${topics[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state != "NEED_SCHEDULE") {
                        break;
                    }
                }

                def tableName1 =  "routine_load_" + tableName
                if (i <= 3) {
                    qt_sql_load_to_single_tablet "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_load_to_single_tablet "select * from ${tableName1} order by k00"
                }
                
                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // column_separator
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]})
                    PROPERTIES
                    (
                        "max_batch_interval" = "1",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${topics[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state != "NEED_SCHEDULE") {
                        break;
                    }
                }

                sleep(10000)
                def tableName1 =  "routine_load_" + tableName
                if (i <= 3) {
                    qt_sql_column_separator "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_column_separator "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // json
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]})
                    PROPERTIES
                    (
                        "format" = "json",
                        "max_batch_interval" = "5",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${jsonTopic[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state == "NEED_SCHEDULE") {
                        continue;
                    }
                    log.info("reason of state changed: ${res[0][17].toString()}".toString())
                    assertEquals(res[0][8].toString(), "RUNNING")
                    break;
                }

                def count = 0
                def tableName1 =  "routine_load_" + tableName
                while (true) {
                    def res = sql "select count(*) from ${tableName1}"
                    def state = sql "show routine load for ${jobs[i]}"
                    log.info("routine load state: ${state[0][8].toString()}".toString())
                    log.info("routine load statistic: ${state[0][14].toString()}".toString())
                    log.info("reason of state changed: ${state[0][17].toString()}".toString())
                    if (res[0][0] > 0) {
                        break
                    }
                    if (count >= 120) {
                        log.error("routine load can not visible for long time")
                        assertEquals(20, res[0][0])
                        break
                    }
                    sleep(5000)
                    count++
                }
                if (i <= 3) {
                    qt_sql_json "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_json "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]})
                    PROPERTIES
                    (
                        "format" = "json",
                        "jsonpaths" = '${jsonpaths[i]}',
                        "max_batch_interval" = "5",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${jsonTopic[i]}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"
                i++
            }

            i = 0
            for (String tableName in tables) {
                while (true) {
                    sleep(1000)
                    def res = sql "show routine load for ${jobs[i]}"
                    def state = res[0][8].toString()
                    if (state == "NEED_SCHEDULE") {
                        continue;
                    }
                    log.info("reason of state changed: ${res[0][17].toString()}".toString())
                    assertEquals(res[0][8].toString(), "RUNNING")
                    break;
                }

                def count = 0
                def tableName1 =  "routine_load_" + tableName
                while (true) {
                    def res = sql "select count(*) from ${tableName1}"
                    def state = sql "show routine load for ${jobs[i]}"
                    log.info("routine load state: ${state[0][8].toString()}".toString())
                    log.info("routine load statistic: ${state[0][14].toString()}".toString())
                    log.info("reason of state changed: ${state[0][17].toString()}".toString())
                    if (res[0][0] > 0) {
                        break
                    }
                    if (count >= 120) {
                        log.error("routine load can not visible for long time")
                        assertEquals(20, res[0][0])
                        break
                    }
                    sleep(5000)
                    count++
                }
                if (i <= 3) {
                    qt_sql_json_jsonpath "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_json_jsonpath "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // TODO: need update kafka script
    // i = 0
    // if (enabled != null && enabled.equalsIgnoreCase("true")) {
    //     try {
    //         for (String tableName in tables) {
    //             sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    //             sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

    //             def name = "routine_load_" + tableName
    //             sql """
    //                 CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
    //                 COLUMNS(${columns[i]})
    //                 PROPERTIES
    //                 (
    //                     "format" = "json",
    //                     "strip_outer_array" = "true",
    //                     "fuzzy_parse" = "true",
    //                     "max_batch_interval" = "5",
    //                     "max_batch_rows" = "300000",
    //                     "max_batch_size" = "209715200"
    //                 )
    //                 FROM KAFKA
    //                 (
    //                     "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
    //                     "kafka_topic" = "${jsonArrayTopic[i]}",
    //                     "property.kafka_default_offsets" = "OFFSET_BEGINNING"
    //                 );
    //             """
    //             sql "sync"
    //             i++
    //         }

    //         i = 0
    //         for (String tableName in tables) {
    //             while (true) {
    //                 sleep(1000)
    //                 def res = sql "show routine load for ${jobs[i]}"
    //                 def state = res[0][8].toString()
    //                 if (state == "NEED_SCHEDULE") {
    //                     continue;
    //                 }
    //                 log.info("reason of state changed: ${res[0][17].toString()}".toString())
    //                 assertEquals(res[0][8].toString(), "RUNNING")
    //                 break;
    //             }

    //             def count = 0
    //             def tableName1 =  "routine_load_" + tableName
    //             while (true) {
    //                 def res = sql "select count(*) from ${tableName1}"
    //                 def state = sql "show routine load for ${jobs[i]}"
    //                 log.info("routine load state: ${state[0][8].toString()}".toString())
    //                 log.info("routine load statistic: ${state[0][14].toString()}".toString())
    //                 log.info("reason of state changed: ${state[0][17].toString()}".toString())
    //                 if (res[0][0] > 0) {
    //                     break
    //                 }
    //                 if (count >= 120) {
    //                     log.error("routine load can not visible for long time")
    //                     assertEquals(20, res[0][0])
    //                     break
    //                 }
    //                 sleep(5000)
    //                 count++
    //             }
    //             if (i <= 3) {
    //                 qt_sql_json_strip_outer_array "select * from ${tableName1} order by k00,k01"
    //             } else {
    //                 qt_sql_json_strip_outer_array "select * from ${tableName1} order by k00"
    //             }

    //             sql "stop routine load for ${jobs[i]}"
    //             i++
    //         }
    //     } finally {
    //         for (String tableName in tables) {
    //             sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
    //         }
    //     }
    // }

    // multi_table
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def j = 0
        for (String jobName in multiTableJobName) {
            try {
                for (String tableName in multiTables) {
                    sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                    sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
                }

                sql """
                    CREATE ROUTINE LOAD ${jobName}
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "max_batch_interval" = "5",
                        "format" = "${formats[j]}",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${jobName}",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """
                sql "sync"

                i = 0
                for (String tableName in multiTables) {
                    while (true) {
                        sleep(1000)
                        def res = sql "show routine load for ${jobName}"
                        def state = res[0][8].toString()
                        if (state == "NEED_SCHEDULE") {
                            continue;
                        }
                        assertEquals(res[0][8].toString(), "RUNNING")
                        break;
                    }

                    def count = 0
                    def tableName1 =  "routine_load_" + tableName
                    while (true) {
                        def res = sql "select count(*) from ${tableName1}"
                        def state = sql "show routine load for ${jobName}"
                        log.info("routine load state: ${state[0][8].toString()}".toString())
                        log.info("routine load statistic: ${state[0][14].toString()}".toString())
                        log.info("reason of state changed: ${state[0][17].toString()}".toString())
                        if (res[0][0] > 0) {
                            break
                        }
                        if (count >= 120) {
                            log.error("routine load can not visible for long time")
                            assertEquals(20, res[0][0])
                            break
                        }
                        sleep(5000)
                        count++
                    }
                    
                    if (i <= 3) {
                        qt_sql_multi_table_one_data "select * from ${tableName1} order by k00,k01"
                    } else {
                        qt_sql_multi_table_one_data "select * from ${tableName1} order by k00"
                    }
                    
                    i++
                }
            } finally {
                sql "stop routine load for ${jobName}"
                for (String tableName in multiTables) {
                    sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                }
            }
            j++
        }
    }
}
