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

suite("test_routine_load_command","p0") {
    def tables = [
                  "dup_tbl_basic",
                  "uniq_tbl_basic",
                  "mow_tbl_basic",
                  "agg_tbl_basic",
                  "dup_tbl_array",
                  "uniq_tbl_array",
                  "mow_tbl_array",
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

    def columns = [ 
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                    "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17",
                  ]

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // show command
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
                    qt_show_command "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_show_command "select * from ${tableName1} order by k00"
                }

                def res = sql "SHOW ROUTINE LOAD TASK WHERE JobName = \"${jobs[i]}\""
                log.info("routine load task DataSource: ${res[0][8].toString()}".toString())
                def json = parseJson(res[0][8])
                assertEquals("20", "${res[0][8][5]}".toString()+"${res[0][8][6]}".toString())

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // pause and resume command
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
                sql "pause routine load for ${jobs[i]}"
                sql "resume routine load for ${jobs[i]}"
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
                    qt_pause_and_resume_command "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_pause_and_resume_command "select * from ${tableName1} order by k00"
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

    // update command
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
                sql "pause routine load for ${jobs[i]}"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"desired_concurrent_number\" = \"1\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_error_number\" = \"1\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_batch_rows\" = \"300001\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_batch_size\" = \"209715201\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_batch_interval\" = \"6\");"
                //sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_filter_ratio\" = \"0.5\");"
                def res = sql "show routine load for ${jobs[i]}"
                def json = parseJson(res[0][11])
                assertEquals("1", json.desired_concurrent_number.toString())
                assertEquals("1", json.max_error_number.toString())
                assertEquals("300001", json.max_batch_rows.toString())
                assertEquals("209715201", json.max_batch_size.toString())
                assertEquals("6", json.max_batch_interval.toString())
                //assertEquals("0.5", json.max_filter_ratio.toString())
                sql "resume routine load for ${jobs[i]}"
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
                    qt_update_command "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_update_command "select * from ${tableName1} order by k00"
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
}