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

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_routine_load_p2","p2,nonConcurrent") {

    sql "create workload group if not exists create_routine_load_group properties ( 'cpu_share'='123');"
    sql "create workload group if not exists alter_routine_load_group properties ( 'cpu_share'='123');"
    Thread.sleep(5000) // wait publish workload group to be

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

    def multiTables1 = [
                  "dup_tbl_basic",
                  "uniq_tbl_basic",
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

    def kafkaCsvTpoics = [
                  "basic_data",
                  "basic_array_data",
                  "basic_data_with_errors",
                  "basic_array_data_with_errors",
                  "basic_data_timezone",
                  "basic_array_data_timezone",
                  "multi_table_csv1",
                  "multi_table_csv",
                ]

    def kafkaJsonTopics = [
                  "basic_data_json",
                  "basic_array_data_json",
                  "basic_data_json_by_line",
                  "basic_array_data_json_by_line",
                  "multi_table_json",
                  "multi_table_json1",
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
                    "multi_table_csv",
                    "multi_table_json",
                  ]

    def multiTableJobName1 = [
                    "multi_table_csv1",
                    "multi_table_json1",
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
    def kafka_broker = "${externalEnvIp}:${kafka_port}"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // define kafka 
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        // Create kafka producer
        def producer = new KafkaProducer<>(props)

        for (String kafkaCsvTopic in kafkaCsvTpoics) {
            def txt = new File("""${context.file.parent}/data/${kafkaCsvTopic}.csv""").text
            def lines = txt.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record)
            }
        }
        for (String kafkaJsonTopic in kafkaJsonTopics) {
            def kafkaJson = new File("""${context.file.parent}/data/${kafkaJsonTopic}.json""").text
            def lines = kafkaJson.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaJsonTopic, null, line)
                producer.send(record)
            }
        }            

    }  

    // send_batch_parallelism
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
                        "send_batch_parallelism" = "2",
                        "max_batch_interval" = "5",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200",
                        "workload_group" = "create_routine_load_group"
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
                    qt_sql_send_batch_parallelism "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_send_batch_parallelism "select * from ${tableName1} order by k00"
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

    // desired_concurrent_number
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
                        "desired_concurrent_number" = "3",
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
                def res = sql "show routine load for ${jobs[i]}"
                log.info("routine load job properties: ${res[0][11].toString()}".toString())
                def json = parseJson(res[0][11])
                assertEquals("3", json.desired_concurrent_number.toString())
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
                    qt_sql_desired_concurrent_number "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_desired_concurrent_number "select * from ${tableName1} order by k00"
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
                try {
                    sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "desired_concurrent_number" = "x",
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
                }catch (Exception e) {
                    log.info("exception: ${e.toString()}".toString())
                    assertEquals(e.toString().contains("desired_concurrent_number must be greater than 0"), true)
                }
                sql "sync"
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
                try {
                    sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "desired_concurrent_number" = "-3",
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
                }catch (Exception e) {
                    log.info("exception: ${e.toString()}".toString())
                    assertEquals(e.toString().contains("desired_concurrent_number must be greater than 0"), true)
                }
                sql "sync"
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
                try {
                    sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "send_batch_parallelism" = "x",
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
                }catch (Exception e) {
                    log.info("exception: ${e.toString()}".toString())
                    assertEquals(e.toString().contains("send_batch_parallelism must be greater than 0"), true)
                }
                sql "sync"
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
                try {
                    sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "send_batch_parallelism" = "-1",
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
                }catch (Exception e) {
                    log.info("exception: ${e.toString()}".toString())
                    assertEquals(e.toString().contains("send_batch_parallelism must be greater than 0"), true)
                }
                sql "sync"
                i++
            }
        } finally {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // num_as_string
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
                        "format" = "json",
                        "num_as_string" = "true",
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
                    qt_sql_num_as_string "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_num_as_string "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            for (String tableName in tables) {
                //sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
            }
        }
    }

    // exec_mem_limit
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

    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                try {
                    sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|"
                    PROPERTIES
                    (
                        "exec_mem_limit" = "test",
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
                } catch (Exception e) {
                    log.info("exception: ${e.toString()}".toString())
                    assertEquals(e.toString(), "java.sql.SQLException: errCode = 2, detailMessage = exec_mem_limit must be greater than 0")
                }
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
                //sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
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

    // invalid format
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                try {
                    sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]})
                    PROPERTIES
                    (
                        "format" = "test",
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
                }catch (Exception e) {
                    log.info("create routine load failed: ${e.getMessage()}")
                    assertEquals(e.getMessage(), "errCode = 2, detailMessage = Format type is invalid. format=`test`")
                }
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

    // disable_simdjson_reader and load json
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        def set_be_param = { paramName, paramValue ->
            // for eache be node, set paramName=paramValue
            for (String id in backendId_to_backendIP.keySet()) {
                def beIp = backendId_to_backendIP.get(id)
                def bePort = backendId_to_backendHttpPort.get(id)
                def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
                assertTrue(out.contains("OK"))
            }
        }

        try {
            set_be_param.call("enable_simdjson_reader", "false")

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
                    qt_disable_simdjson_reader "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_disable_simdjson_reader "select * from ${tableName1} order by k00"
                }

                sql "stop routine load for ${jobs[i]}"
                i++
            }
        } finally {
            set_be_param.call("enable_simdjson_reader", "true")
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

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def j = 0
        for (String jobName in multiTableJobName1) {
            try {
                for (String tableName in multiTables1) {
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
                for (String tableName in multiTables1) {
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
                        qt_sql_multi_table "select * from ${tableName1} order by k00,k01"
                    } else {
                        qt_sql_multi_table "select * from ${tableName1} order by k00"
                    }

                    i++
                }
            } finally {
                sql "stop routine load for ${jobName}"
                for (String tableName in multiTables1) {
                    sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                }
            }
            j++
        }
    }

    // show command
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
                def res = sql "show routine load for ${jobs[i]}"
                log.info("routine load job properties: ${res[0][11].toString()}".toString())
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"desired_concurrent_number\" = \"1\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_error_number\" = \"1\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_batch_rows\" = \"300001\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_batch_size\" = \"209715201\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_batch_interval\" = \"6\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"max_filter_ratio\" = \"0.5\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"jsonpaths\" = \"jsonpaths\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"json_root\" = \"json_root\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"strip_outer_array\" = \"true\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"strict_mode\" = \"true\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"timezone\" = \"Asia/Shanghai\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"num_as_string\" = \"true\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"fuzzy_parse\" = \"true\");"
                sql "ALTER ROUTINE LOAD FOR ${jobs[i]} PROPERTIES(\"workload_group\" = \"alter_routine_load_group\");"
                res = sql "show routine load for ${jobs[i]}"
                log.info("routine load job properties: ${res[0][11].toString()}".toString())

                res = sql "show routine load for ${jobs[i]}"
                def json = parseJson(res[0][11])
                assertEquals("1", json.desired_concurrent_number.toString())
                assertEquals("1", json.max_error_number.toString())
                assertEquals("300001", json.max_batch_rows.toString())
                assertEquals("209715201", json.max_batch_size.toString())
                assertEquals("6", json.max_batch_interval.toString())
                //TODO(bug): Can not update
                //assertEquals("0.5", json.max_filter_ratio.toString())
                assertEquals("jsonpaths", json.jsonpaths.toString())
                assertEquals("json_root", json.json_root.toString())
                assertEquals("true", json.strict_mode.toString())
                assertEquals("true", json.strip_outer_array.toString())
                assertEquals("Asia/Shanghai", json.timezone.toString())
                assertEquals("true", json.num_as_string.toString())
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


//test PRECEDING FILTER condition
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
                    COLUMNS TERMINATED BY "|",
                    PRECEDING FILTER k00 = 8
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
                    qt_sql_preceding_filter "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_preceding_filter "select * from ${tableName1} order by k00"
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

    //test where condition
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
                    COLUMNS TERMINATED BY "|",
                    WHERE k00 = 8
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
                    qt_sql_where "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_where "select * from ${tableName1} order by k00"
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

    //test delete condition
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                if (i != 2){
                    i++
                    continue
                }
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    WITH MERGE
                    COLUMNS(${columns[i]}),
                    COLUMNS TERMINATED BY "|",
                    DELETE ON k00=8
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
                if (i != 2){
                    i++
                    continue
                }
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
                    qt_sql_delete "select * from ${tableName1} order by k00,k01"
                } else {
                    qt_sql_delete "select * from ${tableName1} order by k00"
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

    // sequence
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            sql new File("""${context.file.parent}/ddl/uniq_tbl_basic_drop_sequence.sql""").text
            sql new File("""${context.file.parent}/ddl/uniq_tbl_basic_create_sequence.sql""").text

            def name = "routine_load_uniq_tbl_basic_sequence"
            def job = "sequence_job"
            sql """
                CREATE ROUTINE LOAD ${job} ON ${name}
                COLUMNS(${columns[0]}),
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
                    "kafka_topic" = "${topics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"

            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${job}"
                def state = res[0][8].toString()
                if (state == "NEED_SCHEDULE") {
                    continue;
                }
                log.info("reason of state changed: ${res[0][17].toString()}".toString())
                assertEquals(res[0][8].toString(), "RUNNING")
                break;
            }

            def count = 0
            while (true) {
                def res = sql "select count(*) from ${name}"
                def state = sql "show routine load for ${job}"
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
            qt_sql_squence "select * from routine_load_uniq_tbl_basic_sequence order by k00,k01"
            sql "stop routine load for ${job}"
        } finally {
            sql new File("""${context.file.parent}/ddl/uniq_tbl_basic_drop_sequence.sql""").text
        }
    }

    // error command
    i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            for (String tableName in tables) {
                sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text

                def name = "routine_load_" + tableName
                sql """
                    CREATE ROUTINE LOAD ${jobs[i]} ON ${name}
                    COLUMNS(${columns[i]},k30),
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
                    if (state != "PAUSED") {
                        continue;
                    }
                    log.info("reason of state changed: ${res[0][17].toString()}".toString())
                    assertEquals(res[0][17].toString(), "ErrorReason{code=errCode = 102, msg='current error rows is more than max_error_number or the max_filter_ratio is more than the value set'}")
                    break;
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