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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.atomic.AtomicReference

suite("test_kafka_share_skew_benchmark", "kafka_share_benchmark,nonConcurrent") {
    def config = context.config.otherConfigs
    if (!config.get("enableKafkaShareBenchmark")?.toString()?.equalsIgnoreCase("true")) {
        logger.info("Skipping opt-in native Kafka Routine Load benchmark")
        return
    }
    assertTrue(RoutineLoadTestUtils.isKafkaTestEnabled(context), "enableKafkaTest is required")
    def kafkaHome = config.get("kafkaTrunkHome")
    assertTrue(kafkaHome != null, "kafkaTrunkHome is required")
    def output = new File(config.get("kafkaShareBenchmarkOutput")?.toString() ?:
            "/tmp/doris-kafka-share-${UUID.randomUUID()}.jsonl")
    assertFalse(output.exists(), "Use a new benchmark output file")
    assertTrue(output.absoluteFile.parentFile.isDirectory(), "Create the output directory first")
    def emit = { Map value ->
        output.append(JsonOutput.toJson(value) + "\n", "UTF-8")
        logger.info("kafka_share_benchmark ${JsonOutput.toJson(value)}")
    }
    int records = (config.get("kafkaShareBenchmarkRecords") ?: "100000") as int
    int payloadBytes = (config.get("kafkaShareBenchmarkPayloadBytes") ?: "256") as int
    int repetitions = (config.get("kafkaShareBenchmarkRepetitions") ?: "3") as int
    def readers = (config.get("kafkaShareBenchmarkReaders") ?: "1,3,9").toString()
            .split(",").collect { it.trim() as int }
    assertTrue(records > 0 && payloadBytes > 0 && payloadBytes <= 1024 && repetitions >= 3)
    assertTrue(readers.every { it > 0 } && readers.toSet().size() == readers.size())
    def broker = RoutineLoadTestUtils.getKafkaBroker(context)
    def tables = [standard: "kafka_skew_classic", share: "kafka_skew_share"]
    def properties = new Properties()
    properties.put("bootstrap.servers", broker)
    def admin = AdminClient.create(properties)
    def runCommand = { List command, File log ->
        def process = new ProcessBuilder(command.collect { it.toString() })
                .redirectErrorStream(true).redirectOutput(log).start()
        if (!process.waitFor(60, TimeUnit.SECONDS)) {
            process.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
            throw new IllegalStateException("Kafka configuration command timed out")
        }
        assertEquals(0, process.exitValue(), log.text)
    }
    def scenarios = [[name: "balanced", partitions: 3, hot: false],
                     [name: "hot95", partitions: 3, hot: true],
                     [name: "single", partitions: 1, hot: false]]
    def partitionFor = { int id, Map scenario ->
        scenario.hot ? (id % 100 < 95 ? 0 : 1 + (id % 100 - 95) % 2) : id % scenario.partitions
    }
    try {
        emit([event: "environment", expected_doris_revision: config.get("kafkaShareBenchmarkDorisRevision"),
              expected_kafka_revision: config.get("kafkaShareBenchmarkKafkaRevision"),
              frontends: sql_return_maparray("SHOW FRONTENDS"), backends: sql_return_maparray("SHOW BACKENDS"),
              jvm: System.getProperty("java.version"), platform: System.getProperty("os.name"),
              doris_build_type: config.get("kafkaShareBenchmarkBuildType") ?: "unknown",
              payload_bytes: payloadBytes, repetitions: repetitions, readers: readers,
              producer: [acks: "all", idempotent: true, batch_size: 65536, linger_ms: 5, compression: "lz4"],
              timing: "job creation to first query observing all unique IDs; poll interval 0.5 seconds"])
        scenarios.each { scenario ->
            readers.each { concurrency ->
                for (int repetition = 0; repetition <= repetitions; ++repetition) {
                    boolean warmup = repetition == 0
                    int count = warmup ? Math.min(records, 10000) : records
                    def suffix = UUID.randomUUID().toString().replace("-", "_")
                    def topic = "doris_skew_${suffix}".toString()
                    boolean created = false
                    try {
                        admin.createTopics([new NewTopic(topic, scenario.partitions as int, (short) 1)])
                                .all().get(30, TimeUnit.SECONDS)
                        created = true
                        def producerProperties = new Properties()
                        producerProperties.putAll(properties)
                        producerProperties.putAll(["key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
                                "value.serializer": "org.apache.kafka.common.serialization.StringSerializer",
                                "acks": "all", "enable.idempotence": "true", "batch.size": "65536",
                                "linger.ms": "5", "compression.type": "lz4", "delivery.timeout.ms": "30000"])
                        def deliveries = new AtomicLongArray(scenario.partitions as int)
                        def failure = new AtomicReference<Exception>()
                        def producer = new KafkaProducer<String, String>(producerProperties)
                        try {
                            def digest = MessageDigest.getInstance("MD5")
                            for (int id = 0; id < count; ++id) {
                                int partition = partitionFor(id, scenario)
                                def hash = digest.digest(id.toString().getBytes(StandardCharsets.UTF_8)).encodeHex().toString()
                                def payload = (hash * ((payloadBytes + 31).intdiv(32))).substring(0, payloadBytes)
                                def value = "${id},${partition},${id * 17L + 7},${payload}".toString()
                                producer.send(new ProducerRecord<String, String>(topic, partition, null, value),
                                        { metadata, error ->
                                            if (error != null) {
                                                failure.compareAndSet(null, error)
                                            } else {
                                                deliveries.incrementAndGet(metadata.partition())
                                            }
                                        })
                            }
                            producer.flush()
                            assertNull(failure.get(), "Producer must confirm every input")
                            assertEquals(count as long, (0..<scenario.partitions).sum { deliveries.get(it) } as long)
                        } finally {
                            producer.close()
                        }
                        def order = repetition % 2 == 0 ? ["standard", "share"] : ["share", "standard"]
                        order.eachWithIndex { mode, orderIndex ->
                            def job = "skew_${mode}_${suffix}".toString()
                            def group = "doris_${job}".toString()
                            boolean started = false
                            def samples = []
                            def result = [event: "result", scenario: scenario.name, partitions: scenario.partitions,
                                    requested_readers: concurrency, repetition: repetition, warmup: warmup,
                                    mode: mode, order: orderIndex, expected: count, topic: topic, job: job,
                                    producer_partition_counts: (0..<scenario.partitions).collect { deliveries.get(it) },
                                    passed: false]
                            try {
                                if (mode == "share") {
                                    def log = new File(output.parentFile, "${job}.log")
                                    runCommand([new File(kafkaHome.toString(), "bin/kafka-configs.sh").path,
                                            "--bootstrap-server", broker, "--entity-type", "groups",
                                            "--entity-name", group, "--alter", "--add-config",
                                            "share.auto.offset.reset=earliest,share.isolation.level=read_committed"], log)
                                }
                                sql "DROP TABLE IF EXISTS ${tables[mode]}"
                                sql """
                                    CREATE TABLE ${tables[mode]} (
                                        event_id BIGINT NOT NULL, source_partition INT NOT NULL,
                                        value BIGINT NOT NULL, payload VARCHAR(1024) NOT NULL
                                    ) DUPLICATE KEY(event_id)
                                    DISTRIBUTED BY HASH(event_id) BUCKETS 3
                                    PROPERTIES ("replication_num" = "1", "compression" = "zstd")
                                """
                                long begin = System.nanoTime()
                                def offset = mode == "standard" ?
                                        ', "property.kafka_default_offsets" = "OFFSET_BEGINNING", "property.isolation.level" = "read_committed"' : ""
                                sql """
                                    CREATE ROUTINE LOAD ${job} ON ${tables[mode]}
                                    COLUMNS TERMINATED BY ","
                                    PROPERTIES ("desired_concurrent_number" = "${concurrency}",
                                        "max_batch_interval" = "5", "max_batch_rows" = "200000",
                                        "strict_mode" = "true", "max_filter_ratio" = "0", "max_error_number" = "0")
                                    FROM KAFKA ("kafka_broker_list" = "${broker}", "kafka_topic" = "${topic}",
                                        "kafka_consumer_type" = "${mode}", "property.group.id" = "${group}" ${offset})
                                """
                                started = true
                                awaitUntil(300, 0.5) {
                                    def rows = sql "SELECT count(*), count(DISTINCT event_id) FROM ${tables[mode]}"
                                    def status = sql_return_maparray "SHOW ROUTINE LOAD FOR ${job}"
                                    assertTrue(status[0].State in ["NEED_SCHEDULE", "RUNNING"], "${status}")
                                    samples.add([seconds: (System.nanoTime() - begin) / 1e9,
                                            rows: rows[0][0] as long, unique: rows[0][1] as long])
                                    rows[0][1] == count
                                }
                                double seconds = samples[-1].seconds
                                sql "PAUSE ROUTINE LOAD FOR ${job}"
                                def status = (sql_return_maparray "SHOW ROUTINE LOAD FOR ${job}")[0]
                                def jobProperties = new JsonSlurper().parseText(status.JobProperties)
                                int effective = jobProperties.current_concurrent_number as int
                                assertEquals(mode == "share" ? concurrency : Math.min(concurrency, scenario.partitions as int),
                                        effective, "FE concurrency limit prevents this comparison")
                                def expectedPartition = scenario.hot ?
                                        "if(event_id % 100 < 95, 0, 1 + (event_id % 100 - 95) % 2)" :
                                        "event_id % ${scenario.partitions}"
                                def verified = (sql """
                                    SELECT count(*), count(DISTINCT event_id),
                                        sum(if(event_id < 0 OR event_id >= ${count}
                                            OR source_partition != ${expectedPartition}
                                            OR value != event_id * 17 + 7
                                            OR payload != substring(repeat(md5(cast(event_id AS STRING)),
                                                ${(payloadBytes + 31).intdiv(32)}), 1, ${payloadBytes}), 1, 0))
                                    FROM ${tables[mode]}
                                """)[0]
                                result.putAll([job_id: status.Id, seconds: seconds, unique_rows: verified[1] as long,
                                        duplicates: (verified[0] as long) - (verified[1] as long),
                                        bad_rows: verified[2] as long, effective_readers: effective,
                                        unique_rows_per_second: count / seconds,
                                        visible_95_percent_seconds: samples.find { it.unique >= Math.ceil(count * 0.95) }.seconds,
                                        samples: samples, statistic: new JsonSlurper().parseText(status.Statistic)])
                                assertEquals(count as long, result.unique_rows as long)
                                assertEquals(0L, result.bad_rows as long)
                                result.passed = true
                            } catch (Throwable error) {
                                result.error = error.toString()
                                throw error
                            } finally {
                                try {
                                    if (started) {
                                        sql "STOP ROUTINE LOAD FOR ${job}"
                                    }
                                } finally {
                                    emit(result)
                                }
                            }
                        }
                    } finally {
                        if (created) {
                            admin.deleteTopics([topic]).all().get(30, TimeUnit.SECONDS)
                        }
                    }
                }
            }
        }
        emit([event: "complete", passed: true])
    } finally {
        admin.close()
        logger.info("Native Kafka benchmark evidence: ${output.absolutePath}")
    }
}
