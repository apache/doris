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

import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import java.time.Duration
import java.util.concurrent.TimeUnit

// 1 create two physical cluster c1, c2, every cluster contains 2 be
// 2 create vcg, c1, c2 are sub compute group of vcg, adn c1 is active cg
// 3 use vcg
// 4 stop a backend of c1
// 5 stop another backend of c1

suite('use_vcg_read_write_routine_load', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    String routine_load_tbl = "test_routine_load_vcg"
    String tbl = "test_virtual_compute_group_tbl"
    String kafkaBrokerList

    // No host ports or shared topics: each docker cluster owns its Kafka container.
    def dockerCommand = { List args ->
        def process = new ProcessBuilder((["docker"] + args).collect { it.toString() }).start()
        def stdout = new StringBuilder()
        def stderr = new StringBuilder()
        def outThread = process.consumeProcessOutputStream(stdout)
        def errThread = process.consumeProcessErrorStream(stderr)
        if (!process.waitFor(180, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            throw new IllegalStateException("Docker command timed out: ${args}")
        }
        outThread.join()
        errThread.join()
        assertEquals(0, process.exitValue(), "Docker command ${args} failed: ${stderr}")
        return stdout.toString().trim()
    }

    def startKafka = { String container ->
        def networks = new JsonSlurper().parseText(dockerCommand([
            "inspect", "--format", "{{json .NetworkSettings.Networks}}", "doris-${cluster.name}-fe-1"
        ]))
        assertEquals(1, networks.size(), "Expected one Doris bridge network")
        String network = networks.keySet().first()
        dockerCommand([
            "run", "-d", "--name", container, "--network", network,
            "-e", "KAFKA_NODE_ID=1",
            "-e", "KAFKA_PROCESS_ROLES=broker,controller",
            "-e", "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
            "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "-e", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
            "-e", "KAFKA_HEAP_OPTS=-Xms256m -Xmx512m",
            "apache/kafka:3.9.1", "bash", "-ec",
            'export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://$(hostname -i):9092"; exec /etc/kafka/docker/run'
        ])
        // The Linux docker runner already accesses FE/BE bridge IPs directly.
        def kafkaNetworks = new JsonSlurper().parseText(dockerCommand([
            "inspect", "--format", "{{json .NetworkSettings.Networks}}", container
        ]))
        kafkaBrokerList = "${kafkaNetworks[network].IPAddress}:9092"
        def admin = AdminClient.create([
            "bootstrap.servers": kafkaBrokerList,
            "default.api.timeout.ms": "10000",
            "request.timeout.ms": "5000"
        ])
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120)
            Throwable lastError
            boolean ready = false
            while (System.nanoTime() < deadline) {
                try {
                    assertEquals(1, admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size())
                    ready = true
                    break
                } catch (Exception e) {
                    lastError = e
                    sleep(1000)
                }
            }
            assertTrue(ready, "Kafka did not become ready at ${kafkaBrokerList}: ${lastError}")
            logger.info("Suite Kafka ready: {} at {}", container, kafkaBrokerList)
        } finally {
            admin.close(Duration.ofSeconds(5))
        }
    }

    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
    ]
    options.feNum = 3
    options.cloudMode = true

    def add_cluster_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/add_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    def alter_cluster_info_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/alter_vcluster_info?token=$token"
            body request_body
            check check_func
        }
    }

    def execute_routind_Load = {
        String topic = "vcg-${UUID.randomUUID()}"
        String jobName = "routine_load_${UUID.randomUUID().toString().replace('-', '')}"
        long expectedRows = (sql("select count(*) from ${routine_load_tbl}")[0][0] as long) + 30
        def props = new Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList)
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                'org.apache.kafka.common.serialization.StringSerializer')
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                'org.apache.kafka.common.serialization.StringSerializer')
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, '30000')
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, '30000')
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, '10000')
        def admin = AdminClient.create(props)
        boolean jobCreated = false
        try {
            admin.createTopics([new NewTopic(topic, 10, (short) 1)]).all().get(30, TimeUnit.SECONDS)
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${routine_load_tbl}
                COLUMNS TERMINATED BY "|", COLUMNS(id, name, score)
                PROPERTIES(
                    "desired_concurrent_number"="2",
                    "max_batch_interval"="6",
                    "max_batch_rows"="200000",
                    "max_batch_size"="104857600")
                FROM KAFKA(
                    "kafka_broker_list"="${kafkaBrokerList}",
                    "kafka_topic"="${topic}",
                    "property.group.id"="${jobName}",
                    "property.kafka_default_offsets"="OFFSET_BEGINNING");
            """
            jobCreated = true
            def producer = new KafkaProducer<String, String>(props)
            try {
                for (int i = 0; i < 30; i++) {
                    producer.send(new ProducerRecord<String, String>(topic,
                            i.toString(), "${i}|abc|${i * 2}".toString())).get(30, TimeUnit.SECONDS)
                    sleep(1000)
                }
            } finally {
                producer.close(Duration.ofSeconds(5))
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120)
            while ((sql("select count(*) from ${routine_load_tbl}")[0][0] as long) < expectedRows
                    && System.nanoTime() < deadline) {
                sleep(1000)
            }
            logger.info("Routine load status: {}", sql_return_maparray("SHOW ROUTINE LOAD FOR ${jobName}"))
            assertEquals(expectedRows, sql("select count(*) from ${routine_load_tbl}")[0][0] as long)
            order_qt_q1 "select * from ${routine_load_tbl}"
        } finally {
            try {
                if (jobCreated) {
                    sql "STOP ROUTINE LOAD FOR ${jobName}"
                }
            } finally {
                admin.close(Duration.ofSeconds(5))
            }
        }
    }

    options.connectToFollower = false

    for (def j = 0; j < 2; j++) {
        docker(options) {
            String kafkaContainer = "vcg-kafka-${UUID.randomUUID()}"
            try {
                startKafka(kafkaContainer)
                def ms = cluster.getAllMetaservices().get(0)
                def msHttpPort = ms.host + ":" + ms.httpPort
                logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

                def clusterName1 = "newcluster1"
                // add cluster newcluster1
                cluster.addBackend(2, clusterName1)

                def clusterName2 = "newcluster2"
                // add cluster newcluster2
                cluster.addBackend(2, clusterName2)

                // add vcluster
                def instance_id = "default_instance_id"
                def normalVclusterName = "normalVirtualClusterName"
                def normalVclusterId = "normalVirtualClusterId"
                def vcgClusterNames = [clusterName1, clusterName2]
                def clusterPolicy = [type: "ActiveStandby", active_cluster_name: "${clusterName1}", standby_cluster_names: ["${clusterName2}"]]
                def clusterMap = [cluster_name: "${normalVclusterName}", cluster_id:"${normalVclusterId}", type:"VIRTUAL", cluster_names:vcgClusterNames, cluster_policy:clusterPolicy]
                def normalInstance = [instance_id: "${instance_id}", cluster: clusterMap]
                def jsonOutput = new JsonOutput()
                def normalVcgBody = jsonOutput.toJson(normalInstance)
                add_cluster_api.call(msHttpPort, normalVcgBody) {
                    respCode, body ->
                        log.info("add normal vitural compute group http cli result: ${body} ${respCode}".toString())
                        def json = parseJson(body)
                        assertTrue(json.code.equalsIgnoreCase("OK"))
                }

                // show cluster
                sleep(5000)
                def showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
                log.info("show compute group {}", showComputeGroup)
                def vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
                assertNotNull(vcgInShow)
                assertTrue(vcgInShow.Policy.contains('"activeComputeGroup":"newcluster1","standbyComputeGroup":"newcluster2"'))

                def showResult = sql "show clusters"
                for (row : showResult) {
                    println row
                }
                showResult = sql "show backends"
                for (row : showResult) {
                    println row
                }

                // get be ip of clusterName1
                def jsonSlurper = new JsonSlurper()
                def cluster1Ips = showResult.findAll { entry ->
                    def raw = entry[19]
                    def info = (raw instanceof String) ? jsonSlurper.parseText(raw) : raw
                    info.compute_group_name == clusterName1
                }.collect { entry ->
                    entry[1]
                }
                log.info("backends of cluster1: ${clusterName1} ${cluster1Ips}".toString())

                def cluster2Ips = showResult.findAll { entry ->
                    def raw = entry[19]
                    def info = (raw instanceof String) ? jsonSlurper.parseText(raw) : raw
                    info.compute_group_name == clusterName2
                }.collect { entry ->
                    entry[1]
                }
                log.info("backends of cluster2: ${clusterName2} ${cluster2Ips}".toString())

                sql """use @${normalVclusterName}"""

                sql """ drop table if exists ${routine_load_tbl} """
                sql """
                    CREATE TABLE IF NOT EXISTS ${routine_load_tbl}
                    (
                        id INT,
                        name CHAR(10),
                        score INT
                    )
                    DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 10;
                """

                sql """ set global enable_profile = true """

                def before_cluster1_be0_load_rows = get_be_metric(cluster1Ips[0], "8040", "load_rows");
                log.info("before_cluster1_be0_load_rows : ${before_cluster1_be0_load_rows}".toString())
                def before_cluster1_be0_flush = get_be_metric(cluster1Ips[0], "8040", "memtable_flush_total");
                log.info("before_cluster1_be0_flush : ${before_cluster1_be0_flush}".toString())

                def before_cluster1_be1_load_rows = get_be_metric(cluster1Ips[1], "8040", "load_rows");
                log.info("before_cluster1_be1_load_rows : ${before_cluster1_be1_load_rows}".toString())
                def before_cluster1_be1_flush = get_be_metric(cluster1Ips[1], "8040", "memtable_flush_total");
                log.info("before_cluster1_be1_flush : ${before_cluster1_be1_flush}".toString())

                def before_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
                log.info("before_cluster2_be0_load_rows : ${before_cluster2_be0_load_rows}".toString())
                def before_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
                log.info("before_cluster2_be0_flush : ${before_cluster2_be0_flush}".toString())

                def before_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
                log.info("before_cluster2_be1_load_rows : ${before_cluster2_be1_load_rows}".toString())
                def before_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
                log.info("before_cluster2_be1_flush : ${before_cluster2_be1_flush}".toString())

                execute_routind_Load.call()

                def after_cluster1_be0_load_rows = get_be_metric(cluster1Ips[0], "8040", "load_rows");
                log.info("after_cluster1_be0_load_rows : ${after_cluster1_be0_load_rows}".toString())
                def after_cluster1_be0_flush = get_be_metric(cluster1Ips[0], "8040", "memtable_flush_total");
                log.info("after_cluster1_be0_flush : ${after_cluster1_be0_flush}".toString())

                def after_cluster1_be1_load_rows = get_be_metric(cluster1Ips[1], "8040", "load_rows");
                log.info("after_cluster1_be1_load_rows : ${after_cluster1_be1_load_rows}".toString())
                def after_cluster1_be1_flush = get_be_metric(cluster1Ips[1], "8040", "memtable_flush_total");
                log.info("after_cluster1_be1_flush : ${after_cluster1_be1_flush}".toString())

                def after_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
                log.info("after_cluster2_be0_load_rows : ${after_cluster2_be0_load_rows}".toString())
                def after_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
                log.info("after_cluster2_be0_flush : ${after_cluster2_be0_flush}".toString())

                def after_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
                log.info("after_cluster2_be1_load_rows : ${after_cluster2_be1_load_rows}".toString())
                def after_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
                log.info("after_cluster2_be1_flush : ${after_cluster2_be1_flush}".toString())

                assertTrue(before_cluster1_be0_load_rows < after_cluster1_be0_load_rows || before_cluster1_be1_load_rows < after_cluster1_be1_load_rows)
                assertTrue(before_cluster1_be0_flush < after_cluster1_be0_flush || before_cluster1_be1_flush < after_cluster1_be1_flush)

                assertTrue(before_cluster2_be0_load_rows == after_cluster2_be0_load_rows)
                assertTrue(before_cluster2_be0_flush == after_cluster2_be0_flush)
                assertTrue(before_cluster2_be1_load_rows == after_cluster2_be1_load_rows)
                assertTrue(before_cluster2_be1_flush == after_cluster2_be1_flush)

                def addrSet = [cluster1Ips[0] + ":" + "8060", cluster1Ips[1] + ":" + "8060"] as Set
                sql """ select count(score) AS theCount from ${routine_load_tbl} group by name order by theCount limit 1 """
                if (options.connectToFollower) {
                    checkProfileNew.call(cluster.getOneFollowerFe(), addrSet)
                } else {
                    checkProfileNew.call(cluster.getMasterFe(), addrSet)
                }

                cluster.stopBackends(4)
                sleep(6000)

                showResult = sql "show backends"
                for (row : showResult) {
                    println row
                }
                cluster1Ips = showResult.findAll { entry ->
                    def raw = entry[19]
                    def info = (raw instanceof String) ? jsonSlurper.parseText(raw) : raw

                    def alive = entry[9]
                    log.info("alive : ${alive}".toString())
                    info.compute_group_name == clusterName1 && alive == "true"
                }.collect { entry ->
                    entry[1]
                }
                log.info("backends of cluster1: ${clusterName1} ${cluster1Ips}".toString())

                before_cluster1_be0_load_rows = get_be_metric(cluster1Ips[0], "8040", "load_rows");
                log.info("before_cluster1_be0_load_rows : ${before_cluster1_be0_load_rows}".toString())
                before_cluster1_be0_flush = get_be_metric(cluster1Ips[0], "8040", "memtable_flush_total");
                log.info("before_cluster1_be0_flush : ${before_cluster1_be0_flush}".toString())

                before_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
                log.info("before_cluster2_be0_load_rows : ${before_cluster2_be0_load_rows}".toString())
                before_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
                log.info("before_cluster2_be0_flush : ${before_cluster2_be0_flush}".toString())

                before_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
                log.info("before_cluster2_be1_load_rows : ${before_cluster2_be1_load_rows}".toString())
                before_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
                log.info("before_cluster2_be1_flush : ${before_cluster2_be1_flush}".toString())

                execute_routind_Load.call()

                after_cluster1_be0_load_rows = get_be_metric(cluster1Ips[0], "8040", "load_rows");
                log.info("after_cluster1_be0_load_rows : ${after_cluster1_be0_load_rows}".toString())
                after_cluster1_be0_flush = get_be_metric(cluster1Ips[0], "8040", "memtable_flush_total");
                log.info("after_cluster1_be0_flush : ${after_cluster1_be0_flush}".toString())

                after_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
                log.info("after_cluster2_be0_load_rows : ${after_cluster2_be0_load_rows}".toString())
                after_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
                log.info("after_cluster2_be0_flush : ${after_cluster2_be0_flush}".toString())

                after_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
                log.info("after_cluster2_be1_load_rows : ${after_cluster2_be1_load_rows}".toString())
                after_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
                log.info("after_cluster2_be1_flush : ${after_cluster2_be1_flush}".toString())

                assertTrue(before_cluster1_be0_load_rows < after_cluster1_be0_load_rows || before_cluster1_be1_load_rows < after_cluster1_be1_load_rows)
                assertTrue(before_cluster1_be0_flush < after_cluster1_be0_flush || before_cluster1_be1_flush < after_cluster1_be1_flush)

                assertTrue(before_cluster2_be0_load_rows == after_cluster2_be0_load_rows)
                assertTrue(before_cluster2_be0_flush == after_cluster2_be0_flush)
                assertTrue(before_cluster2_be1_load_rows == after_cluster2_be1_load_rows)
                assertTrue(before_cluster2_be1_flush == after_cluster2_be1_flush)

                addrSet = [cluster1Ips[0] + ":" + "8060"] as Set
                sql """ select count(score) AS theCount from ${routine_load_tbl} group by name order by theCount limit 1 """
                if (options.connectToFollower) {
                    checkProfileNew.call(cluster.getOneFollowerFe(), addrSet)
                } else {
                    checkProfileNew.call(cluster.getMasterFe(), addrSet)
                }

                cluster.stopBackends(5)

                before_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
                log.info("before_cluster2_be0_load_rows : ${before_cluster2_be0_load_rows}".toString())
                before_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
                log.info("before_cluster2_be0_flush : ${before_cluster2_be0_flush}".toString())

                before_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
                log.info("before_cluster2_be1_load_rows : ${before_cluster2_be1_load_rows}".toString())
                before_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
                log.info("before_cluster2_be1_flush : ${before_cluster2_be1_flush}".toString())

                execute_routind_Load.call()

                after_cluster2_be0_load_rows = get_be_metric(cluster2Ips[0], "8040", "load_rows");
                log.info("after_cluster2_be0_load_rows : ${after_cluster2_be0_load_rows}".toString())
                after_cluster2_be0_flush = get_be_metric(cluster2Ips[0], "8040", "memtable_flush_total");
                log.info("after_cluster2_be0_flush : ${after_cluster2_be0_flush}".toString())

                after_cluster2_be1_load_rows = get_be_metric(cluster2Ips[1], "8040", "load_rows");
                log.info("after_cluster2_be1_load_rows : ${after_cluster2_be1_load_rows}".toString())
                after_cluster2_be1_flush = get_be_metric(cluster2Ips[1], "8040", "memtable_flush_total");
                log.info("after_cluster2_be1_flush : ${after_cluster2_be1_flush}".toString())

                assertTrue(before_cluster2_be0_load_rows < after_cluster2_be0_load_rows || before_cluster2_be1_load_rows < after_cluster2_be1_load_rows)
                assertTrue(before_cluster2_be0_flush < after_cluster2_be0_flush || before_cluster2_be1_flush < after_cluster2_be1_flush)

                addrSet = [cluster2Ips[0] + ":" + "8060", cluster2Ips[1] + ":" + "8060"] as Set
                sql """ select count(score) AS theCount from ${routine_load_tbl} group by name order by theCount limit 1 """
                if (options.connectToFollower) {
                    checkProfileNew.call(cluster.getOneFollowerFe(), addrSet)
                } else {
                    checkProfileNew.call(cluster.getMasterFe(), addrSet)
                }

                sleep(16000)
                // show cluster
                showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
                log.info("show compute group {}", showComputeGroup)
                vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
                assertNotNull(vcgInShow)
                assertTrue(vcgInShow.Policy.contains('"activeComputeGroup":"newcluster2","standbyComputeGroup":"newcluster1"'))
            } finally {
                try {
                    logger.info("Kafka container log: {}", dockerCommand(["logs", "--tail", "100", kafkaContainer]))
                } finally {
                    dockerCommand(["rm", "-f", "-v", kafkaContainer])
                }
            }
        }
        // connect to follower, run again
        options.connectToFollower = true
    }
}
