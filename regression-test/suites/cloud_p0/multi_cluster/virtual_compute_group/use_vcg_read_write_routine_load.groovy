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

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ListTopicsOptions

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

// 1 create two physical cluster c1, c2, every cluster contains 2 be
// 2 create vcg, c1, c2 are sub compute group of vcg, adn c1 is active cg
// 3 use vcg
// 4 stop a backend of c1
// 5 stop another backend of c1

suite('use_vcg_read_write_routine_load', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    String routine_load_tbl = "test_routine_load_vcg"
    String tbl = "test_virtual_compute_group_tbl"
    def topic = "test-topic"

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
        ExecutorService pool;
        String kafka_broker_list = context.config.otherConfigs.get("externalEnvIp") + ":" + context.config.otherConfigs.get("kafka_port")
        pool = Executors.newFixedThreadPool(1)
        pool.execute{
             def props = new Properties()
             props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_broker_list)
             props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
             'org.apache.kafka.common.serialization.StringSerializer')
             props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
             'org.apache.kafka.common.serialization.StringSerializer')

             AdminClient adminClient = AdminClient.create(props);
             def delResult = adminClient.deleteTopics([topic] as List<String>)
             println("the result is " + delResult);
             def cnt = 0
             while (!delResult.all().isDone()) {
                 sleep(1000)
                 if (cnt++ > 100) {
                    log.info("failed to wait for delResult")
                    break
                 }
             }

             NewTopic newTopic = new NewTopic(topic, 10, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)
             List<NewTopic> newTopics = new ArrayList<NewTopic>();
             newTopics.add(newTopic);
             def createResult = adminClient.createTopics(newTopics);
             println("the result is " + createResult);

             adminClient.close();

             def producer = new KafkaProducer<String, String>(props)
             for (int i = 0; i < 30; i++) {
                 String msg_key = i.toString();
                 String msg_value = i.toString() + "|" + "abc" + "|" + (i * 2).toString();
                 def message = new ProducerRecord<String, String>(topic, msg_key, msg_value)
                 producer.send(message)
                 sleep(1000)
             }

             producer.close()
        }

        pool.shutdown()                 //all tasks submitted

        sleep(1000);

        long timestamp = System.currentTimeMillis()
        String job_name = "routine_load_test_" + String.valueOf(timestamp);
        sql """
            CREATE ROUTINE LOAD ${job_name} ON
            ${routine_load_tbl} COLUMNS TERMINATED BY "|",
            COLUMNS(id, name, score)
            PROPERTIES(
            "desired_concurrent_number"="2",
            "max_batch_interval"="6",
            "max_batch_rows"="200000",
            "max_batch_size"="104857600")
            FROM KAFKA(
            "kafka_broker_list"="${kafka_broker_list}",
            "kafka_topic"="${topic}",
            "property.group.id"="gid6",
            "property.clinet.id"="cid6",
            "property.kafka_default_offsets"="OFFSET_BEGINNING");
        """

        while (!pool.isTerminated()){}
        sleep(30000);
        order_qt_q1 "select * from ${routine_load_tbl}"
    }

    options.connectToFollower = false

    for (def j = 0; j < 2; j++) {
        docker(options) {
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
            def normalVclusterName = "normalVirtualClusterName"
            def normalVclusterId = "normalVirtualClusterId"
            def vcgClusterNames = [clusterName1, clusterName2]
            def clusterPolicy = [type: "ActiveStandby", active_cluster_name: "${clusterName1}", standby_cluster_names: ["${clusterName2}"]]
            clusterMap = [cluster_name: "${normalVclusterName}", cluster_id:"${normalVclusterId}", type:"VIRTUAL", cluster_names:vcgClusterNames, cluster_policy:clusterPolicy]
            def normalInstance = [instance_id: "${instance_id}", cluster: clusterMap]
            jsonOutput = new JsonOutput()
            def normalVcgBody = jsonOutput.toJson(normalInstance)
            add_cluster_api.call(msHttpPort, normalVcgBody) {
                respCode, body ->
                    log.info("add normal vitural compute group http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            // show cluster
            sleep(5000)
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group {}", showComputeGroup)
            def vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNotNull(vcgInShow)
            assertTrue(vcgInShow.Policy.contains("activeComputeGroup='newcluster1', standbyComputeGroup='newcluster2'"))

            showResult = sql "show clusters"
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

            sql """ set enable_profile = true """

            before_cluster1_be0_load_rows = get_be_metric(cluster1Ips[0], "8040", "load_rows");
            log.info("before_cluster1_be0_load_rows : ${before_cluster1_be0_load_rows}".toString())
            before_cluster1_be0_flush = get_be_metric(cluster1Ips[0], "8040", "memtable_flush_total");
            log.info("before_cluster1_be0_flush : ${before_cluster1_be0_flush}".toString())

            before_cluster1_be1_load_rows = get_be_metric(cluster1Ips[1], "8040", "load_rows");
            log.info("before_cluster1_be1_load_rows : ${before_cluster1_be1_load_rows}".toString())
            before_cluster1_be1_flush = get_be_metric(cluster1Ips[1], "8040", "memtable_flush_total");
            log.info("before_cluster1_be1_flush : ${before_cluster1_be1_flush}".toString())

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

            after_cluster1_be1_load_rows = get_be_metric(cluster1Ips[1], "8040", "load_rows");
            log.info("after_cluster1_be1_load_rows : ${after_cluster1_be1_load_rows}".toString())
            after_cluster1_be1_flush = get_be_metric(cluster1Ips[1], "8040", "memtable_flush_total");
            log.info("after_cluster1_be1_flush : ${after_cluster1_be1_flush}".toString())

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

            set = [cluster1Ips[0] + ":" + "8060", cluster1Ips[1] + ":" + "8060"] as Set
            sql """ select count(score) AS theCount from ${routine_load_tbl} group by name order by theCount limit 1 """
            checkProfileNew.call(set)

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

            set = [cluster1Ips[0] + ":" + "8060"] as Set
            sql """ select count(score) AS theCount from ${routine_load_tbl} group by name order by theCount limit 1 """
            checkProfileNew.call(set)

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

            set = [cluster2Ips[0] + ":" + "8060", cluster2Ips[1] + ":" + "8060"] as Set
            sql """ select count(score) AS theCount from ${routine_load_tbl} group by name order by theCount limit 1 """
            checkProfileNew.call(set)

            sleep(16000)
            // show cluster
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group {}", showComputeGroup)
            vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNotNull(vcgInShow)
            assertTrue(vcgInShow.Policy.contains("activeComputeGroup='newcluster2', standbyComputeGroup='newcluster1'"))
        }
        // connect to follower, run again
        //options.connectToFollower = true
    }
}
