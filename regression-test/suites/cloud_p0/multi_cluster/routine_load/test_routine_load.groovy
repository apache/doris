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

suite("test_routine_load") {
    def topic = "test-topic"
    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);

    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    wait_cluster_change()

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "regression_cluster_id1");
    wait_cluster_change()

    result  = sql "show clusters"
    assertEquals(result.size(), 2);

    sql "use @regression_cluster_name1"
    result  = sql "show clusters"

    sql """ set enable_profile = true """

    def before_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("before_cluster0_load_rows : ${before_cluster0_load_rows}".toString())
    def before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    def before_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("before_cluster1_load_rows : ${before_cluster1_load_rows}".toString())
    def before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("before_cluster1_flush : ${before_cluster1_flush}".toString())

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

    def tableName = "test_routine_load"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            id INT,
            name CHAR(10),
            score INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1;
    """

    sleep(1000);

    long timestamp = System.currentTimeMillis()
    String job_name = "routine_load_test_" + String.valueOf(timestamp);
    sql """
        CREATE ROUTINE LOAD ${job_name} ON
        ${tableName} COLUMNS TERMINATED BY "|",
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
    order_qt_q1 "select * from ${tableName}"

    def after_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("after_cluster0_load_rows : ${after_cluster0_load_rows}".toString())
    def after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    def after_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("after_cluster1_load_rows : ${after_cluster1_load_rows}".toString())
    def after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    assertTrue(before_cluster0_load_rows == after_cluster0_load_rows)
    assertTrue(before_cluster0_flush == after_cluster0_flush)

    assertTrue(before_cluster1_load_rows != after_cluster1_load_rows)
    assertTrue(before_cluster1_flush != after_cluster1_flush)
    sql """ DROP TABLE IF EXISTS ${tableName}; """
}

