#!/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
kafka的配置文件
"""

import pytest
from kafka import KafkaProducer
import util
import palo_client
import palo_config
config = palo_config.config

LOG = palo_client.LOG
L = palo_client.L

zookeeper = config.kafka_zookeeper
kafka_broker_list = config.kafka_broker_list

java_home = 'xxxx'
user = 'xxx'
password = 'xxx'
host = 'xxxx'
kafka_path = '/home/xxxx/kafka/kafka_2.12-2.0.1'


def gen_file(file):
    path = '/home/xxxx/data/sys/%s' % file
    return path


def send_to_kafka(topic, file):
    """send file all line to kafka"""
    offset = get_topic_offset(topic) 
    print(offset)
    LOG.info(L('KAFKA TOPIC OFFSET', broker=kafka_broker_list, topic=topic, offset=offset))
    cmd = 'export JAVA_HOME=%s; %s/bin/kafka-console-producer.sh --broker-list %s --topic %s < %s' \
          % (java_home, kafka_path, kafka_broker_list, topic, gen_file(file))
    print(cmd)
    status, output = util.exec_cmd(cmd, user, password, host)
    LOG.info(L('KAFKA PRODUCER', broker=kafka_broker_list, topic=topic, file=file, status=status))
    print(status)
    # assert status == 0, output
    offset = get_topic_offset(topic)
    print(offset)
    LOG.info(L('KAFKA TOPIC OFFSET', broker=kafka_broker_list, topic=topic, offset=offset))
    if status != 0:
        raise pytest.skip('send kafka data failed, skip this case')


def send_msg_to_kafka(msg, topic, partition=None):
    """send 1 msg to kafka"""
    producer = KafkaProducer(bootstrap_servers=kafka_broker_list.split(','))
    future = producer.send(topic, msg, partition=partition)
    result = future.get(10)
    LOG.info(L('KAFKA MSG SEND', msg=msg, ret=result))


def create_kafka_topic(topic_name, partition_num):
    """create kafka topic"""
    cmd = 'export JAVA_HOME=%s; %s/bin/kafka-topics.sh --create --zookeeper %s ' \
          '--replication-factor 1 --partitions %s --topic %s' % (java_home,
                                                                 kafka_path,
                                                                 zookeeper,
                                                                 partition_num, topic_name)
    print(cmd)
    status, output = util.exec_cmd(cmd, user, password, host)
    LOG.info(L('CREATE KAFKA TOPIC', zookeeper=zookeeper, partiton_num=partition_num, 
               topic=topic_name, status=status))
    print(status, output)
    assert status == 0, output


def get_topic_offset(topic):
    """get topic offset"""
    cmd = 'export JAVA_HOME=%s; %s/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list %s --topic %s --time -1' \
          % (java_home, kafka_path, kafka_broker_list, topic)
    print(cmd)
    status, output = util.exec_cmd(cmd, user, password, host)
    
    assert status == 0, output
    partitions = str(output, 'utf8').split("\r\n")
    offset = {}
    for p in partitions[1:-1]:
        item = p.split(':')
        offset[item[1]] = item[2]
    return offset


if __name__ == '__main__':
    # 执行routine load case前要创建Topic，以查询端口为区分，避免两个环境同时执行时相互影响
    port = 9030
    p1 = 'multi-partitions-50-%s' % port
    create_kafka_topic(p1, 50)
    p2 = 'single-partition-%s' % port
    create_kafka_topic(p2, 1)
    p3 = 'single-partition-1-%s' % port
    create_kafka_topic(p3, 1)
    p4 = 'single-partition-2-%s' % port
    create_kafka_topic(p4, 1)
    p5 = 'single-partition-3-%s' % port
    create_kafka_topic(p5, 1)
    p6 = 'single-partition-4-%s' % port
    create_kafka_topic(p6, 1)
    p7 = 'single-partition-5-%s' % port
    create_kafka_topic(p7, 1)
    p8 = 'three-partition-%s' % port
    create_kafka_topic(p8, 3)
    p9 = 'first-test-%s' % port
    create_kafka_topic(p9, 10)
    p10 = 'time-zone-%s' % port
    create_kafka_topic(p10, 8)
    p11 = 'routine-load-delete-%s' % port
    create_kafka_topic(p11, 5)
    p12 = 'single-partition-6-%s' % port
    create_kafka_topic(p12, 1)
