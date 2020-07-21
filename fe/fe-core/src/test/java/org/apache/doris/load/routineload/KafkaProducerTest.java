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

package org.apache.doris.load.routineload;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTest {

    public Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "xxx");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducerTest kafkaProducerTest = new KafkaProducerTest();
        Producer<Long, String> kafkaProducer = kafkaProducerTest.createProducer();
        int i = 1;
        while (true) {
            String value = String.valueOf(i);
            if (i % 10000 == 0) {
                value = value + "\t" + value;
            }
            ProducerRecord<Long, String> record = new ProducerRecord<>("miaoling", value);
            try {
                RecordMetadata metadata = kafkaProducer.send(record).get();
                System.out.println("Record send with value " + value + " to partition " +
                                           metadata.partition() + " with offset " + metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("Error in sending record " + value);
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record " + value);
                System.out.println(e);
            }
            i++;
        }
    }

}
