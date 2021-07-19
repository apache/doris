/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.doris;

import com.google.common.collect.Maps;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Doris Sink test
 */
@Slf4j
public class DorisPulsarIOTest {

    private Map<String, Object> config;
    private static final String TOPIC = "doris-sink";
    private DorisGenericRecordSink dorisGenericRecordSink;

    @Data
    public static class StreamTest {
        private long id;
        private long id2;
        private String username;

        @Override
        public String toString() {
            return "StreamTest{" +
                    "id=" + id +
                    ", id2=" + id2 +
                    ", username='" + username + '\'' +
                    '}';
        }
    }

    @BeforeMethod
    public final void setUp() throws Exception {
        config = Maps.newHashMap();
        config.put("doris_host", "127.0.0.1");
        config.put("doris_db", "db1");
        config.put("doris_table", "stream_test");
        config.put("doris_user", "root");
        config.put("doris_password", "");
        config.put("doris_http_port", "8030");
        config.put("job_failure_retries", "2");
        config.put("job_label_repeat_retries", "3");
        config.put("timeout", 1000);
        config.put("batchSize", 100);

        dorisGenericRecordSink = new DorisGenericRecordSink();
        dorisGenericRecordSink.open(config, null);
    }

    @Test
    public void testSendData() throws ExecutionException, InterruptedException, TimeoutException {
        Message<GenericRecord> insertMessage = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;

        StreamTest streamTest = new StreamTest();
        streamTest.setId(1L);
        streamTest.setId2(2L);
        streamTest.setUsername("username-1");
        AvroSchema<StreamTest> schema = AvroSchema.of(SchemaDefinition.<StreamTest>builder()
                .withPojo(StreamTest.class).build());

        byte[] insertBytes = schema.encode(streamTest);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Record<GenericRecord> insertRecord = PulsarRecord.<GenericRecord>builder()
                .message(insertMessage)
                .topicName(TOPIC)
                .ackFunction(() -> future.complete(null))
                .build();

        genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());
        when(insertMessage.getValue()).thenReturn(genericAvroSchema.decode(insertBytes));
        log.info("foo:{}, Message.getValue: {}, record.getValue: {}",
                streamTest.toString(),
                insertMessage.getValue().toString(),
                insertRecord.getValue().toString());

        dorisGenericRecordSink.write(insertRecord);
        log.info("executed write");
        future.get(2, TimeUnit.SECONDS);
    }

    @Test
    public void producerMessage() {
        final String pulsarServiceUrl = "pulsar://localhost:6650";
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarServiceUrl)
                .build()) {
            RecordSchemaBuilder schemaBuilder = SchemaBuilder.record(
                    "io.streamnative.examples.schema.json"
            );
            schemaBuilder.field("id")
                    .type(SchemaType.INT64)
                    .required();
            schemaBuilder.field("id2")
                    .type(SchemaType.INT64)
                    .required();
            schemaBuilder.field("username")
                    .type(SchemaType.STRING)
                    .required();
            SchemaInfo schemaInfo = schemaBuilder.build(SchemaType.JSON);
            GenericSchema<GenericRecord> schema = Schema.generic(schemaInfo);
            try (Producer<GenericRecord> producer = client.newProducer(schema)
                    .topic(TOPIC)
                    .create()) {
                final int numMessages = 1000;
                for (long i = 0L; i < numMessages; i++) {
                    final long id = i;
                    final long id2 = i + 1L;
                    String username = "user-" + i;
                    GenericRecord record = schema.newRecordBuilder()
                            .set("id", id)
                            .set("id2", id2)
                            .set("username", username)
                            .build();
                    // send the payment in an async way
                    producer.newMessage()
                            .key(username)
                            .value(record)
                            .sendAsync();
                    if (i % 100 == 0) {
                        Thread.sleep(200);
                    }
                }
                // flush out all outstanding messages
                producer.flush();
                log.info("Successfully produced %d messages to a topic called %s%n",
                        numMessages, TOPIC);
            }
        } catch (PulsarClientException | InterruptedException e) {
            log.error("Failed to produce generic avro messages to pulsar:");
            e.printStackTrace();
            Runtime.getRuntime().exit(-1);
        }
    }
}