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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.thrift.BackendServiceConstants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaPropertyValidationTest {
    private KafkaDataSourceProperties properties(String key, String value, boolean alter) {
        Map<String, String> source = new HashMap<>();
        if (!alter) {
            source.put("kafka_broker_list", "127.0.0.1:9092");
            source.put("kafka_topic", "test_topic");
        }
        source.put("property." + key, value);
        KafkaDataSourceProperties properties = new KafkaDataSourceProperties(source);
        properties.setAlter(alter);
        properties.setTimezone("UTC");
        return properties;
    }

    @Test
    public void testCreateAndAlterRejectExecutableAndUnknownProperties() {
        for (boolean alter : new boolean[] {false, true}) {
            for (String key : List.of("sasl.kerberos.kinit.cmd", "sasl.kerberos.ticket.cache",
                    "sasl.kerberos.min.time.before.relogin", "plugin.library.paths", "ssl.engine.location",
                    "ssl.engine.id", "ssl.providers", "unknown.future.property", "sasl.jaas.config",
                    "security.protocol\0suffix")) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> properties(key, "sensitive-value", alter).analyze());
                Assertions.assertTrue(error.getMessage().contains("is not allowed in Routine Load"), key);
                Assertions.assertFalse(error.getMessage().contains("sensitive-value"));
            }
        }
    }

    @Test
    public void testRejectKerberosRefreshReenable() {
        for (boolean alter : new boolean[] {false, true}) {
            for (String value : List.of("1", "60000", "00", "0\0suffix", "FILE:interval")) {
                Assertions.assertThrows(AnalysisException.class,
                        () -> properties("sasl.kerberos.min.time.before.relogin", value, alter).analyze());
            }
        }
    }

    @Test
    public void testCertificateAndKeytabFilesRequireManagedReferences() throws Exception {
        for (boolean alter : new boolean[] {false, true}) {
            for (String key : BackendServiceConstants.KAFKA_FILE_PROPERTIES) {
                for (String value : List.of("/etc/ssl/private/key.pem", "relative.pem", "", "FILE:")) {
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            () -> properties(key, value, alter).analyze());
                    Assertions.assertTrue(error.getMessage().contains("CREATE FILE"));
                }
                KafkaDataSourceProperties properties = properties(key, "FILE:client.pem", alter);
                properties.analyze();
                Assertions.assertEquals("FILE:client.pem", properties.getCustomKafkaProperties().get(key));
            }
        }
    }

    @Test
    public void testNormalConsumerPropertiesAndDefaultOffsets() throws Exception {
        Map<String, String> accepted = Map.of(
                "group.id", "routine-load-group",
                "client.id", "routine-load-client",
                "security.protocol", "SASL_SSL",
                "sasl.mechanisms", "SCRAM-SHA-256",
                "sasl.username", "user",
                "sasl.password", "secret",
                "isolation.level", "read_committed",
                "fetch.max.bytes", "1048576",
                "kafka_default_offsets", "OFFSET_BEGINNING");
        for (boolean alter : new boolean[] {false, true}) {
            for (Map.Entry<String, String> property : accepted.entrySet()) {
                properties(property.getKey(), property.getValue(), alter).analyze();
            }
        }
    }

    @Test
    public void testAwsPropertiesWithAndWithoutPrefix() throws Exception {
        for (String prefix : List.of("", "property.")) {
            Map<String, String> source = new HashMap<>();
            source.put("kafka_broker_list", "127.0.0.1:9098");
            source.put("kafka_topic", "test_topic");
            source.put("property.security.protocol", "SASL_SSL");
            source.put("property.sasl.mechanism", "OAUTHBEARER");
            source.put(prefix + "aws.region", "us-east-1");
            source.put(prefix + "aws.role_arn", "arn:aws:iam::123456789012:role/test");
            KafkaDataSourceProperties properties = new KafkaDataSourceProperties(source);
            properties.setTimezone("UTC");
            properties.analyze();
            Assertions.assertEquals("us-east-1", properties.getCustomKafkaProperties().get("aws.region"));
        }
    }

    @Test
    public void testDisableKerberosCommandForOlderBackends() throws Exception {
        properties("sasl.kerberos.min.time.before.relogin", "0", false).analyze();
        for (Map<String, String> custom : List.of(Map.<String, String>of(),
                Map.of("group.id", "test", "sasl.kerberos.principal", "user;unused"),
                Map.of("kafka_default_offsets", "OFFSET_BEGINNING",
                        "kafka_origin_default_offsets", "OFFSET_BEGINNING"))) {
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L,
                    "127.0.0.1:9092", "test_topic", UserIdentity.ADMIN);
            Deencapsulation.setField(job, "customProperties", custom);
            Deencapsulation.invoke(job, "convertCustomProperties", true);
            Assertions.assertEquals("0", job.getConvertedCustomProperties()
                    .get("sasl.kerberos.min.time.before.relogin"));
        }
    }

    @Test
    public void testPersistedPropertiesRejectedBeforeMetadataRequests() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L,
                "127.0.0.1:9092", "test_topic", UserIdentity.ADMIN);
        Deencapsulation.setField(job, "customProperties", Map.of("plugin.library.paths", "unused.so"));
        // Even a previously populated conversion cache must not bypass validation.
        Deencapsulation.setField(job, "convertedCustomProperties", Map.of("group.id", "cached"));
        DdlException error = Assertions.assertThrows(DdlException.class,
                () -> Deencapsulation.invoke(job, "convertCustomProperties", false));
        Assertions.assertTrue(error.getMessage().contains("is not allowed in Routine Load"));
    }
}
