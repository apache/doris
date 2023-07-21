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

package org.apache.doris.persist;

import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.JsonParseException;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class DataSourcePropertiesTest {

    @Test
    public void testKafkaDataSourceSerializatio() throws IOException, UserException {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KafkaConfiguration.KAFKA_TOPIC.getName(), "mytopic");
        dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "0, 1");
        dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "10000, 20000");
        dataSourceProperties.put(KafkaConfiguration.KAFKA_BROKER_LIST.getName(), "127.0.0.1:9080");
        dataSourceProperties.put("property.group.id", "mygroup");
        KafkaDataSourceProperties kafkaDataSourceProperties = new KafkaDataSourceProperties(dataSourceProperties);
        kafkaDataSourceProperties.convertAndCheckDataSourceProperties();
        File file = new File("./kafka_datasource_properties");
        file.createNewFile();
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()))) {
            String json = GsonUtils.GSON.toJson(kafkaDataSourceProperties);
            Text.writeString(out, json);
            out.flush();
        }
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(file.toPath()))) {
            String json = Text.readString(dis);
            KafkaDataSourceProperties properties = (KafkaDataSourceProperties)
                    GsonUtils.GSON.fromJson(json, AbstractDataSourceProperties.class);
            Assertions.assertEquals(properties.getTopic(),
                    dataSourceProperties.get(KafkaConfiguration.KAFKA_TOPIC.getName()));
        }
    }

    @Test(expected = JsonParseException.class)
    public void testNotSupportDataSourceSerializatio() throws IOException {
        TestDataSourceProperties testDataSourceProperties = new TestDataSourceProperties(Maps.newHashMap());
        File file = new File("./test_datasource_properties");

        file.createNewFile();
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()))) {
            String json = GsonUtils.GSON.toJson(testDataSourceProperties);
            Text.writeString(out, json);
            out.flush();
        }

        try (DataInputStream dis = new DataInputStream(Files.newInputStream(file.toPath()))) {
            String json = Text.readString(dis);
            GsonUtils.GSON.fromJson(json, AbstractDataSourceProperties.class);
        }
    }

    class TestDataSourceProperties extends AbstractDataSourceProperties {

        public TestDataSourceProperties(Map<String, String> originalDataSourceProperties) {
            super(originalDataSourceProperties);
        }

        @Override
        protected String getDataSourceType() {
            return "test";
        }

        @Override
        protected List<String> getRequiredProperties() throws UserException {
            return null;
        }

        @Override
        public void convertAndCheckDataSourceProperties() throws UserException {

        }

    }
}
