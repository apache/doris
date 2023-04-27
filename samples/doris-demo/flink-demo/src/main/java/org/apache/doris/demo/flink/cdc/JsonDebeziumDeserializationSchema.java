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
package org.apache.doris.demo.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;

/**
 *Customize DebeziumDeserializationSchema, return jsonObject
 */
public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<JSONObject> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonDebeziumDeserializationSchema.class);

    private static final long serialVersionUID = 7906905121308228264L;

    public JsonDebeziumDeserializationSchema() {
    }
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> collector) throws Exception {
        JSONObject resJson = new JSONObject();
        try {
            Struct valueStruct = (Struct) sourceRecord.value();
            Struct afterStruct = valueStruct.getStruct("after");
            Struct beforeStruct = valueStruct.getStruct("before");
            if (afterStruct != null && beforeStruct != null) {
                LOGGER.info("Updated, ignored ...");
            } else if (afterStruct != null) {
                List<Field> fields = afterStruct.schema().fields();
                String name;
                Object value;
                for (Field field : fields) {
                    name = field.name();
                    value = afterStruct.get(name);
                    resJson.put(name.toLowerCase(Locale.ROOT), value);
                }
            } else if (beforeStruct != null) {
                LOGGER.info("Deleted, ignored ...");
            } else {
                LOGGER.warn("No this operation ...");
            }
        } catch (Exception e) {
            LOGGER.error("Deserialize throws exception:", e);
        }
        collector.collect(resJson);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return BasicTypeInfo.of(JSONObject.class);
    }
}
