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

package org.apache.doris.cdcclient.source.deserialize;

import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL-specific deserializer that handles DDL schema change events.
 *
 * <p>When a schema change event is detected, it parses the HistoryRecord, computes the diff against
 * stored tableSchemas, generates Doris ALTER TABLE SQL, and returns a SCHEMA_CHANGE result.
 */
public class MySqlDebeziumJsonDeserializer extends DebeziumJsonDeserializer {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MySqlDebeziumJsonDeserializer.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private String targetDb;

    @Override
    public void init(Map<String, String> props) {
        super.init(props);
        this.targetDb = props.get(DataSourceConfigKeys.DATABASE);
    }

    @Override
    public DeserializeResult deserialize(Map<String, String> context, SourceRecord record)
            throws IOException {
        if (RecordUtils.isSchemaChangeEvent(record)) {
            return handleSchemaChangeEvent(record, context);
        }
        return super.deserialize(context, record);
    }

    private DeserializeResult handleSchemaChangeEvent(
            SourceRecord record, Map<String, String> context) {
        // todo: record has mysql ddl, need to convert doris ddl
        return DeserializeResult.empty();
    }
}
