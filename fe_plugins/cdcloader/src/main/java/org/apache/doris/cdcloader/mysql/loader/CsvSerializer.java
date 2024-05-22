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

package org.apache.doris.cdcloader.mysql.loader;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;

public class CsvSerializer implements DorisRecordSerializer<SourceRecord>{
    private static final long serialVersionUID = 1L;
    private static final String SEPARATOR = ",";

    @Override
    public DorisRecord serialize(SourceRecord record) throws IOException {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        String result = extractAfterRow(value, valueSchema);
        String db = value.getStruct(Envelope.FieldName.SOURCE).getString("db");
        String table = value.getStruct(Envelope.FieldName.SOURCE).getString("table");
        DorisRecord dorisRecord = new DorisRecord(db, table, result);
        return dorisRecord;
    }

    private String extractAfterRow(Struct value, Schema valueSchema) {
        StringBuilder record = new StringBuilder();
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        //String sourceSchema = value.getString(Envelope.FieldName.SOURCE);

        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        afterSchema.fields().forEach(field -> {
            record.append(after.get(field.name()));
            record.append(SEPARATOR);
        });
        record.deleteCharAt(record.length() - 1);
        return record.toString();
    }
}
