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

package org.apache.doris.cdcloader.mysql.serialize;

import io.debezium.data.Envelope;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvSerializer implements DorisRecordSerializer<SourceRecord, DorisRecord>{
    private static final long serialVersionUID = 1L;
    private static final String SEPARATOR = ",";
    private static final String DELETE_SIGN_TRUE = "1";
    private static final String DELETE_SIGN_FALSE = "0";

    @Override
    public DorisRecord serialize(SourceRecord record) throws IOException {
        List<String> rows = new ArrayList<>();
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        if(Envelope.Operation.DELETE.equals(op)){
            String deleteRow = extractBeforeRow(value, valueSchema);
            if(StringUtils.isNotEmpty(deleteRow)){
                rows.add(deleteRow);
            }
        }

        String insertRow = extractAfterRow(value, valueSchema);
        if(StringUtils.isNotEmpty(insertRow)){
            rows.add(insertRow);
        }
        String db = value.getStruct(Envelope.FieldName.SOURCE).getString("db");
        String table = value.getStruct(Envelope.FieldName.SOURCE).getString("table");
        DorisRecord dorisRecord = new DorisRecord(db, table, rows);
        return dorisRecord;
    }

    private String extractAfterRow(Struct value, Schema valueSchema) {
        StringBuilder record = new StringBuilder();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        if(after == null){
            return null;
        }
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        afterSchema.fields().forEach(field -> {
            record.append(after.get(field.name()));
            record.append(SEPARATOR);
        });
        record.append(DELETE_SIGN_FALSE);
        return record.toString();
    }

    private String extractBeforeRow(Struct value, Schema valueSchema) {
        StringBuilder record = new StringBuilder();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        if(before == null){
            return null;
        }
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        beforeSchema.fields().forEach(field -> {
            record.append(before.get(field.name()));
            record.append(SEPARATOR);
        });
        record.append(DELETE_SIGN_TRUE);
        return record.toString();
    }
}
