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

package org.pentaho.di.trans.steps.dorisstreamloader.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.steps.dorisstreamloader.load.EscapeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.CSV;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.DORIS_DELETE_SIGN;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.JSON;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.NULL_VALUE;

/** Serializer for RowData. */
public class DorisRecordSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(DorisRecordSerializer.class);
    String[] fieldNames;
    String type;
    private ObjectMapper objectMapper;
    private final String fieldDelimiter;
    private final ValueMetaInterface[] formatMeta;
    private LogChannelInterface log;
    private final boolean deletable;

    private DorisRecordSerializer(
            String[] fieldNames,
            ValueMetaInterface[] formatMeta,
            String type,
            String fieldDelimiter,
            LogChannelInterface log,
            boolean deletable) {
        this.fieldNames = fieldNames;
        this.type = type;
        this.fieldDelimiter = fieldDelimiter;
        if (JSON.equals(type)) {
            objectMapper = new ObjectMapper();
        }
        this.formatMeta = formatMeta;
        this.log = log;
        this.deletable = deletable;
    }


    public byte[] serialize(Object[] record) throws IOException, KettleException {
        int maxIndex = Math.min(record.length, fieldNames.length);
        String valString;
        if (JSON.equals(type)) {
            valString = buildJsonString(record, maxIndex);
        } else if (CSV.equals(type)) {
            valString = buildCSVString(record, maxIndex);
        } else {
            throw new IllegalArgumentException("The type " + type + " is not supported!");
        }
        log.logRowlevel("Serialized record: " + valString);
        return valString.getBytes(StandardCharsets.UTF_8);
    }


    public String buildJsonString(Object[] record, int maxIndex) throws IOException, KettleException {
        int fieldIndex = 0;
        Map<String, String> valueMap = new HashMap<>();
        while (fieldIndex < maxIndex) {
            Object field = convertExternal(record[fieldIndex], formatMeta[fieldIndex]);
            String value = field != null ? field.toString() : null;
            valueMap.put(fieldNames[fieldIndex], value);
            fieldIndex++;
        }
        if (deletable) {
            // All load data will be deleted
            valueMap.put(DORIS_DELETE_SIGN, "1");
        }
        return objectMapper.writeValueAsString(valueMap);
    }

    public String buildCSVString(Object[] record, int maxIndex) throws IOException, KettleException {
        int fieldIndex = 0;
        StringJoiner joiner = new StringJoiner(fieldDelimiter);
        while (fieldIndex < maxIndex) {
            Object field = convertExternal(record[fieldIndex], formatMeta[fieldIndex]);
            String value = field != null ? field.toString() : NULL_VALUE;
            joiner.add(value);
            fieldIndex++;
        }
        if (deletable) {
            // All load data will be deleted
            joiner.add("1");
        }
        return joiner.toString();
    }

    private Object convertExternal(Object r, ValueMetaInterface sourceMeta) throws KettleException {
        if (r == null) {
            return null;
        }
        try {
            switch (sourceMeta.getType()) {
                case ValueMetaInterface.TYPE_BOOLEAN:
                    return sourceMeta.getBoolean(r);
                case ValueMetaInterface.TYPE_INTEGER:
                    return sourceMeta.getInteger(r);
                case ValueMetaInterface.TYPE_NUMBER:
                    return sourceMeta.getNumber(r);
                case ValueMetaInterface.TYPE_BIGNUMBER:
                    return sourceMeta.getBigNumber(r);
                case ValueMetaInterface.TYPE_DATE:
                    Date dateValue = sourceMeta.getDate(r);;
                    return new java.sql.Date(dateValue.getTime());
                case ValueMetaInterface.TYPE_TIMESTAMP:
                    java.sql.Timestamp timestampValue = (Timestamp) sourceMeta.getDate(r);
                    return timestampValue;
                case ValueMetaInterface.TYPE_BINARY:
                case ValueMetaInterface.TYPE_STRING:
                    return sourceMeta.getString(r);
                default:
                    // Unknow type, use origin value
                    return r;
            }
        } catch (Exception e) {
            throw new KettleException("Error serializing rows of data to the Doris: ", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for RowDataSerializer. */
    public static class Builder {
        private String[] fieldNames;
        private ValueMetaInterface[] formatMeta;
        private String type;
        private String fieldDelimiter;
        private LogChannelInterface log;
        private boolean deletable;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFormatMeta(ValueMetaInterface[] formatMeta) {
            this.formatMeta = formatMeta;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setFieldDelimiter(String fieldDelimiter) {
            this.fieldDelimiter = EscapeHandler.escapeString(fieldDelimiter);
            return this;
        }

        public Builder setLogChannelInterface(LogChannelInterface log) {
            this.log = log;
            return this;
        }

        public Builder setDeletable(boolean deletable) {
            this.deletable = deletable;
            return this;
        }

        public DorisRecordSerializer build() {
            Preconditions.checkState(
                    CSV.equals(type) && fieldDelimiter != null
                            || JSON.equals(type));
            Preconditions.checkNotNull(formatMeta);
            Preconditions.checkNotNull(fieldNames);

            return new DorisRecordSerializer(fieldNames, formatMeta, type, fieldDelimiter, log, deletable);
        }
    }
}
