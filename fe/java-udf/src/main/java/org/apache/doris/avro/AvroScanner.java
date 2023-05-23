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

package org.apache.doris.avro;

import org.apache.doris.jni.JniScanner;
import org.apache.doris.jni.vec.ColumnType;
import org.apache.doris.jni.vec.ScanPredicate;
import org.apache.doris.jni.vec.VectorTableSchema;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPrimitiveType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AvroScanner extends JniScanner {

    private static final Logger LOG = LogManager.getLogger(AvroScanner.class);
    private final TFileType fileType;
    private final String uri;
    private final Map<String, String> requiredParams;
    private Integer fetchSize;
    private String[] columnTypes;
    private String[] requiredFields;
    private ColumnType[] requiredTypes;
    private AvroReader avroReader;
    private boolean isGetTableSchema = false;

    /**
     * Call by JNI for get table data
     *
     * @param fetchSize The size of data fetched each time
     * @param requiredParams required params
     */
    public AvroScanner(int fetchSize, Map<String, String> requiredParams) {
        this.fetchSize = fetchSize;
        this.requiredParams = requiredParams;
        this.columnTypes = requiredParams.get(AvroProperties.COLUMNS_TYPES)
                .split(AvroProperties.COLUMNS_TYPE_DELIMITER);
        this.requiredFields = requiredParams.get(AvroProperties.REQUIRED_FIELDS).split(AvroProperties.FIELDS_DELIMITER);
        this.fileType = TFileType.findByValue(Integer.parseInt(requiredParams.get(AvroProperties.FILE_TYPE)));
        this.uri = requiredParams.get(AvroProperties.URI);
        this.requiredTypes = new ColumnType[requiredFields.length];
        buildParams();
    }

    /**
     * Call by JNI for get table Schema
     *
     * @param requiredParams required params
     */
    public AvroScanner(Map<String, String> requiredParams) {
        this.requiredParams = requiredParams;
        this.uri = requiredParams.get(AvroProperties.URI);
        this.fileType = TFileType.findByValue(Integer.parseInt(requiredParams.get(AvroProperties.FILE_TYPE)));
        isGetTableSchema = true;
    }

    private void buildParams() {
        for (int i = 0; i < requiredFields.length; i++) {
            ColumnType columnType = ColumnType.parseType(requiredFields[i], columnTypes[i]);
            requiredTypes[i] = columnType;
        }
    }

    @Override
    public void open() throws IOException {
        switch (fileType) {
            case FILE_HDFS:
                this.avroReader = new HDFSFileReader(uri);
                break;
            case FILE_S3:
                String bucketName = requiredParams.get(AvroProperties.S3_BUCKET);
                String key = requiredParams.get(AvroProperties.S3_KEY);
                String accessKey = requiredParams.get(AvroProperties.S3_ACCESS_KEY);
                String secretKey = requiredParams.get(AvroProperties.S3_SECRET_KEY);
                String endpoint = requiredParams.get(AvroProperties.S3_ENDPOINT);
                String region = requiredParams.get(AvroProperties.S3_REGION);
                this.avroReader = new S3FileReader(accessKey, secretKey, endpoint, region, bucketName, key);
                break;
            default:
                LOG.warn("Unsupported " + fileType.name() + " file type.");
                throw new RuntimeException("Unsupported " + fileType.name() + " file type.");
        }
        this.avroReader.open(new Configuration());
        if (!isGetTableSchema) {
            initTableInfo(requiredTypes, requiredFields, new ScanPredicate[0], fetchSize);
        }
    }

    @Override
    public void close() throws IOException {
        avroReader.close();
    }

    @Override
    protected int getNext() throws IOException {
        int numRows = 0;
        for (; numRows < getBatchSize(); numRows++) {
            if (!avroReader.hasNext()) {
                break;
            }
            GenericRecord rowRecord = (GenericRecord) avroReader.getNext();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = rowRecord.get(requiredFields[i]);
                if (fieldData == null) {
                    appendData(i, null);
                } else {
                    AvroColumnValue fieldValue = new AvroColumnValue(fieldData, requiredTypes[i]);
                    appendData(i, fieldValue);
                }
            }
        }
        return numRows;
    }

    public VectorTableSchema parseTableSchema() throws IOException {
        Schema schema = avroReader.getSchema();
        List<Field> schemaFields = schema.getFields();
        TPrimitiveType[] schemaTypes = new TPrimitiveType[schemaFields.size()];
        String[] fields = new String[schemaFields.size()];
        for (int i = 0; i < schemaFields.size(); i++) {
            fields[i] = schemaFields.get(i).name();
            Schema.Type type = schemaFields.get(i).schema().getType();
            switch (type) {
                case STRING:
                    schemaTypes[i] = TPrimitiveType.STRING;
                    break;
                case INT:
                    schemaTypes[i] = TPrimitiveType.INT;
                    break;
                case LONG:
                    schemaTypes[i] = TPrimitiveType.BIGINT;
                    break;
                case BOOLEAN:
                    schemaTypes[i] = TPrimitiveType.BOOLEAN;
                    break;
                case FLOAT:
                    schemaTypes[i] = TPrimitiveType.FLOAT;
                    break;
                case DOUBLE:
                    schemaTypes[i] = TPrimitiveType.DOUBLE;
                    break;
                case ARRAY:
                case MAP:
                default:
                    throw new IOException("avro format: " + type.getName() + " is not supported.");

            }
        }
        return new VectorTableSchema(fields, schemaTypes);
    }

}
