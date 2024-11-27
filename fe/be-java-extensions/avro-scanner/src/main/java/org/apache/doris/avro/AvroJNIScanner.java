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

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.thrift.TFileType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class AvroJNIScanner extends JniScanner {

    private static final Logger LOG = LogManager.getLogger(AvroJNIScanner.class);
    private final TFileType fileType;
    private final String uri;
    private final Map<String, String> requiredParams;
    private final Integer fetchSize;
    private final ClassLoader classLoader;
    private int[] requiredColumnIds;
    private String[] columnTypes;
    private String[] requiredFields;
    private Set<String> requiredFieldSet;
    private ColumnType[] requiredTypes;
    private AvroReader avroReader;
    private final boolean isGetTableSchema;
    private StructObjectInspector rowInspector;
    private Deserializer deserializer;
    private StructField[] structFields;
    private ObjectInspector[] fieldInspectors;
    private String serde;
    private AvroFileContext avroFileContext;
    private AvroWrapper<Pair<Integer, Long>> inputPair;
    private NullWritable ignore;
    private Long splitStartOffset;
    private Long splitSize;
    private Long splitFileSize;

    /**
     * Call by JNI for get table data or get table schema
     *
     * @param fetchSize The size of data fetched each time
     * @param requiredParams required params
     */
    public AvroJNIScanner(int fetchSize, Map<String, String> requiredParams) {
        this.classLoader = this.getClass().getClassLoader();
        this.requiredParams = requiredParams;
        this.fetchSize = fetchSize;
        this.isGetTableSchema = Boolean.parseBoolean(requiredParams.get(AvroProperties.IS_GET_TABLE_SCHEMA));
        this.fileType = TFileType.findByValue(Integer.parseInt(requiredParams.get(AvroProperties.FILE_TYPE)));
        this.uri = requiredParams.get(AvroProperties.URI);
        if (!isGetTableSchema) {
            this.columnTypes = requiredParams.get(AvroProperties.COLUMNS_TYPES)
                    .split(AvroProperties.COLUMNS_TYPE_DELIMITER);
            this.requiredFields = requiredParams.get(AvroProperties.REQUIRED_FIELDS)
                    .split(AvroProperties.FIELDS_DELIMITER);
            this.requiredFieldSet = new HashSet<>(Arrays.asList(requiredFields));
            this.requiredTypes = new ColumnType[requiredFields.length];
            this.serde = requiredParams.get(AvroProperties.HIVE_SERDE);
            this.structFields = new StructField[requiredFields.length];
            this.fieldInspectors = new ObjectInspector[requiredFields.length];
            this.inputPair = new AvroWrapper<>(null);
            this.ignore = NullWritable.get();
            this.splitStartOffset = Long.parseLong(requiredParams.get(AvroProperties.SPLIT_START_OFFSET));
            this.splitSize = Long.parseLong(requiredParams.get(AvroProperties.SPLIT_SIZE));
            this.splitFileSize = Long.parseLong(requiredParams.get(AvroProperties.SPLIT_FILE_SIZE));
        }
    }

    private void initFieldInspector() throws Exception {
        requiredColumnIds = new int[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            ColumnType columnType = ColumnType.parseType(requiredFields[i], columnTypes[i]);
            requiredTypes[i] = columnType;
            requiredColumnIds[i] = i;
        }

        Properties properties = createProperties();
        deserializer = getDeserializer(new Configuration(), properties, this.serde);
        rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
        }
    }

    public Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
                Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        properties.setProperty(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, String.join(",", requiredFields));
        properties.setProperty(AvroProperties.COLUMNS, String.join(",", requiredFields));
        properties.setProperty(AvroProperties.COLUMNS2TYPES, String.join(",", columnTypes));
        properties.setProperty(serdeConstants.SERIALIZATION_LIB, this.serde);
        return properties;
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name)
            throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(name, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    @Override
    public void open() throws IOException {
        Thread.currentThread().setContextClassLoader(classLoader);
        switch (fileType) {
            case FILE_HDFS:
                this.avroReader = new HDFSFileReader(uri);
                break;
            case FILE_S3:
                String accessKey = requiredParams.get(AvroProperties.S3_ACCESS_KEY);
                String secretKey = requiredParams.get(AvroProperties.S3_SECRET_KEY);
                String endpoint = requiredParams.get(AvroProperties.S3_ENDPOINT);
                String region = requiredParams.get(AvroProperties.S3_REGION);
                this.avroReader = new S3FileReader(accessKey, secretKey, endpoint, region, uri);
                break;
            default:
                LOG.warn("Unsupported " + fileType.name() + " file type.");
                throw new IOException("Unsupported " + fileType.name() + " file type.");
        }
        if (!isGetTableSchema) {
            initDataReader();
        }
        this.avroReader.open(avroFileContext, isGetTableSchema);
    }

    private void initDataReader() {
        try {
            initAvroFileContext();
            initFieldInspector();
            initTableInfo(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            LOG.warn("Failed to init avro scanner. ", e);
            throw new RuntimeException(e);
        }
    }

    private void initAvroFileContext() {
        avroFileContext = new AvroFileContext();
        avroFileContext.setRequiredFields(requiredFieldSet);
        avroFileContext.setSplitStartOffset(splitStartOffset);
        avroFileContext.setSplitSize(splitSize);
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(avroReader)) {
            avroReader.close();
        }
    }

    @Override
    protected int getNext() throws IOException {
        int numRows = 0;
        for (; numRows < getBatchSize(); numRows++) {
            if (!avroReader.hasNext(inputPair, ignore)) {
                break;
            }
            GenericRecord rowRecord = (GenericRecord) avroReader.getNext();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = rowRecord.get(requiredFields[i]);
                if (fieldData == null) {
                    appendData(i, null);
                } else {
                    AvroColumnValue fieldValue = new AvroColumnValue(fieldInspectors[i], fieldData);
                    appendData(i, fieldValue);
                }
            }
        }
        return numRows;
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        Schema schema = avroReader.getSchema();
        return AvroTypeUtils.parseTableSchema(schema);
    }

}
