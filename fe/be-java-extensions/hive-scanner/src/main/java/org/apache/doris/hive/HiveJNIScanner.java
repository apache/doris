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

package org.apache.doris.hive;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.common.jni.vec.TableSchema.SchemaColumn;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPrimitiveType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class HiveJNIScanner extends JniScanner {

    private static final Logger LOG = LogManager.getLogger(HiveJNIScanner.class);
    private final ClassLoader classLoader;
    private final TFileType fileType;
    private final String uri;
    private final int fetchSize;
    private final Map<String, String> requiredParams;
    private final String[] columnTypes;
    private final String[] columnNames;
    private final String[] requiredFields;
    private final ColumnType[] requiredTypes;
    private final StructField[] structFields;
    private final ObjectInspector[] fieldInspectors;
    private Path path;
    private FileSystem fs;
    private LongWritable key;
    private BytesRefArrayWritable cols;
    private StructObjectInspector rowInspector;
    private Deserializer deserializer;
    private String serde;
    private int[] requiredColumnIds;
    private RecordReader<Writable, Writable> reader;
    private Long splitStartOffset;
    private Long splitSize;

    public HiveJNIScanner(int fetchSize, Map<String, String> requiredParams) {
        this.classLoader = this.getClass().getClassLoader();
        this.fetchSize = fetchSize;
        this.requiredParams = requiredParams;
        this.fileType = TFileType.findByValue(Integer.parseInt(requiredParams.get(HiveProperties.FILE_TYPE)));
        this.columnNames = requiredParams.get(HiveProperties.COLUMNS_NAMES).split(HiveProperties.FIELDS_DELIMITER);
        this.columnTypes = requiredParams.get(HiveProperties.COLUMNS_TYPES)
                .split(HiveProperties.COLUMNS_TYPE_DELIMITER);
        this.requiredFields = requiredParams.get(HiveProperties.REQUIRED_FIELDS).split(HiveProperties.FIELDS_DELIMITER);
        this.uri = requiredParams.get(HiveProperties.URI);
        this.serde = requiredParams.get(HiveProperties.HIVE_SERDE);
        this.requiredTypes = new ColumnType[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.requiredColumnIds = new int[requiredFields.length];
        this.splitStartOffset = Long.parseLong(requiredParams.get(HiveProperties.SPLIT_START_OFFSET));
        this.splitSize = Long.parseLong(requiredParams.get(HiveProperties.SPLIT_SIZE));
    }

    private static TPrimitiveType typeFromColumnType(ColumnType columnType) {
        // TODO:
        return TPrimitiveType.STRING;
    }

    private void initFieldInspector() throws Exception {
        HashMap<String, String> hiveColumnNamesToTypes = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            hiveColumnNamesToTypes.put(columnNames[i], columnTypes[i]);
        }
        for (int i = 0; i < requiredFields.length; i++) {
            String typeStr = hiveColumnNamesToTypes.get(requiredFields[i]);
            ColumnType columnType = ColumnType.parseType(requiredFields[i], typeStr);
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

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
                Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        properties.setProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, String.join(",", requiredFields));
        properties.setProperty(HiveProperties.COLUMNS, String.join(",", requiredFields));
        properties.setProperty(HiveProperties.COLUMNS2TYPES, String.join(",", columnTypes));
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
        try {
            initFieldInspector();
            initTableInfo(requiredTypes, requiredFields, fetchSize);
        } catch (Exception e) {
            LOG.error("Failed to init hive scanner.", e);
            throw new IOException("Failed to init hive scanner.", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.reader != null) {
            this.reader.close();
        }
    }

    @Override
    protected int getNext() throws IOException {
        int numRows = 0;
        Writable key = reader.createKey();
        Writable value = reader.createValue();
        try {
            for (; numRows < getBatchSize(); numRows++) {
                if (!reader.next(key, value)) {
                    break;
                }
                Object row = deserializer.deserialize(value);
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(row, structFields[i]);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        HiveColumnValue fieldValue = new HiveColumnValue(fieldInspectors[i], fieldData,
                                "Asia/Shanghai");
                        appendData(i, fieldValue);
                    }
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            LOG.error("Failed to get data.", e);
            throw new IOException("Failed to get data.", e);
        }
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        return createTableSchema(requiredFields, requiredTypes);
    }

    private TableSchema createTableSchema(String[] requiredFields, ColumnType[] requiredTypes) {
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        for (int i = 0; i < requiredFields.length; i++) {
            SchemaColumn schemaColumn = new SchemaColumn();
            TPrimitiveType tPrimitiveType = typeFromColumnType(requiredTypes[i]);
            schemaColumn.setName(requiredFields[i]);
            schemaColumn.setType(tPrimitiveType);
            schemaColumns.add(schemaColumn);
        }
        return new TableSchema(schemaColumns);
    }
}
