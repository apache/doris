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
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    private StructObjectInspector rowInspector;
    private Deserializer deserializer;
    // private String serde;
    private int[] requiredColumnIds;
    // private RecordReader<Writable, Writable> reader;
    private RCFile.Reader reader;
    private Long splitStartOffset;
    private Long splitSize;
    private LazyBinaryColumnarSerDe serde;

    public HiveJNIScanner(int fetchSize, Map<String, String> requiredParams) {
        this.classLoader = this.getClass().getClassLoader();
        this.fetchSize = fetchSize;
        this.requiredParams = requiredParams;
        this.fileType = TFileType.findByValue(Integer.parseInt(requiredParams.get(HiveProperties.FILE_TYPE)));
        // this.columnNames = requiredParams.get(HiveProperties.COLUMNS_NAMES).split(HiveProperties.FIELDS_DELIMITER);
        // this.columnTypes = requiredParams.get(HiveProperties.COLUMNS_TYPES)
        //         .split(HiveProperties.COLUMNS_TYPE_DELIMITER);
        // this.serde = requiredParams.get(HiveProperties.HIVE_SERDE);
        this.columnNames = new String[] {
                "col_tinyint",
                "col_smallint",
                "col_int",
                "col_bigint",
                "col_float",
                "col_double",
                "col_decimal",
                "col_string",
                "col_char",
                "col_varchar",
                "col_boolean",
                "col_timestamp",
                "col_date",
                "col_array",
                "col_map",
                "col_struct"
        };
        this.columnTypes = new String[] {
                "tinyint",
                "smallint",
                "int",
                "bigint",
                "float",
                "double",
                "decimal(10,2)",
                "string",
                "char(10)",
                "varchar(20)",
                "boolean",
                "datetime",
                "date",
                "array<string>",
                "map<string,int>",
                "struct<name string, age int>"
        };
        this.requiredFields = new String[] {
                "col_tinyint",
                "col_smallint",
                "col_int",
                "col_bigint",
                "col_float",
                "col_double",
                "col_decimal",
                "col_string",
                "col_char",
                "col_varchar",
                "col_boolean",
                "col_timestamp",
                "col_date",
                "col_array",
                "col_map",
                "col_struct"
        };
        this.serde = new LazyBinaryColumnarSerDe();
        this.uri = requiredParams.get(HiveProperties.URI);
        this.requiredTypes = new ColumnType[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.requiredColumnIds = new int[requiredFields.length];
        // this.splitStartOffset = Long.parseLong(requiredParams.get(HiveProperties.SPLIT_START_OFFSET));
        // this.splitSize = Long.parseLong(requiredParams.get(HiveProperties.SPLIT_SIZE));
    }

    private static TPrimitiveType typeFromColumnType(ColumnType columnType, SchemaColumn schemaColumn)
            throws UnsupportedOperationException {
        switch (columnType.getType()) {
            case UNSUPPORTED:
                return TPrimitiveType.UNSUPPORTED;
            case BOOLEAN:
                return TPrimitiveType.BOOLEAN;
            case BYTE:
            case BINARY:
                return TPrimitiveType.BINARY;
            case TINYINT:
                return TPrimitiveType.TINYINT;
            case SMALLINT:
                return TPrimitiveType.SMALLINT;
            case INT:
                return TPrimitiveType.INT;
            case BIGINT:
                return TPrimitiveType.BIGINT;
            case LARGEINT:
                return TPrimitiveType.LARGEINT;
            case FLOAT:
                return TPrimitiveType.FLOAT;
            case DOUBLE:
                return TPrimitiveType.DOUBLE;
            case DATE:
                return TPrimitiveType.DATE;
            case DATEV2:
                return TPrimitiveType.DATEV2;
            case DATETIME:
                return TPrimitiveType.DATETIME;
            case DATETIMEV2:
                return TPrimitiveType.DATETIMEV2;
            case CHAR:
                return TPrimitiveType.CHAR;
            case VARCHAR:
                return TPrimitiveType.VARCHAR;
            case DECIMALV2:
                return TPrimitiveType.DECIMALV2;
            case DECIMAL32:
                return TPrimitiveType.DECIMAL32;
            case DECIMAL64:
                return TPrimitiveType.DECIMAL64;
            case DECIMAL128:
                return TPrimitiveType.DECIMAL128I;
            case STRING:
                return TPrimitiveType.STRING;
            case ARRAY:
                SchemaColumn arrayChildColumn = new SchemaColumn();
                schemaColumn.addChildColumns(Collections.singletonList(arrayChildColumn));
                arrayChildColumn.setType(typeFromColumnType(columnType.getChildTypes().get(0), arrayChildColumn));
                return TPrimitiveType.ARRAY;
            case MAP:
                SchemaColumn keyChildColumn = new SchemaColumn();
                keyChildColumn.setType(TPrimitiveType.STRING);
                SchemaColumn valueChildColumn = new SchemaColumn();
                valueChildColumn.setType(typeFromColumnType(columnType.getChildTypes().get(1), valueChildColumn));
                schemaColumn.addChildColumns(Arrays.asList(keyChildColumn, valueChildColumn));
                return TPrimitiveType.MAP;
            case STRUCT:
                List<ColumnType> fields = columnType.getChildTypes();
                List<SchemaColumn> childSchemaColumn = new ArrayList<>();
                for (ColumnType field : fields) {
                    SchemaColumn structChildColumn = new SchemaColumn();
                    structChildColumn.setName(field.getName());
                    schemaColumn.setType(typeFromColumnType(field, structChildColumn));
                    childSchemaColumn.add(structChildColumn);
                }
                schemaColumn.addChildColumns(childSchemaColumn);
                return TPrimitiveType.STRUCT;
            default:
                throw new UnsupportedOperationException("Unsupported column type: " + columnType.getType());
        }
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
        serde.initialize(new Configuration(), properties);
        // deserializer = getDeserializer(new Configuration(), properties, );
        // rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        rowInspector = (StructObjectInspector) serde.getObjectInspector();

        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
        }
    }

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(HiveProperties.COLUMNS,
                "col_tinyint,col_smallint,col_int,"
                        + "col_bigint,col_float,col_double,"
                        + "col_decimal,col_string,col_char,col_varchar,"
                        + "col_boolean,col_timestamp,"
                        + "col_date,col_array,col_map,col_struct");
        properties.setProperty(HiveProperties.COLUMNS2TYPES, "tinyint,"
                + "smallint,int,bigint,float,double,"
                + "decimal(10,2),string,char(10),varchar(20),boolean,timestamp,date:"
                + "array<string>,map<string,int>,struct<name:string,age:int>");
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
        LOG.info("start open func");
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
        int batchSize = getBatchSize();
        LOG.error("Start getNext func");
        final LongWritable key = new LongWritable();
        final BytesRefArrayWritable cols = new BytesRefArrayWritable();
        try {
            while (reader.next(key)) {
                if (numRows >= batchSize) {
                    break;
                }
                reader.getCurrentRow(cols);
                final LazyBinaryColumnarStruct row = (LazyBinaryColumnarStruct) serde.deserialize(cols);
                final ArrayList<Object> objects = row.getFieldsAsList();
                for (int i = 0; i < requiredFields.length; i++) {
                    // Object fieldData = rowInspector.getStructFieldData(row, structFields[i]);
                    Object fieldData = objects.get(i);
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
        LOG.error("Start to parse table schema");
        return createTableSchema(requiredFields, requiredTypes);
    }

    private TableSchema createTableSchema(String[] requiredFields, ColumnType[] requiredTypes) {
        LOG.error("Start to create table schema");
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        for (int i = 0; i < requiredFields.length; i++) {
            SchemaColumn schemaColumn = new SchemaColumn();
            LOG.error("get type " + requiredTypes[i].getName());
            TPrimitiveType tPrimitiveType = typeFromColumnType(requiredTypes[i], schemaColumn);
            schemaColumn.setName(requiredFields[i]);
            schemaColumn.setType(tPrimitiveType);
            LOG.error("type value = " + schemaColumn.getType());
            schemaColumns.add(schemaColumn);
        }
        LOG.error("schema columns size = " + schemaColumns.size());
        return new TableSchema(schemaColumns);
    }
}
