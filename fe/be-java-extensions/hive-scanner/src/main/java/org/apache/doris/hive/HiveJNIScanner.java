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
import org.apache.doris.common.jni.vec.ColumnType.Type;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.common.jni.vec.TableSchema.SchemaColumn;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPrimitiveType;

import io.trino.spi.classloader.ThreadContextClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
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
    private final boolean isGetTableSchema;
    private final String serde;
    private StructObjectInspector rowInspector;
    private Deserializer deserializer;
    private int[] requiredColumnIds;
    private RecordReader<Writable, Writable> reader;
    private Long splitStartOffset;
    private Long splitSize;

    public HiveJNIScanner(int fetchSize, Map<String, String> requiredParams) {
        this.classLoader = this.getClass().getClassLoader();
        this.fetchSize = fetchSize;
        this.requiredParams = requiredParams;
        this.fileType = TFileType.findByValue(Integer.parseInt(requiredParams.get(HiveProperties.FILE_TYPE)));
        this.isGetTableSchema = Boolean.parseBoolean(requiredParams.get(HiveProperties.IS_GET_TABLE_SCHEMA));
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
                "struct<name:string,age:int>"
        };
        this.requiredFields = new String[] {
                "col_tinyint",
        };
        this.uri = requiredParams.get(HiveProperties.URI);
        this.requiredTypes = new ColumnType[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.requiredColumnIds = new int[requiredFields.length];
        this.splitStartOffset = 0L;
        this.splitSize = 4096L;
        this.serde = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe";
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

    private void initReader(JobConf jobConf, Properties properties) throws Exception {
        Path path = new Path(uri);
        FileSplit fileSplit = new FileSplit(path, splitStartOffset, splitSize, (String[]) null);

        InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf,
                "org.apache.hadoop.hive.ql.io.RCFileInputFormat");
        reader = (RecordReader<Writable, Writable>) inputFormatClass.getRecordReader(fileSplit, jobConf, Reporter.NULL);

        deserializer = getDeserializer(jobConf, properties, serde);
        rowInspector = getTableObjectInspector(deserializer);
        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
        }
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat) throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer) throws Exception {
        ObjectInspector inspector = deserializer.getObjectInspector();
        return (StructObjectInspector) inspector;
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
                + "decimal(10,2),string,char(10),varchar(20),boolean,timestamp,date,"
                + "array<string>,map<string,int>,struct<name:string,age:int>");
        properties.setProperty("hive.io.file.readcolumn.ids",
                Arrays.stream(this.requiredColumnIds)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.joining(",")));
        properties.setProperty("hive.io.readcolumn.names", String.join(",", this.requiredFields));
        properties.setProperty("serialization.lib", this.serde);
        return properties;
    }

    private JobConf makeJobConf(Properties properties) {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf);
        jobConf.setBoolean("hive.io.file.read.all.columns", false);
        properties.stringPropertyNames().forEach(name -> jobConf.set(name, properties.getProperty(name)));
        return jobConf;
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
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            parseRequiredTypes();
            initTableInfo(requiredTypes, requiredFields, fetchSize);
            Properties properties = createProperties();
            JobConf jobConf = makeJobConf(properties);
            initReader(jobConf, properties);
        } catch (Exception e) {
            close();
            LOG.error("Failed to open the hive reader.", e);
            throw new IOException("Failed to open the hive reader.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close the hive reader.", e);
            throw new IOException("Failed to close the hive reader.", e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Writable key = (Writable) reader.createKey();
            Writable value = (Writable) reader.createValue();
            int numRows = 0;
            for (; numRows < getBatchSize(); numRows++) {
                if (!reader.next(key, value)) {
                    break;
                }
                Object rowData = deserializer.deserialize(value);
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        HiveColumnValue fieldValue = new HiveColumnValue(fieldInspectors[i], fieldData);
                        appendData(i, fieldValue);
                    }
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next off-heap table chunk of hive.", e);
            throw new IOException("Failed to get the next off-heap table chunk of hive.", e);
        }
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        LOG.error("Start to parse table schema");
        return createTableSchema(requiredFields, requiredTypes);
    }

    private void parseRequiredTypes() {
        HashMap<String, Integer> hiveColumnNameToIndex = new HashMap<>();
        HashMap<String, String> hiveColumnNameToType = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            hiveColumnNameToIndex.put(columnNames[i], i);
            hiveColumnNameToType.put(columnNames[i], columnTypes[i]);
        }

        for (int i = 0; i < requiredFields.length; i++) {
            requiredColumnIds[i] = hiveColumnNameToIndex.get(requiredFields[i]);
            requiredTypes[i] = new ColumnType(requiredFields[i], Type.TINYINT);
        }
    }

    private TableSchema createTableSchema(String[] requiredFields, ColumnType[] requiredTypes) {
        LOG.error("Start to create table schema");
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        for (int i = 0; i < requiredFields.length; i++) {
            SchemaColumn schemaColumn = new SchemaColumn();
            schemaColumn.setName(requiredFields[i]);
            schemaColumn.setType(TPrimitiveType.TINYINT);
            LOG.error("type value = " + schemaColumn.getType());
            schemaColumns.add(schemaColumn);
        }
        LOG.error("schema columns size = " + schemaColumns.size());
        return new TableSchema(schemaColumns);
    }
}
