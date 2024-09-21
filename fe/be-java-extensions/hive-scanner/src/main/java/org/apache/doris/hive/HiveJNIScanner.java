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
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import io.trino.spi.classloader.ThreadContextClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
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
import java.util.Arrays;
import java.util.HashMap;
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
    private final String[] requiredFields;
    private final ColumnType[] requiredTypes;
    private final int[] requiredColumnIds;
    private final TFileFormatType fileFormat;
    private final StructField[] structFields;
    private final ObjectInspector[] fieldInspectors;
    private final Long splitStartOffset;
    private final Long splitSize;
    private StructObjectInspector rowInspector;
    private Deserializer deserializer;
    private RecordReader<Writable, Writable> reader;
    private Writable key;
    private Writable value;
    private HiveFileContext hiveFileContext;

    public HiveJNIScanner(int fetchSize, Map<String, String> requiredParams) {
        this.classLoader = this.getClass().getClassLoader();
        this.fetchSize = fetchSize;
        this.requiredParams = requiredParams;
        this.fileType = TFileType.findByValue(Integer.parseInt(requiredParams.get(HiveProperties.FILE_TYPE)));
        this.fileFormat = TFileFormatType.findByValue(Integer.parseInt(requiredParams.get(HiveProperties.FILE_FORMAT)));
        this.columnTypes = requiredParams.get(HiveProperties.COLUMNS_TYPES)
                .split(HiveProperties.COLUMNS_TYPE_DELIMITER);
        this.requiredFields = requiredParams.get(HiveProperties.REQUIRED_FIELDS).split(HiveProperties.FIELDS_DELIMITER);
        this.requiredTypes = new ColumnType[requiredFields.length];
        this.requiredColumnIds = new int[requiredFields.length];
        this.uri = requiredParams.get(HiveProperties.URI);
        this.splitStartOffset = Long.parseLong(requiredParams.get(HiveProperties.SPLIT_START_OFFSET));
        this.splitSize = Long.parseLong(requiredParams.get(HiveProperties.SPLIT_SIZE));
        this.structFields = new StructField[requiredFields.length];
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
    }

    private void initReader() throws Exception {
        this.hiveFileContext = new HiveFileContext(fileFormat);
        Properties properties = createProperties();
        JobConf jobConf = makeJobConf(properties);
        switch (fileType) {
            case FILE_LOCAL:
            case FILE_HDFS:
                break;
            case FILE_S3:
                String accessKey = requiredParams.get(HiveProperties.S3_ACCESS_KEY);
                String secretKey = requiredParams.get(HiveProperties.S3_SECRET_KEY);
                String endpoint = requiredParams.get(HiveProperties.S3_ENDPOINT);
                String region = requiredParams.get(HiveProperties.S3_REGION);
                break;
            default:
                throw new Exception("Unsupported " + fileType.getValue() + " file type.");
        }
        Path path = new Path(uri);
        FileSplit fileSplit = new FileSplit(path, splitStartOffset, splitSize, (String[]) null);
        InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf, hiveFileContext.getInputFormat());
        reader = (RecordReader<Writable, Writable>) inputFormatClass.getRecordReader(fileSplit, jobConf, Reporter.NULL);
        deserializer = getDeserializer(jobConf, properties, hiveFileContext.getSerde());
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
        properties.setProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
                Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        properties.setProperty(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, String.join(",", requiredFields));
        properties.setProperty(HiveProperties.COLUMNS, String.join(",", requiredFields));
        properties.setProperty(HiveProperties.COLUMNS2TYPES, String.join(",", columnTypes));
        properties.setProperty(serdeConstants.SERIALIZATION_LIB, hiveFileContext.getSerde());
        return properties;
    }

    private JobConf makeJobConf(Properties properties) {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf);
        jobConf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
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
            initReader();
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
            key = reader.createKey();
            value = reader.createValue();
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
            LOG.error("Failed to get next row of data.", e);
            throw new IOException("Failed to get next row of data.", e);
        }
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        return null;
    }

    private void parseRequiredTypes() {
        HashMap<String, Integer> hiveColumnNameToIndex = new HashMap<>();
        HashMap<String, String> hiveColumnNameToType = new HashMap<>();
        for (int i = 0; i < requiredFields.length; i++) {
            hiveColumnNameToIndex.put(requiredFields[i], i);
            hiveColumnNameToType.put(requiredFields[i], columnTypes[i]);
        }

        for (int i = 0; i < requiredFields.length; i++) {
            String fieldName = requiredFields[i];
            requiredColumnIds[i] = hiveColumnNameToIndex.get(fieldName);
            String typeStr = hiveColumnNameToType.get(fieldName);
            requiredTypes[i] = ColumnType.parseType(fieldName, typeStr);
        }
    }
}
