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

package org.apache.doris.hudi;

import org.apache.doris.common.classloader.ThreadClassLoaderContext;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * HadoopHudiJniScanner is a JniScanner implementation that reads Hudi data using hudi-hadoop-mr.
 */
public class HadoopHudiJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopHudiJniScanner.class);

    private static final String HADOOP_CONF_PREFIX = "hadoop_conf.";

    // Hudi data info
    private final String basePath;
    private final String dataFilePath;
    private final long dataFileLength;
    private final String[] deltaFilePaths;
    private final String instantTime;
    private final String serde;
    private final String inputFormat;

    // schema info
    private final String hudiColumnNames;
    private final String[] hudiColumnTypes;
    private final String[] requiredFields;
    private List<Integer> requiredColumnIds;
    private ColumnType[] requiredTypes;

    // Hadoop info
    private RecordReader<NullWritable, ArrayWritable> reader;
    private StructObjectInspector rowInspector;
    private final ObjectInspector[] fieldInspectors;
    private final StructField[] structFields;
    private Deserializer deserializer;
    private final Map<String, String> fsOptionsProps;

    // scanner info
    private final HadoopHudiColumnValue columnValue;
    private final int fetchSize;
    private final ClassLoader classLoader;

    public HadoopHudiJniScanner(int fetchSize, Map<String, String> params) {
        this.basePath = params.get("base_path");
        this.dataFilePath = params.get("data_file_path");
        this.dataFileLength = Long.parseLong(params.get("data_file_length"));
        if (Strings.isNullOrEmpty(params.get("delta_file_paths"))) {
            this.deltaFilePaths = new String[0];
        } else {
            this.deltaFilePaths = params.get("delta_file_paths").split(",");
        }
        this.instantTime = params.get("instant_time");
        this.serde = params.get("serde");
        this.inputFormat = params.get("input_format");

        this.hudiColumnNames = params.get("hudi_column_names");
        this.hudiColumnTypes = params.get("hudi_column_types").split("#");
        this.requiredFields = params.get("required_fields").split(",");

        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.fsOptionsProps = Maps.newHashMap();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(HADOOP_CONF_PREFIX)) {
                fsOptionsProps.put(entry.getKey().substring(HADOOP_CONF_PREFIX.length()), entry.getValue());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get hudi params {}: {}", entry.getKey(), entry.getValue());
            }
        }

        ZoneId zoneId;
        if (Strings.isNullOrEmpty(params.get("time_zone"))) {
            zoneId = ZoneId.systemDefault();
        } else {
            zoneId = ZoneId.of(params.get("time_zone"));
        }
        this.columnValue = new HadoopHudiColumnValue(zoneId);
        this.fetchSize = fetchSize;
        this.classLoader = this.getClass().getClassLoader();
    }

    @Override
    public void open() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            initRequiredColumnsAndTypes();
            initTableInfo(requiredTypes, requiredFields, fetchSize);
            Properties properties = getReaderProperties();
            initReader(properties);
        } catch (Exception e) {
            close();
            LOG.warn("failed to open hadoop hudi jni scanner", e);
            throw new IOException("failed to open hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            NullWritable key = reader.createKey();
            ArrayWritable value = reader.createValue();
            int numRows = 0;
            for (; numRows < fetchSize; numRows++) {
                if (!reader.next(key, value)) {
                    break;
                }
                Object rowData = deserializer.deserialize(value);
                for (int i = 0; i < fields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
                    columnValue.setRow(fieldData);
                    // LOG.info("rows: {}, column: {}, col name: {}, col type: {}, inspector: {}",
                    //        numRows, i, types[i].getName(), types[i].getType().name(),
                    //        fieldInspectors[i].getTypeName());
                    columnValue.setField(types[i], fieldInspectors[i]);
                    appendData(i, columnValue);
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            LOG.warn("failed to get next in hadoop hudi jni scanner", e);
            throw new IOException("failed to get next in hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.warn("failed to close hadoop hudi jni scanner", e);
            throw new IOException("failed to close hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    private void initRequiredColumnsAndTypes() {
        String[] splitHudiColumnNames = hudiColumnNames.split(",");

        Map<String, Integer> hudiColNameToIdx =
                IntStream.range(0, splitHudiColumnNames.length)
                        .boxed()
                        .collect(Collectors.toMap(i -> splitHudiColumnNames[i], i -> i));

        Map<String, String> hudiColNameToType =
                IntStream.range(0, splitHudiColumnNames.length)
                        .boxed()
                        .collect(Collectors.toMap(i -> splitHudiColumnNames[i], i -> hudiColumnTypes[i]));

        requiredTypes = Arrays.stream(requiredFields)
                .map(field -> ColumnType.parseType(field, hudiColNameToType.get(field)))
                .toArray(ColumnType[]::new);

        requiredColumnIds = Arrays.stream(requiredFields)
                .mapToInt(hudiColNameToIdx::get)
                .boxed().collect(Collectors.toList());
    }

    private Properties getReaderProperties() {
        Properties properties = new Properties();
        properties.setProperty("hive.io.file.readcolumn.ids", Joiner.on(",").join(requiredColumnIds));
        properties.setProperty("hive.io.file.readcolumn.names", Joiner.on(",").join(this.requiredFields));
        properties.setProperty("columns", this.hudiColumnNames);
        properties.setProperty("columns.types", Joiner.on(",").join(hudiColumnTypes));
        properties.setProperty("serialization.lib", this.serde);
        properties.setProperty("hive.io.file.read.all.columns", "false");
        fsOptionsProps.forEach(properties::setProperty);
        return properties;
    }

    private void initReader(Properties properties) throws Exception {
        String realtimePath = dataFileLength != -1 ? dataFilePath : deltaFilePaths[0];
        long realtimeLength = dataFileLength != -1 ? dataFileLength : 0;
        Path path = new Path(realtimePath);
        FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, (String[]) null);
        List<HoodieLogFile> logFiles = Arrays.stream(deltaFilePaths).map(HoodieLogFile::new)
                .collect(Collectors.toList());
        FileSplit hudiSplit =
                new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, instantTime, false, Option.empty());

        JobConf jobConf = new JobConf(new Configuration());
        properties.stringPropertyNames().forEach(name -> jobConf.set(name, properties.getProperty(name)));
        InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf, inputFormat);
        reader = (RecordReader<NullWritable, ArrayWritable>) inputFormatClass
                .getRecordReader(hudiSplit, jobConf, Reporter.NULL);

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

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name)
            throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(name, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer) throws Exception {
        ObjectInspector inspector = deserializer.getObjectInspector();
        Preconditions.checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT,
                "expected STRUCT: %s", inspector.getCategory());
        return (StructObjectInspector) inspector;
    }
}
