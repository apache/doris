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
import org.apache.doris.common.security.authentication.AuthenticationConfig;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.client.HoodieHadoopTable;
import org.apache.hudi.hadoop.client.HoodieRecordReader;
import org.apache.hudi.hadoop.client.HoodieSplit.SerializableInputSplit;
import org.apache.hudi.hadoop.config.InputFormatConfig;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.PrivilegedAction;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * JDHadoopHudiJniScanner is a JniScanner implementation that reads Hudi data using hudi-hadoop-mr.
 */
public class JDHadoopHudiJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(JDHadoopHudiJniScanner.class);
    private static final AtomicBoolean LOGGED_CLASSLOADING_INFO = new AtomicBoolean(false);

    private static final String HADOOP_CONF_PREFIX = "hadoop_conf.";
    /** Doris slot types from BE, aligned with required_fields (same as Hive JNI scanner). */
    private static final String COLUMNS_TYPES = "columns_types";

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
    /** Doris query slot types from BE; used for JNI wire format (may differ from Hive hudi_column_types). */
    private final String[] dorisColumnTypes;
    private final String[] requiredFields;
    private int[] requiredColumnIds;
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

    private final String hadoopUserName;
    private final String hadoopUserToken;

    private final long initReaderTimeoutMs;

    // New reader fields
    private final boolean useNewReader;
    private final String serializedInputSplit;
    private final String databaseName;
    private final String tableName;
    private final String startInstant;
    private final String endInstant;
    private final String serializedHoodieTable;
    /** From scan params (FE sets it from @incr 'iscdc', default false); used by new reader getRecordReader options. */
    private final boolean enableCdcRead;
    private HoodieRecordReader recordReader;
    private Iterator<ArrayWritable> newReaderIterator;

    public static int HUDI_READER_INIT_CORE_THREAD_NUM = 20;
    public static int HUDI_READER_INIT_MAXIMUM_THREAD_NUM = 256;
    public static int HUDI_READER_INIT_QUEUE_CAPACITY = 102400;

    private static final ThreadPoolExecutor hudiReaderInitExecutorService = buildHudiReaderInitExecutor();

    private static ThreadPoolExecutor buildHudiReaderInitExecutor() {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(HUDI_READER_INIT_QUEUE_CAPACITY);
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("HudiReaderInit-%d")
                .build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                HUDI_READER_INIT_CORE_THREAD_NUM,
                HUDI_READER_INIT_MAXIMUM_THREAD_NUM,
                60L,
                TimeUnit.SECONDS,
                queue,
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy()
        );
        return executor;
    }

    public JDHadoopHudiJniScanner(int fetchSize, Map<String, String> params) {
        // Check if using new reader
        this.useNewReader = params.containsKey("serialized_input_split")
                && !Strings.isNullOrEmpty(params.get("serialized_input_split"));

        if (useNewReader) {
            // New reader path
            this.serializedInputSplit = params.get("serialized_input_split");
            this.databaseName = params.get("database_name");
            this.tableName = params.get("table_name");
            this.startInstant = params.get("start_instant");
            this.endInstant = params.get("end_instant");
            this.serializedHoodieTable = params.get("serialized_hoodie_table");
        } else {
            // Old reader path
            this.serializedInputSplit = null;
            this.databaseName = null;
            this.tableName = null;
            this.startInstant = null;
            this.endInstant = null;
            this.serializedHoodieTable = null;
        }
        // FE provides this option in scan properties (see HudiScanNode.getLocationProperties()).
        // Default to false when missing.
        this.enableCdcRead = Boolean.parseBoolean(params.get(InputFormatConfig.ENABLE_CDC_READ));

        this.basePath = params.get("base_path");
        this.dataFilePath = params.get("data_file_path");
        this.dataFileLength = parseLongOrDefault(params.get("data_file_length"), -1L);
        if (Strings.isNullOrEmpty(params.get("delta_file_paths"))) {
            this.deltaFilePaths = new String[0];
        } else {
            this.deltaFilePaths = params.get("delta_file_paths").split(",");
        }
        this.instantTime = params.get("instant_time");
        this.serde = params.get("serde");
        this.inputFormat = params.get("input_format");

        this.hudiColumnNames = requireNonEmptyParam(params, "hudi_column_names");
        this.hudiColumnTypes = splitRequired(params, "hudi_column_types", "#");
        this.dorisColumnTypes = splitOptional(params, COLUMNS_TYPES, "#");
        if (StringUtils.isEmpty(params.get("required_fields"))) {
            this.requiredFields = splitRequired(params, "hudi_primary_keys", ",");
        } else {
            this.requiredFields = splitRequired(params, "required_fields", ",");
        }
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.fsOptionsProps = Maps.newHashMap();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(HADOOP_CONF_PREFIX)) {
                fsOptionsProps.put(entry.getKey().substring(HADOOP_CONF_PREFIX.length()), entry.getValue());
            } else if (entry.getKey().equals("hoodie_memory_spillable_map_path")) {
                fsOptionsProps.put("hoodie.memory.spillable.map.path", entry.getValue());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get hudi params {}: {}", entry.getKey(), entry.getValue());
            }
        }

        ZoneId zoneId;
        if (Strings.isNullOrEmpty(params.get("time_zone"))) {
            zoneId = ZoneId.systemDefault();
        } else {
            String timeZone = params.get("time_zone");
            try {
                zoneId = ZoneId.of(timeZone);
            } catch (DateTimeException e) {
                LOG.warn("Invalid time_zone '{}', fallback to system default timezone", timeZone, e);
                zoneId = ZoneId.systemDefault();
            }
        }
        this.columnValue = new HadoopHudiColumnValue(zoneId);
        this.fetchSize = fetchSize;
        this.classLoader = this.getClass().getClassLoader();
        String resolvedUser = params.get("HADOOP_USER_NAME");
        if (Strings.isNullOrEmpty(resolvedUser)) {
            resolvedUser = fsOptionsProps.get(AuthenticationConfig.HADOOP_USER_NAME);
        }
        if (Strings.isNullOrEmpty(resolvedUser)) {
            String envUser = System.getenv("HADOOP_USER_NAME");
            if (!Strings.isNullOrEmpty(envUser)) {
                resolvedUser = envUser;
            }
        }
        this.hadoopUserName = resolvedUser != null ? resolvedUser : "";
        this.hadoopUserToken = params.get("HADOOP_USER_TOKEN");
        this.initReaderTimeoutMs = parseLongOrDefault(params.get("hudi_init_reader_timeout_ms"), 30000L);
    }

    public String getBasePath() {
        return basePath;
    }

    public String getDataFilePath() {
        return dataFilePath;
    }

    public long getDataFileLength() {
        return dataFileLength;
    }

    public String[] getDeltaFilePaths() {
        return deltaFilePaths;
    }

    public String getInstantTime() {
        return instantTime;
    }

    public String getSerde() {
        return serde;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getHudiColumnNames() {
        return hudiColumnNames;
    }

    public String[] getHudiColumnTypes() {
        return hudiColumnTypes;
    }

    public String[] getRequiredFields() {
        return requiredFields;
    }

    public Map<String, String> getFsOptionsProps() {
        return fsOptionsProps;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public HadoopHudiColumnValue getColumnValue() {
        return columnValue;
    }

    public String getHadoopUserName() {
        return hadoopUserName;
    }

    public String getHadoopUserToken() {
        return hadoopUserToken;
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
            if (useNewReader) {
                return getNextNewReader();
            } else {
                return getNextOldReader();
            }
        } catch (Exception e) {
            close();
            LOG.warn("failed to get next in hadoop hudi jni scanner", e);
            throw new IOException("failed to get next in hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    private int getNextNewReader() throws IOException {
        if (recordReader == null) {
            return 0;
        }

        int numRows = 0;

        while (numRows < fetchSize) {
            if (newReaderIterator == null || !newReaderIterator.hasNext()) {
                newReaderIterator = readFromNewReader();
                if (newReaderIterator == null || !newReaderIterator.hasNext()) {
                    break;
                }
            }
            ArrayWritable value = newReaderIterator.next();
            // Process ArrayWritable - iterate through required fields
            Writable[] writables = value.get();
            for (int i = 0; i < requiredFields.length && i < fields.length; i++) {
                int colIdx = requiredColumnIds[i];
                if (colIdx >= 0 && colIdx < writables.length) {
                    Writable writable = writables[colIdx];
                    columnValue.setRow(writable != null ? writable : null);
                    columnValue.setField(types[i], fieldInspectors[i]);
                    appendData(i, columnValue);
                }
            }
            numRows++;
        }

        return numRows;
    }

    protected Iterator<ArrayWritable> readFromNewReader() throws IOException {
        return recordReader.read();
    }

    private int getNextOldReader() throws IOException {
        NullWritable key = reader.createKey();
        ArrayWritable value = reader.createValue();
        int numRows = 0;
        for (; numRows < fetchSize; numRows++) {
            if (!reader.next(key, value)) {
                break;
            }
            try {
                Object rowData = deserializer.deserialize(value);
                for (int i = 0; i < fields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
                    columnValue.setRow(fieldData);
                    columnValue.setField(types[i], fieldInspectors[i]);
                    appendData(i, columnValue);
                }
            } catch (SerDeException e) {
                throw new IOException("Failed to deserialize row data: " + e.getMessage(), e);
            }
        }
        return numRows;
    }

    @Override
    public void close() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            if (useNewReader) {
                if (recordReader != null) {
                    // HoodieRecordReader may not have close method, but if it does, call it
                    // For now, just set to null
                    recordReader = null;
                }
                newReaderIterator = null;
            } else {
                if (reader != null) {
                    reader.close();
                }
            }
        } catch (IOException e) {
            LOG.warn("failed to close hadoop hudi jni scanner", e);
            throw new IOException("failed to close hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    private void initRequiredColumnsAndTypes() {
        requiredTypes = new ColumnType[requiredFields.length];
        requiredColumnIds = new int[requiredFields.length];
        parseRequiredTypes();
        if (!useNewReader) {
            String[] splitHudiColumnNames = hudiColumnNames.split(",");
            Map<String, Integer> hudiColNameToIdx =
                    IntStream.range(0, splitHudiColumnNames.length)
                            .boxed()
                            .collect(Collectors.toMap(i -> splitHudiColumnNames[i], i -> i));
            requiredColumnIds = Arrays.stream(requiredFields)
                    .mapToInt(hudiColNameToIdx::get)
                    .toArray();
        }
    }

    /**
     * JNI column types must match Doris BE slot types (columns_types from BE).
     * hudi_column_types carries Hive/HMS types and is only used for serde properties and fallback.
     */
    private void parseRequiredTypes() {
        if (dorisColumnTypes.length == requiredFields.length) {
            for (int i = 0; i < requiredFields.length; i++) {
                requiredTypes[i] = ColumnType.parseType(requiredFields[i], dorisColumnTypes[i]);
            }
            return;
        }
        Map<String, String> hudiColNameToType = buildHudiColNameToTypeMap();
        for (int i = 0; i < requiredFields.length; i++) {
            String fieldName = requiredFields[i];
            String hiveType = hudiColNameToType.get(fieldName);
            if (hiveType == null) {
                throw new IllegalArgumentException(
                        "Required field " + fieldName + " not found in Hudi column names: " + hudiColumnNames);
            }
            requiredTypes[i] = ColumnType.parseType(fieldName, hiveType);
        }
    }

    private Map<String, String> buildHudiColNameToTypeMap() {
        String[] splitHudiColumnNames = hudiColumnNames.split(",");
        return IntStream.range(0, splitHudiColumnNames.length)
                .boxed()
                .collect(Collectors.toMap(i -> splitHudiColumnNames[i], i -> hudiColumnTypes[i]));
    }

    private static String[] splitOptional(Map<String, String> params, String key, String delimiter) {
        String value = params.get(key);
        if (Strings.isNullOrEmpty(value)) {
            return new String[0];
        }
        return value.split(delimiter);
    }

    private Properties getReaderProperties() {
        Properties properties = new Properties();
        properties.setProperty("hive.io.file.readcolumn.ids",
                Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf)
                        .collect(Collectors.joining(",")));
        properties.setProperty("hive.io.file.readcolumn.names", Joiner.on(",").join(this.requiredFields));
        properties.setProperty("columns", this.hudiColumnNames);
        properties.setProperty("columns.types", Joiner.on(",").join(hudiColumnTypes));
        properties.setProperty("serialization.lib", this.serde);
        properties.setProperty("hive.io.file.read.all.columns", "false");
        fsOptionsProps.forEach(properties::setProperty);
        return properties;
    }

    private void initReader(Properties properties) throws Exception {
        if (useNewReader) {
            // New reader path: use HoodieHadoopTable.getRecordReader
            initNewReader(properties);
        } else {
            // Old reader path
            initOldReader(properties);
        }
    }

    private void initNewReader(Properties properties) throws Exception {
        // Deserialize the input split
        SerializableInputSplit serializableSplit = deserializeInputSplit(serializedInputSplit);

        // Build Hadoop configuration: apply catalog/fs keys first, then serde/hive reader props.
        // Some HDFS builds (e.g. ConfigService on close) misbehave when FileSystem is first touched from
        // pooled worker threads; new reader init therefore runs synchronously on the open() thread.
        Configuration baseConf = new Configuration();
        for (Map.Entry<String, String> e : fsOptionsProps.entrySet()) {
            if (e.getKey() != null && e.getValue() != null) {
                baseConf.set(e.getKey(), e.getValue());
            }
        }
        JobConf jobConf = new JobConf(baseConf);
        for (String name : properties.stringPropertyNames()) {
            String v = properties.getProperty(name);
            if (v != null) {
                jobConf.set(name, v);
            }
        }

        // Get HDFS user and token
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserName,
                    null, hadoopUserToken);
        if (Strings.isNullOrEmpty(serializedHoodieTable)) {
            throw new IllegalArgumentException("serialized_hoodie_table is required in new hudi reader path");
        }
        HoodieHadoopTable table = deserializeHoodieTable(serializedHoodieTable);

        // Build options (must match FE getSplits / getLocationProperties; not an OS env var).
        Map<String, String> options = new HashMap<>();
        options.put(InputFormatConfig.ENABLE_CDC_READ, String.valueOf(this.enableCdcRead));

        // Get column names from required fields
        List<String> readColumnNames = Arrays.asList(requiredFields);

        String splitDebugInfo = String.valueOf(serializableSplit.get());

        Future<HoodieRecordReader> buildReaderFuture = hudiReaderInitExecutorService.submit(() ->
                ugi.doAs((PrivilegedAction<HoodieRecordReader>) () -> {
                    try {
                        return table.getRecordReader(serializableSplit.get(), readColumnNames, options);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
        );

        try {
            recordReader = buildReaderFuture.get(initReaderTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            buildReaderFuture.cancel(true);
            LOG.warn("Timeout to init Hudi new reader, timeoutMs: {}, queue size: {}, running threads: {}, "
                    + "table: {}.{}, dataFilePath: {}, split: {}",
                    initReaderTimeoutMs,
                    hudiReaderInitExecutorService.getQueue().size(),
                    hudiReaderInitExecutorService.getActiveCount(),
                    databaseName, tableName, dataFilePath, splitDebugInfo, ex);
            throw new IOException("Timeout to init Hudi new reader, timeoutMs=" + initReaderTimeoutMs
                + ", table=" + databaseName + "." + tableName
                + ", dataFilePath=" + dataFilePath
                + ", split=" + splitDebugInfo, ex);
        } catch (ExecutionException ex) {
            buildReaderFuture.cancel(true);
            Throwable rootCause = ex.getCause() != null ? ex.getCause() : ex;
            LOG.warn("Failed to init Hudi new reader, queue size: {}, running threads: {}, table: {}.{}, "
                    + "dataFilePath: {}, split: {}, requiredFields: {}",
                    hudiReaderInitExecutorService.getQueue().size(),
                    hudiReaderInitExecutorService.getActiveCount(),
                    databaseName, tableName, dataFilePath, splitDebugInfo, Arrays.toString(requiredFields), rootCause);
            throw new IOException("Failed to init Hudi new reader: " + rootCause.getMessage()
                + ", table=" + databaseName + "." + tableName
                + ", dataFilePath=" + dataFilePath
                + ", split=" + splitDebugInfo
                + ", requiredFields=" + Arrays.toString(requiredFields), rootCause);
        } catch (InterruptedException ex) {
            buildReaderFuture.cancel(true);
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while init Hudi new reader, table=" + databaseName + "."
                + tableName + ", dataFilePath=" + dataFilePath + ", split=" + splitDebugInfo, ex);
        }

        // Get output fields and initialize inspectors
        // Hoodie bundle variants may relocate FieldSchema package differently (e.g. opg... or org...).
        // Keep this code package-agnostic to avoid compile/runtime conflicts when switching Hudi artifacts.
        List<?> outputFields = recordReader.outputFields();

        // Build a map from field name to index in Hoodie output row.
        Map<String, Integer> fieldNameToIndex = new HashMap<>();
        for (int i = 0; i < outputFields.size(); i++) {
            Object field = outputFields.get(i);
            try {
                String fieldName = (String) field.getClass().getMethod("getName").invoke(field);
                fieldNameToIndex.put(fieldName, i);
            } catch (ReflectiveOperationException e) {
                throw new IOException("Failed to read output field schema from HoodieRecordReader", e);
            }
        }

        // Initialize column indices and inspectors. JNI types come from parseRequiredTypes().
        deserializer = initDeserializer(jobConf, properties);
        rowInspector = initRowObjectInspector();
        for (int i = 0; i < requiredFields.length; i++) {
            String fieldName = requiredFields[i];
            if (fieldNameToIndex.containsKey(fieldName)) {
                requiredColumnIds[i] = fieldNameToIndex.get(fieldName);
                StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
                structFields[i] = field;
                fieldInspectors[i] = field.getFieldObjectInspector();
            }
        }
    }

    //private ObjectInspector createObjectInspectorFromType(String type) {
    //    // Create ObjectInspector from type string using ObjectInspectorFactory
    //    org.apache.hadoop.hive.serde2.typeinfo.TypeInfo typeInfo =
    //            org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString(type);
    //    return org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
    //            .getStandardObjectInspectorFromTypeInfo(typeInfo);
    //}

    private void initOldReader(Properties properties) throws Exception {
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

        InputFormat<?, ?> inputFormatClass = getInputFormat(jobConf, inputFormat);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserName,
                null, hadoopUserToken);

        // Async init: pool threads need ThreadClassLoaderContext like open()/getNext().
        Future<RecordReader<NullWritable, ArrayWritable>> buildReaderFuture = hudiReaderInitExecutorService.submit(() ->
                ugi.doAs((PrivilegedAction<RecordReader<NullWritable, ArrayWritable>>) () -> {
                    try {
                        return (RecordReader<NullWritable, ArrayWritable>) inputFormatClass.getRecordReader(
                            hudiSplit, jobConf, Reporter.NULL);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
        );
        try {
            reader = buildReaderFuture.get(initReaderTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            buildReaderFuture.cancel(true);
            LOG.warn("Failed to init Hudi RecordReader, size of query: {}, size of running threads: {}, hudiSplit: {}",
                    hudiReaderInitExecutorService.getQueue().size(),
                    hudiReaderInitExecutorService.getActiveCount(), hudiSplit, ex);
            throw ex;
        }

        deserializer = initDeserializer(jobConf, properties);
        rowInspector = initRowObjectInspector();
        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
        }
    }

    private SerializableInputSplit deserializeInputSplit(String serialized) throws Exception {
        byte[] bytes = Base64.getUrlDecoder().decode(serialized.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (SerializableInputSplit) ois.readObject();
        }
    }

    private HoodieHadoopTable deserializeHoodieTable(String serialized) throws Exception {
        byte[] bytes = Base64.getUrlDecoder().decode(serialized.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (HoodieHadoopTable) ois.readObject();
        }
    }

    private InputFormat<?, ?> getInputFormat(Configuration conf, String inputFormat) throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private Deserializer initDeserializer(Configuration configuration, Properties properties)
            throws Exception {
        // 1. 增加校验逻辑
        if (serde == null || serde.trim().isEmpty()) {
            // 方案A：抛出包含表名/分区等上下文的清晰异常
            throw new IllegalArgumentException("Failed to initialize deserializer: "
                    + "serde class name is empty for current table/partition. serde=" + serde);
            // 方案B：根据 Hudi 表类型提供默认兜底（例如默认 Parquet）
            // serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
        }

        Class<? extends Deserializer> deserializerClass = Class.forName(serde, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector initRowObjectInspector() throws Exception {
        Preconditions.checkNotNull(deserializer);
        ObjectInspector inspector = deserializer.getObjectInspector();
        return (StructObjectInspector) inspector;
    }

    private static long parseLongOrDefault(String rawValue, long defaultValue) {
        if (Strings.isNullOrEmpty(rawValue)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(rawValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value: '" + rawValue + "'", e);
        }
    }

    private static String requireNonEmptyParam(Map<String, String> params, String key) {
        String value = params.get(key);
        if (Strings.isNullOrEmpty(value)) {
            throw new IllegalArgumentException("Required param is missing or empty: " + key);
        }
        return value;
    }

    private static String[] splitRequired(Map<String, String> params, String key, String delimiterRegex) {
        return requireNonEmptyParam(params, key).split(delimiterRegex);
    }
}
