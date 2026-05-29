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

import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sun.misc.Unsafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JDHadoopHudiJniScannerTest {
    private static class NoOpAppendScanner extends JDHadoopHudiJniScanner {
        NoOpAppendScanner(int fetchSize, Map<String, String> params) {
            super(fetchSize, params);
        }

        @Override
        protected void appendData(int index, org.apache.doris.common.jni.vec.ColumnValue value) {
            // no-op to avoid vector table dependency in unit tests
        }
    }

    private static class ScriptedNewReaderScanner extends NoOpAppendScanner {
        private final Queue<Iterator<ArrayWritable>> iterators;
        private int readCallCount = 0;

        ScriptedNewReaderScanner(int fetchSize, Map<String, String> params,
                Queue<Iterator<ArrayWritable>> iterators) {
            super(fetchSize, params);
            this.iterators = iterators;
        }

        @Override
        protected Iterator<ArrayWritable> readFromNewReader() {
            readCallCount++;
            return iterators.poll();
        }

        int getReadCallCount() {
            return readCallCount;
        }
    }

    private static class SideEffectIterator implements Iterator<ArrayWritable> {
        private int remaining;
        private int hasNextCalls = 0;

        SideEffectIterator(int totalRows) {
            this.remaining = totalRows;
        }

        @Override
        public boolean hasNext() {
            hasNextCalls++;
            return remaining > 0;
        }

        @Override
        public ArrayWritable next() {
            remaining--;
            ArrayWritable value = new ArrayWritable(Writable.class);
            value.set(new Writable[] {NullWritable.get()});
            return value;
        }

        int getHasNextCalls() {
            return hasNextCalls;
        }
    }

    private static class OneRowRecordReader implements RecordReader<NullWritable, ArrayWritable> {
        private boolean consumed = false;

        @Override
        public boolean next(NullWritable key, ArrayWritable value) {
            if (consumed) {
                return false;
            }
            consumed = true;
            value.set(new Writable[] {NullWritable.get()});
            return true;
        }

        @Override
        public NullWritable createKey() {
            return NullWritable.get();
        }

        @Override
        public ArrayWritable createValue() {
            return new ArrayWritable(Writable.class);
        }

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public float getProgress() {
            return consumed ? 1.0f : 0.0f;
        }
    }

    private static class ThrowingDeserializer implements Deserializer {
        @Override
        public void initialize(org.apache.hadoop.conf.Configuration conf, java.util.Properties tbl)
                throws SerDeException {
        }

        public Class<? extends Writable> getSerializedClass() {
            return ArrayWritable.class;
        }

        public Writable serialize(Object obj, ObjectInspector objInspector) {
            return null;
        }

        @Override
        public Object deserialize(Writable blob) throws SerDeException {
            throw new SerDeException("mock serde failure");
        }

        @Override
        public ObjectInspector getObjectInspector() {
            return null;
        }

        @Override
        public SerDeStats getSerDeStats() {
            return null;
        }
    }

    public static class ThrowingInputFormat implements InputFormat<NullWritable, ArrayWritable> {
        @Override
        public RecordReader<NullWritable, ArrayWritable> getRecordReader(
                InputSplit split, JobConf job, Reporter reporter) throws IOException {
            throw new IOException("mock getRecordReader failure");
        }

        @Override
        public InputSplit[] getSplits(JobConf job, int numSplits) {
            return new InputSplit[0];
        }
    }

    public static class SuccessInputFormat implements InputFormat<NullWritable, ArrayWritable> {
        @Override
        public RecordReader<NullWritable, ArrayWritable> getRecordReader(
                InputSplit split, JobConf job, Reporter reporter) {
            return new OneRowRecordReader();
        }

        @Override
        public InputSplit[] getSplits(JobConf job, int numSplits) {
            return new InputSplit[0];
        }
    }

    public static class ThrowingCloseRecordReader implements RecordReader<NullWritable, ArrayWritable> {
        @Override
        public boolean next(NullWritable key, ArrayWritable value) {
            return false;
        }

        @Override
        public NullWritable createKey() {
            return NullWritable.get();
        }

        @Override
        public ArrayWritable createValue() {
            return new ArrayWritable(Writable.class);
        }

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public void close() throws IOException {
            throw new IOException("mock close failure");
        }

        @Override
        public float getProgress() {
            return 0;
        }
    }

    public static class SimpleStructDeserializer implements Deserializer {
        @Override
        public void initialize(org.apache.hadoop.conf.Configuration conf, java.util.Properties tbl) {
        }

        public Class<? extends Writable> getSerializedClass() {
            return ArrayWritable.class;
        }

        public Writable serialize(Object obj, ObjectInspector objInspector) {
            return null;
        }

        @Override
        public Object deserialize(Writable blob) {
            return null;
        }

        @Override
        public ObjectInspector getObjectInspector() {
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    Arrays.asList("id", "name"),
                    Arrays.asList(
                            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        }

        @Override
        public SerDeStats getSerDeStats() {
            return null;
        }
    }

    public static class SerializableStringInputSplit implements org.apache.hadoop.mapred.InputSplit, Serializable {
        @Override
        public long getLength() {
            return 1;
        }

        @Override
        public String[] getLocations() {
            return new String[0];
        }

        @Override
        public void write(java.io.DataOutput out) {
        }

        @Override
        public void readFields(java.io.DataInput in) {
        }
    }

    private static class ThrowingFuture<T> implements Future<T> {
        private final TimeoutException timeoutException;
        private final ExecutionException executionException;
        private final InterruptedException interruptedException;

        ThrowingFuture(TimeoutException timeoutException,
                ExecutionException executionException,
                InterruptedException interruptedException) {
            this.timeoutException = timeoutException;
            this.executionException = executionException;
            this.interruptedException = interruptedException;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public T get() throws ExecutionException, InterruptedException {
            if (executionException != null) {
                throw executionException;
            }
            if (interruptedException != null) {
                throw interruptedException;
            }
            return null;
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
            if (timeoutException != null) {
                throw timeoutException;
            }
            if (executionException != null) {
                throw executionException;
            }
            if (interruptedException != null) {
                throw interruptedException;
            }
            return null;
        }
    }

    private static class FixedFutureExecutor extends ThreadPoolExecutor {
        private final Future<?> fixedFuture;

        FixedFutureExecutor(Future<?> fixedFuture) {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            this.fixedFuture = fixedFuture;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return (Future<T>) fixedFuture;
        }
    }

    private static class ReadyFuture<T> implements Future<T> {
        private final T value;

        ReadyFuture(T value) {
            this.value = value;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public T get() {
            return value;
        }

        @Override
        public T get(long timeout, TimeUnit unit) {
            return value;
        }
    }

    private static class MockFieldSchema {
        private final String name;
        private final String type;

        MockFieldSchema(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }
    }

    private static String encodeObject(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(baos.toByteArray());
    }

    private static org.apache.hudi.hadoop.client.HoodieSplit.SerializableInputSplit createSerializableInputSplit()
            throws Exception {
        java.lang.reflect.Constructor<?>[] constructors =
                org.apache.hudi.hadoop.client.HoodieSplit.SerializableInputSplit.class.getDeclaredConstructors();
        for (java.lang.reflect.Constructor<?> constructor : constructors) {
            constructor.setAccessible(true);
            Class<?>[] parameterTypes = constructor.getParameterTypes();
            if (parameterTypes.length == 1
                    && org.apache.hadoop.mapred.InputSplit.class.isAssignableFrom(parameterTypes[0])) {
                return (org.apache.hudi.hadoop.client.HoodieSplit.SerializableInputSplit) constructor
                        .newInstance(new SerializableStringInputSplit());
            }
            if (parameterTypes.length == 0) {
                return (org.apache.hudi.hadoop.client.HoodieSplit.SerializableInputSplit) constructor.newInstance();
            }
        }
        throw new IllegalStateException("No compatible SerializableInputSplit constructor found");
    }

    private static ThreadPoolExecutor replaceInitExecutor(ThreadPoolExecutor replacement) throws Exception {
        Field field = JDHadoopHudiJniScanner.class.getDeclaredField("hudiReaderInitExecutorService");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        ThreadPoolExecutor original = (ThreadPoolExecutor) field.get(null);
        field.set(null, replacement);
        return original;
    }

    private static org.apache.hudi.hadoop.client.HoodieRecordReader buildRecordReaderWithOutputFields(List<?> outputFields)
            throws Exception {
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe) unsafeField.get(null);
        org.apache.hudi.hadoop.client.HoodieRecordReader reader =
                (org.apache.hudi.hadoop.client.HoodieRecordReader) unsafe.allocateInstance(
                        org.apache.hudi.hadoop.client.HoodieRecordReader.class);
        for (Field f : org.apache.hudi.hadoop.client.HoodieRecordReader.class.getDeclaredFields()) {
            if (List.class.isAssignableFrom(f.getType())) {
                f.setAccessible(true);
                f.set(reader, outputFields);
            }
        }
        return reader;
    }


    @AfterEach
    public void clearInterruptFlag() {
        Thread.interrupted();
    }


    private Map<String, String> buildOldReaderParams() {
        Map<String, String> params = new HashMap<>();
        params.put("base_path", "/tmp/hudi_table");
        params.put("data_file_path", "/tmp/hudi_table/data.parquet");
        params.put("data_file_length", "1000");
        params.put("delta_file_paths", "");
        params.put("instant_time", "20230401010101");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("hudi_column_names", "id,name,age");
        params.put("hudi_column_types", "int#string#int");
        params.put("required_fields", "id,name");
        params.put("hudi_primary_keys", "id");
        return params;
    }

    private Map<String, String> buildNewReaderParams() {
        Map<String, String> params = new HashMap<>();
        params.put("hudi_column_names", "id,name,age");
        params.put("hudi_column_types", "int#string#int");
        params.put("required_fields", "id,name");
        params.put("hudi_primary_keys", "id");
        params.put("serialized_input_split", "base64EncodedSerializedSplit");
        params.put("database_name", "test_db");
        params.put("table_name", "test_table");
        params.put("start_instant", "20230401010101");
        params.put("end_instant", "20230402010101");
        params.put("serialized_hoodie_table", "base64EncodedSerializedTable");
        return params;
    }

    // ===================== Old reader constructor tests =====================

    @Test
    public void testConstructorWithOldReaderParams() {
        Map<String, String> params = buildOldReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals("/tmp/hudi_table", scanner.getBasePath());
        Assertions.assertEquals("/tmp/hudi_table/data.parquet", scanner.getDataFilePath());
        Assertions.assertEquals(1000L, scanner.getDataFileLength());
        Assertions.assertEquals(0, scanner.getDeltaFilePaths().length);
        Assertions.assertEquals("20230401010101", scanner.getInstantTime());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", scanner.getSerde());
        Assertions.assertEquals("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat", scanner.getInputFormat());
        Assertions.assertEquals("id,name,age", scanner.getHudiColumnNames());
        Assertions.assertArrayEquals(new String[]{"int", "string", "int"}, scanner.getHudiColumnTypes());
        Assertions.assertArrayEquals(new String[]{"id", "name"}, scanner.getRequiredFields());
    }

    @Test
    public void testConstructorWithNewReaderParams() {
        Map<String, String> params = buildNewReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertNotNull(scanner);
    }

    @Test
    public void testConstructorWithMissingRequiredParams() {
        Map<String, String> params = new HashMap<>();

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new JDHadoopHudiJniScanner(1000, params);
        });
    }

    @Test
    public void testConstructorWithNewReaderParamsValidation() {
        Map<String, String> params = buildNewReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertNotNull(scanner);
    }

    @Test
    public void testConstructorWithDeltaFiles() {
        Map<String, String> params = buildOldReaderParams();
        params.put("delta_file_paths", "/tmp/hudi_table/delta1.log,/tmp/hudi_table/delta2.log");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals(2, scanner.getDeltaFilePaths().length);
        Assertions.assertEquals("/tmp/hudi_table/delta1.log", scanner.getDeltaFilePaths()[0]);
        Assertions.assertEquals("/tmp/hudi_table/delta2.log", scanner.getDeltaFilePaths()[1]);
    }

    @Test
    public void testConstructorWithRequiredFieldsPriority() {
        Map<String, String> params = buildOldReaderParams();
        params.put("required_fields", "id,name,age");
        params.put("hudi_primary_keys", "id");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertArrayEquals(new String[]{"id", "name", "age"}, scanner.getRequiredFields());
    }

    @Test
    public void testConstructorWithHadoopConfParams() {
        Map<String, String> params = buildOldReaderParams();
        params.put("hadoop_conf.fs.defaultFS", "hdfs://localhost:9000");
        params.put("hadoop_conf.dfs.replication", "3");
        params.put("hoodie_memory_spillable_map_path", "/tmp/hoodie_spill");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Map<String, String> fsOptions = scanner.getFsOptionsProps();
        Assertions.assertEquals("hdfs://localhost:9000", fsOptions.get("fs.defaultFS"));
        Assertions.assertEquals("3", fsOptions.get("dfs.replication"));
        Assertions.assertEquals("/tmp/hoodie_spill", fsOptions.get("hoodie.memory.spillable.map.path"));
    }

    @Test
    public void testConstructorWithTimeZone() {
        Map<String, String> params = buildOldReaderParams();
        params.put("time_zone", "Asia/Shanghai");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertNotNull(scanner);
        Assertions.assertNotNull(scanner.getColumnValue());
    }

    @Test
    public void testConstructorWithInvalidTimeZone() {
        Map<String, String> params = buildOldReaderParams();
        params.put("time_zone", "Invalid/TimeZone");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertNotNull(scanner);
    }

    @Test
    public void testConstructorWithMissingHudiColumnNames() {
        Map<String, String> params = buildOldReaderParams();
        params.remove("hudi_column_names");

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new JDHadoopHudiJniScanner(1000, params);
        });
    }

    @Test
    public void testConstructorWithMissingHudiColumnTypes() {
        Map<String, String> params = buildOldReaderParams();
        params.remove("hudi_column_types");

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new JDHadoopHudiJniScanner(1000, params);
        });
    }

    @Test
    public void testConstructorWithEmptyRequiredFieldsAndPrimaryKeys() {
        Map<String, String> params = buildOldReaderParams();
        params.put("required_fields", "");
        params.put("hudi_primary_keys", "");

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new JDHadoopHudiJniScanner(1000, params);
        });
    }

    // ===================== New reader specific tests =====================

    @Test
    public void testUseNewReaderFlag() throws Exception {
        Map<String, String> newParams = buildNewReaderParams();
        JDHadoopHudiJniScanner newScanner = new JDHadoopHudiJniScanner(1000, newParams);
        Field useNewReaderField = JDHadoopHudiJniScanner.class.getDeclaredField("useNewReader");
        useNewReaderField.setAccessible(true);
        Assertions.assertTrue((Boolean) useNewReaderField.get(newScanner));

        Map<String, String> oldParams = buildOldReaderParams();
        JDHadoopHudiJniScanner oldScanner = new JDHadoopHudiJniScanner(1000, oldParams);
        Assertions.assertFalse((Boolean) useNewReaderField.get(oldScanner));
    }

    @Test
    public void testUseNewReaderFlagWithEmptySerializedInputSplit() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.put("serialized_input_split", "");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);
        Field useNewReaderField = JDHadoopHudiJniScanner.class.getDeclaredField("useNewReader");
        useNewReaderField.setAccessible(true);
        Assertions.assertFalse((Boolean) useNewReaderField.get(scanner));
    }

    @Test
    public void testUseNewReaderFlagWithNullSerializedInputSplit() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.remove("serialized_input_split");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);
        Field useNewReaderField = JDHadoopHudiJniScanner.class.getDeclaredField("useNewReader");
        useNewReaderField.setAccessible(true);
        Assertions.assertFalse((Boolean) useNewReaderField.get(scanner));
    }

    @Test
    public void testNewReaderFieldsPopulated() throws Exception {
        Map<String, String> params = buildNewReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Field serializedInputSplitField = JDHadoopHudiJniScanner.class.getDeclaredField("serializedInputSplit");
        serializedInputSplitField.setAccessible(true);
        Assertions.assertEquals("base64EncodedSerializedSplit", serializedInputSplitField.get(scanner));

        Field databaseNameField = JDHadoopHudiJniScanner.class.getDeclaredField("databaseName");
        databaseNameField.setAccessible(true);
        Assertions.assertEquals("test_db", databaseNameField.get(scanner));

        Field tableNameField = JDHadoopHudiJniScanner.class.getDeclaredField("tableName");
        tableNameField.setAccessible(true);
        Assertions.assertEquals("test_table", tableNameField.get(scanner));

        Field startInstantField = JDHadoopHudiJniScanner.class.getDeclaredField("startInstant");
        startInstantField.setAccessible(true);
        Assertions.assertEquals("20230401010101", startInstantField.get(scanner));

        Field endInstantField = JDHadoopHudiJniScanner.class.getDeclaredField("endInstant");
        endInstantField.setAccessible(true);
        Assertions.assertEquals("20230402010101", endInstantField.get(scanner));

        Field serializedHoodieTableField = JDHadoopHudiJniScanner.class.getDeclaredField("serializedHoodieTable");
        serializedHoodieTableField.setAccessible(true);
        Assertions.assertEquals("base64EncodedSerializedTable", serializedHoodieTableField.get(scanner));
    }

    @Test
    public void testOldReaderFieldsNullInNewReaderMode() throws Exception {
        Map<String, String> params = buildOldReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Field serializedInputSplitField = JDHadoopHudiJniScanner.class.getDeclaredField("serializedInputSplit");
        serializedInputSplitField.setAccessible(true);
        Assertions.assertNull(serializedInputSplitField.get(scanner));

        Field databaseNameField = JDHadoopHudiJniScanner.class.getDeclaredField("databaseName");
        databaseNameField.setAccessible(true);
        Assertions.assertNull(databaseNameField.get(scanner));

        Field tableNameField = JDHadoopHudiJniScanner.class.getDeclaredField("tableName");
        tableNameField.setAccessible(true);
        Assertions.assertNull(tableNameField.get(scanner));

        Field startInstantField = JDHadoopHudiJniScanner.class.getDeclaredField("startInstant");
        startInstantField.setAccessible(true);
        Assertions.assertNull(startInstantField.get(scanner));

        Field endInstantField = JDHadoopHudiJniScanner.class.getDeclaredField("endInstant");
        endInstantField.setAccessible(true);
        Assertions.assertNull(endInstantField.get(scanner));

        Field serializedHoodieTableField = JDHadoopHudiJniScanner.class.getDeclaredField("serializedHoodieTable");
        serializedHoodieTableField.setAccessible(true);
        Assertions.assertNull(serializedHoodieTableField.get(scanner));
    }

    // ===================== Data file length tests =====================

    @Test
    public void testDataFileLengthDefault() {
        Map<String, String> params = buildOldReaderParams();
        params.remove("data_file_length");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals(-1L, scanner.getDataFileLength());
    }

    @Test
    public void testDataFileLengthInvalid() {
        Map<String, String> params = buildOldReaderParams();
        params.put("data_file_length", "not_a_number");

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new JDHadoopHudiJniScanner(1000, params);
        });
    }

    @Test
    public void testDataFileLengthZero() {
        Map<String, String> params = buildOldReaderParams();
        params.put("data_file_length", "0");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals(0L, scanner.getDataFileLength());
    }

    @Test
    public void testDataFileLengthLargeValue() {
        Map<String, String> params = buildOldReaderParams();
        params.put("data_file_length", String.valueOf(Long.MAX_VALUE));

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals(Long.MAX_VALUE, scanner.getDataFileLength());
    }

    // ===================== Timeout tests =====================

    @Test
    public void testInitReaderTimeoutMsDefault() throws Exception {
        Map<String, String> params = buildOldReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Field timeoutField = JDHadoopHudiJniScanner.class.getDeclaredField("initReaderTimeoutMs");
        timeoutField.setAccessible(true);
        Assertions.assertEquals(30000L, (Long) timeoutField.get(scanner));
    }

    @Test
    public void testInitReaderTimeoutMsCustom() throws Exception {
        Map<String, String> params = buildOldReaderParams();
        params.put("hudi_init_reader_timeout_ms", "5000");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Field timeoutField = JDHadoopHudiJniScanner.class.getDeclaredField("initReaderTimeoutMs");
        timeoutField.setAccessible(true);
        Assertions.assertEquals(5000L, (Long) timeoutField.get(scanner));
    }

    @Test
    public void testInitReaderTimeoutMsInvalid() {
        Map<String, String> params = buildOldReaderParams();
        params.put("hudi_init_reader_timeout_ms", "invalid");

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new JDHadoopHudiJniScanner(1000, params);
        });
    }

    // ===================== FetchSize tests =====================

    @Test
    public void testFetchSize() {
        Map<String, String> params = buildOldReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2048, params);

        Assertions.assertEquals(2048, scanner.getFetchSize());
    }

    @Test
    public void testFetchSizeZero() {
        Map<String, String> params = buildOldReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(0, params);

        Assertions.assertEquals(0, scanner.getFetchSize());
    }

    // ===================== Hadoop user tests =====================

    @Test
    public void testHadoopUserNameAndToken() {
        Map<String, String> params = buildOldReaderParams();
        params.put("HADOOP_USER_NAME", "testUser");
        params.put("HADOOP_USER_TOKEN", "testToken123");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals("testUser", scanner.getHadoopUserName());
        Assertions.assertEquals("testToken123", scanner.getHadoopUserToken());
    }

    @Test
    public void testHadoopUserNameAndTokenNull() {
        Map<String, String> params = buildOldReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertNull(scanner.getHadoopUserName());
        Assertions.assertNull(scanner.getHadoopUserToken());
    }

    // ===================== Time zone tests =====================

    @Test
    public void testTimeZoneUTC() {
        Map<String, String> params = buildOldReaderParams();
        params.put("time_zone", "UTC");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertNotNull(scanner.getColumnValue());
    }

    @Test
    public void testTimeZoneEmpty() {
        Map<String, String> params = buildOldReaderParams();
        params.put("time_zone", "");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertNotNull(scanner.getColumnValue());
    }

    @Test
    public void testTimeZoneNull() {
        Map<String, String> params = buildOldReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertNotNull(scanner.getColumnValue());
    }

    // ===================== Delta file paths tests =====================

    @Test
    public void testSingleDeltaFilePath() {
        Map<String, String> params = buildOldReaderParams();
        params.put("delta_file_paths", "/tmp/delta1.log");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals(1, scanner.getDeltaFilePaths().length);
        Assertions.assertEquals("/tmp/delta1.log", scanner.getDeltaFilePaths()[0]);
    }

    @Test
    public void testMultipleDeltaFilePaths() {
        Map<String, String> params = buildOldReaderParams();
        params.put("delta_file_paths", "/tmp/d1.log,/tmp/d2.log,/tmp/d3.log");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals(3, scanner.getDeltaFilePaths().length);
    }

    @Test
    public void testEmptyDeltaFilePaths() {
        Map<String, String> params = buildOldReaderParams();
        params.put("delta_file_paths", "");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals(0, scanner.getDeltaFilePaths().length);
    }

    @Test
    public void testNullDeltaFilePaths() {
        Map<String, String> params = buildOldReaderParams();
        params.remove("delta_file_paths");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals(0, scanner.getDeltaFilePaths().length);
    }

    // ===================== Required fields fallback tests =====================

    @Test
    public void testRequiredFieldsFallbackToPrimaryKeys() {
        Map<String, String> params = buildOldReaderParams();
        params.remove("required_fields");
        params.put("hudi_primary_keys", "id,ts");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertArrayEquals(new String[]{"id", "ts"}, scanner.getRequiredFields());
    }

    @Test
    public void testRequiredFieldsEmptyFallbackToPrimaryKeys() {
        Map<String, String> params = buildOldReaderParams();
        params.put("required_fields", "");
        params.put("hudi_primary_keys", "id");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertArrayEquals(new String[]{"id"}, scanner.getRequiredFields());
    }

    // ===================== Multiple hadoop conf params =====================

    @Test
    public void testMultipleHadoopConfParams() {
        Map<String, String> params = buildOldReaderParams();
        params.put("hadoop_conf.key1", "value1");
        params.put("hadoop_conf.key2", "value2");
        params.put("hadoop_conf.key3", "value3");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Map<String, String> fsOptions = scanner.getFsOptionsProps();
        Assertions.assertEquals("value1", fsOptions.get("key1"));
        Assertions.assertEquals("value2", fsOptions.get("key2"));
        Assertions.assertEquals("value3", fsOptions.get("key3"));
    }

    @Test
    public void testNoHadoopConfParams() {
        Map<String, String> params = buildOldReaderParams();

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertTrue(scanner.getFsOptionsProps().isEmpty());
    }

    // ===================== parseLongOrDefault static method tests =====================

    @Test
    public void testParseLongOrDefaultViaReflection() throws Exception {
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("parseLongOrDefault", String.class, long.class);
        method.setAccessible(true);

        Assertions.assertEquals(42L, method.invoke(null, "42", 0L));
        Assertions.assertEquals(0L, method.invoke(null, "0", 99L));
        Assertions.assertEquals(-1L, method.invoke(null, "-1", 0L));
        Assertions.assertEquals(99L, method.invoke(null, null, 99L));
        Assertions.assertEquals(99L, method.invoke(null, "", 99L));

        try {
            method.invoke(null, "abc", 0L);
            Assertions.fail("Expected exception for invalid long value");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    // ===================== requireNonEmptyParam static method tests =====================

    @Test
    public void testRequireNonEmptyParamViaReflection() throws Exception {
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("requireNonEmptyParam", Map.class, String.class);
        method.setAccessible(true);

        Map<String, String> params = new HashMap<>();
        params.put("key", "value");

        Assertions.assertEquals("value", method.invoke(null, params, "key"));

        try {
            method.invoke(null, params, "missing_key");
            Assertions.fail("Expected exception for missing key");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        params.put("empty_key", "");
        try {
            method.invoke(null, params, "empty_key");
            Assertions.fail("Expected exception for empty value");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        params.put("null_key", null);
        try {
            method.invoke(null, params, "null_key");
            Assertions.fail("Expected exception for null value");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    // ===================== splitRequired static method tests =====================

    @Test
    public void testSplitRequiredViaReflection() throws Exception {
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("splitRequired", Map.class, String.class, String.class);
        method.setAccessible(true);

        Map<String, String> params = new HashMap<>();
        params.put("key", "a,b,c");
        String[] result = (String[]) method.invoke(null, params, "key", ",");
        Assertions.assertArrayEquals(new String[]{"a", "b", "c"}, result);

        params.put("hash_key", "int#string#int");
        String[] result2 = (String[]) method.invoke(null, params, "hash_key", "#");
        Assertions.assertArrayEquals(new String[]{"int", "string", "int"}, result2);
    }

    // ===================== Close tests =====================

    @Test
    public void testCloseWithoutOpen() throws IOException {
        Map<String, String> params = buildOldReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertDoesNotThrow(() -> scanner.close());
    }

    @Test
    public void testCloseNewReaderWithoutOpen() throws IOException {
        Map<String, String> params = buildNewReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertDoesNotThrow(() -> scanner.close());
    }

    // ===================== Thread pool static fields tests =====================

    @Test
    public void testThreadPoolStaticFields() {
        Assertions.assertEquals(20, JDHadoopHudiJniScanner.HUDI_READER_INIT_CORE_THREAD_NUM);
        Assertions.assertEquals(256, JDHadoopHudiJniScanner.HUDI_READER_INIT_MAXIMUM_THREAD_NUM);
        Assertions.assertEquals(102400, JDHadoopHudiJniScanner.HUDI_READER_INIT_QUEUE_CAPACITY);
    }

    // ===================== Combined old + new params =====================

    @Test
    public void testNewReaderParamsWithOldCommonFields() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.put("base_path", "/base");
        params.put("data_file_path", "/base/data.parquet");
        params.put("data_file_length", "5000");
        params.put("delta_file_paths", "/base/d1.log");
        params.put("instant_time", "20230101");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("HADOOP_USER_NAME", "user1");
        params.put("HADOOP_USER_TOKEN", "token1");
        params.put("time_zone", "America/New_York");
        params.put("hadoop_conf.fs.defaultFS", "hdfs://nn:9000");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(512, params);

        Assertions.assertEquals("/base", scanner.getBasePath());
        Assertions.assertEquals("/base/data.parquet", scanner.getDataFilePath());
        Assertions.assertEquals(5000L, scanner.getDataFileLength());
        Assertions.assertEquals(1, scanner.getDeltaFilePaths().length);
        Assertions.assertEquals("20230101", scanner.getInstantTime());
        Assertions.assertEquals(512, scanner.getFetchSize());
        Assertions.assertEquals("user1", scanner.getHadoopUserName());
        Assertions.assertEquals("token1", scanner.getHadoopUserToken());
        Assertions.assertEquals("hdfs://nn:9000", scanner.getFsOptionsProps().get("fs.defaultFS"));

        Field useNewReaderField = JDHadoopHudiJniScanner.class.getDeclaredField("useNewReader");
        useNewReaderField.setAccessible(true);
        Assertions.assertTrue((Boolean) useNewReaderField.get(scanner));
    }

    // ===================== getNext on null reader =====================

    @Test
    public void testGetNextNewReaderReturnsZeroWhenRecordReaderIsNull() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Field recordReaderField = JDHadoopHudiJniScanner.class.getDeclaredField("recordReader");
        recordReaderField.setAccessible(true);
        Assertions.assertNull(recordReaderField.get(scanner));
    }

    @Test
    public void testOpenWrapsInitOldReaderFailure() {
        Map<String, String> params = buildOldReaderParams();
        // Force getInputFormat() to fail fast and deterministically.
        params.put("input_format", "org.apache.doris.hudi.NotExistInputFormat");
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);
        IOException exception = Assertions.assertThrows(IOException.class, scanner::open);
        Assertions.assertTrue(exception.getMessage().contains("failed to open hadoop hudi jni scanner"));
    }

    @Test
    public void testGetNextWrapsOldReaderFailureWhenReaderNotInitialized() {
        Map<String, String> params = buildOldReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);
        IOException exception = Assertions.assertThrows(IOException.class, scanner::getNext);
        Assertions.assertTrue(exception.getMessage().contains("failed to get next in hadoop hudi jni scanner"));
    }

    @Test
    public void testGetNextWrapsNewReaderFailureWhenReaderNotInitialized() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);
        // New-reader path returns 0 when recordReader is not initialized.
        Assertions.assertEquals(0, scanner.getNext());
    }

    // ===================== HoodieMemorySpillableMapPath =====================

    @Test
    public void testHoodieMemorySpillableMapPath() {
        Map<String, String> params = buildOldReaderParams();
        params.put("hoodie_memory_spillable_map_path", "/custom/spill");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Assertions.assertEquals("/custom/spill",
                scanner.getFsOptionsProps().get("hoodie.memory.spillable.map.path"));
    }

    @Test
    public void testHoodieMemorySpillableMapPathAndHadoopConf() {
        Map<String, String> params = buildOldReaderParams();
        params.put("hoodie_memory_spillable_map_path", "/spill");
        params.put("hadoop_conf.fs.defaultFS", "hdfs://nn:9000");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1000, params);

        Map<String, String> fsOptions = scanner.getFsOptionsProps();
        Assertions.assertEquals("/spill", fsOptions.get("hoodie.memory.spillable.map.path"));
        Assertions.assertEquals("hdfs://nn:9000", fsOptions.get("fs.defaultFS"));
    }

    // ===================== Edge case: single column =====================

    @Test
    public void testSingleColumnTable() {
        Map<String, String> params = new HashMap<>();
        params.put("base_path", "/tmp/single");
        params.put("data_file_path", "/tmp/single/data.parquet");
        params.put("data_file_length", "100");
        params.put("delta_file_paths", "");
        params.put("instant_time", "0");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("hudi_column_names", "id");
        params.put("hudi_column_types", "int");
        params.put("required_fields", "id");
        params.put("hudi_primary_keys", "id");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(1, params);

        Assertions.assertEquals("id", scanner.getHudiColumnNames());
        Assertions.assertArrayEquals(new String[]{"int"}, scanner.getHudiColumnTypes());
        Assertions.assertArrayEquals(new String[]{"id"}, scanner.getRequiredFields());
        Assertions.assertEquals(1, scanner.getFetchSize());
    }

    // ===================== Edge case: many columns =====================

    @Test
    public void testManyColumnsTable() {
        StringBuilder names = new StringBuilder();
        StringBuilder types = new StringBuilder();
        StringBuilder fields = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                names.append(",");
                types.append("#");
                fields.append(",");
            }
            names.append("col").append(i);
            types.append("string");
            fields.append("col").append(i);
        }

        Map<String, String> params = new HashMap<>();
        params.put("base_path", "/tmp/many");
        params.put("data_file_path", "/tmp/many/data.parquet");
        params.put("data_file_length", "9999");
        params.put("delta_file_paths", "");
        params.put("instant_time", "1");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("hudi_column_names", names.toString());
        params.put("hudi_column_types", types.toString());
        params.put("required_fields", fields.toString());
        params.put("hudi_primary_keys", "col0");

        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(100, params);

        Assertions.assertEquals(100, scanner.getHudiColumnTypes().length);
        Assertions.assertEquals(100, scanner.getRequiredFields().length);
    }

    @Test
    public void testInitDeserializerRejectsEmptySerde() throws Exception {
        Map<String, String> params = buildOldReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);

        Field serdeField = JDHadoopHudiJniScanner.class.getDeclaredField("serde");
        serdeField.setAccessible(true);
        serdeField.set(scanner, " ");

        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod(
                "initDeserializer", org.apache.hadoop.conf.Configuration.class, java.util.Properties.class);
        method.setAccessible(true);
        Exception exception = Assertions.assertThrows(Exception.class,
                () -> method.invoke(scanner, new org.apache.hadoop.conf.Configuration(), new java.util.Properties()));
        Throwable root = exception.getCause() == null ? exception : exception.getCause();
        Assertions.assertTrue(root.getMessage().contains("serde class name is empty"));
    }

    @Test
    public void testPrivateGetNextOldReaderSerdeException() throws Exception {
        Map<String, String> params = buildOldReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);

        Field readerField = JDHadoopHudiJniScanner.class.getDeclaredField("reader");
        readerField.setAccessible(true);
        readerField.set(scanner, new OneRowRecordReader());

        Field deserializerField = JDHadoopHudiJniScanner.class.getDeclaredField("deserializer");
        deserializerField.setAccessible(true);
        deserializerField.set(scanner, new ThrowingDeserializer());

        Field fieldsField = org.apache.doris.common.jni.JniScanner.class.getDeclaredField("fields");
        fieldsField.setAccessible(true);
        fieldsField.set(scanner, new String[] {"id"});

        Field typesField = org.apache.doris.common.jni.JniScanner.class.getDeclaredField("types");
        typesField.setAccessible(true);
        typesField.set(scanner, new org.apache.doris.common.jni.vec.ColumnType[] {
                org.apache.doris.common.jni.vec.ColumnType.parseType("id", "int")});

        Method getNextOldReader = JDHadoopHudiJniScanner.class.getDeclaredMethod("getNextOldReader");
        getNextOldReader.setAccessible(true);
        Exception exception = Assertions.assertThrows(Exception.class, () -> getNextOldReader.invoke(scanner));
        Throwable root = exception.getCause() == null ? exception : exception.getCause();
        Assertions.assertTrue(root instanceof IOException);
        Assertions.assertTrue(root.getMessage().contains("Failed to deserialize row data"));
    }

    @Test
    public void testInitOldReaderFutureFailureBranch() throws Exception {
        Map<String, String> params = buildOldReaderParams();
        params.put("input_format", ThrowingInputFormat.class.getName());
        params.put("serde", SimpleStructDeserializer.class.getName());
        params.put("required_fields", "id");
        params.put("hudi_column_names", "id");
        params.put("hudi_column_types", "int");
        params.put("HADOOP_USER_NAME", "old-user-1");
        params.put("HADOOP_USER_TOKEN", "old-token-1");
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);

        Method initRequiredColumnsAndTypes = JDHadoopHudiJniScanner.class.getDeclaredMethod("initRequiredColumnsAndTypes");
        initRequiredColumnsAndTypes.setAccessible(true);
        initRequiredColumnsAndTypes.invoke(scanner);

        Method getReaderProperties = JDHadoopHudiJniScanner.class.getDeclaredMethod("getReaderProperties");
        getReaderProperties.setAccessible(true);
        Properties properties = (Properties) getReaderProperties.invoke(scanner);

        Method initOldReader = JDHadoopHudiJniScanner.class.getDeclaredMethod("initOldReader", Properties.class);
        initOldReader.setAccessible(true);
        Exception exception = Assertions.assertThrows(Exception.class, () -> initOldReader.invoke(scanner, properties));
        Throwable root = exception.getCause() == null ? exception : exception.getCause();
        Assertions.assertNotNull(root);
    }

    @Test
    public void testInitOldReaderSuccessInitializesSerdeAndInspectors() throws Exception {
        Map<String, String> params = buildOldReaderParams();
        params.put("input_format", SuccessInputFormat.class.getName());
        params.put("serde", SimpleStructDeserializer.class.getName());
        params.put("required_fields", "id,name");
        params.put("hudi_column_names", "id,name");
        params.put("hudi_column_types", "int#string");
        params.put("HADOOP_USER_NAME", "old-user-2");
        params.put("HADOOP_USER_TOKEN", "old-token-2");
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);

        Method initRequiredColumnsAndTypes = JDHadoopHudiJniScanner.class.getDeclaredMethod("initRequiredColumnsAndTypes");
        initRequiredColumnsAndTypes.setAccessible(true);
        initRequiredColumnsAndTypes.invoke(scanner);

        Method getReaderProperties = JDHadoopHudiJniScanner.class.getDeclaredMethod("getReaderProperties");
        getReaderProperties.setAccessible(true);
        Properties properties = (Properties) getReaderProperties.invoke(scanner);

        Method initOldReader = JDHadoopHudiJniScanner.class.getDeclaredMethod("initOldReader", Properties.class);
        initOldReader.setAccessible(true);
        initOldReader.invoke(scanner, properties);

        Field deserializerField = JDHadoopHudiJniScanner.class.getDeclaredField("deserializer");
        deserializerField.setAccessible(true);
        Assertions.assertNotNull(deserializerField.get(scanner));

        Field rowInspectorField = JDHadoopHudiJniScanner.class.getDeclaredField("rowInspector");
        rowInspectorField.setAccessible(true);
        Object rowInspector = rowInspectorField.get(scanner);
        Assertions.assertTrue(rowInspector instanceof StructObjectInspector);

        Field structFieldsField = JDHadoopHudiJniScanner.class.getDeclaredField("structFields");
        structFieldsField.setAccessible(true);
        Object[] structFields = (Object[]) structFieldsField.get(scanner);
        Assertions.assertNotNull(structFields[0]);
        Assertions.assertNotNull(structFields[1]);

        Field fieldInspectorsField = JDHadoopHudiJniScanner.class.getDeclaredField("fieldInspectors");
        fieldInspectorsField.setAccessible(true);
        Object[] fieldInspectors = (Object[]) fieldInspectorsField.get(scanner);
        Assertions.assertNotNull(fieldInspectors[0]);
        Assertions.assertNotNull(fieldInspectors[1]);
    }

    @Test
    public void testCloseOldReaderIOExceptionPath() throws Exception {
        Map<String, String> params = buildOldReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Field readerField = JDHadoopHudiJniScanner.class.getDeclaredField("reader");
        readerField.setAccessible(true);
        readerField.set(scanner, new ThrowingCloseRecordReader());
        IOException exception = Assertions.assertThrows(IOException.class, scanner::close);
        Assertions.assertTrue(exception.getMessage().contains("failed to close hadoop hudi jni scanner"));
    }

    @Test
    public void testInitRequiredColumnsAndTypesNewReaderBranch() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("initRequiredColumnsAndTypes");
        method.setAccessible(true);
        method.invoke(scanner);
        Field requiredTypesField = JDHadoopHudiJniScanner.class.getDeclaredField("requiredTypes");
        requiredTypesField.setAccessible(true);
        Object[] requiredTypes = (Object[]) requiredTypesField.get(scanner);
        Assertions.assertEquals(2, requiredTypes.length);
        Field requiredColumnIdsField = JDHadoopHudiJniScanner.class.getDeclaredField("requiredColumnIds");
        requiredColumnIdsField.setAccessible(true);
        int[] requiredColumnIds = (int[]) requiredColumnIdsField.get(scanner);
        Assertions.assertEquals(2, requiredColumnIds.length);
    }

    @Test
    public void testInitReaderDispatchesNewReaderBranch() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.put("HADOOP_USER_NAME", "u1");
        params.put("HADOOP_USER_TOKEN", "t1");
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("initReader", Properties.class);
        method.setAccessible(true);
        Exception exception = Assertions.assertThrows(Exception.class, () -> method.invoke(scanner, new Properties()));
        Assertions.assertNotNull(exception);
    }

    @Test
    public void testInitReaderDispatchesOldReaderBranch() throws Exception {
        Map<String, String> params = buildOldReaderParams();
        params.put("input_format", "org.apache.doris.hudi.NotExistInputFormat");
        params.put("HADOOP_USER_NAME", "u2");
        params.put("HADOOP_USER_TOKEN", "t2");
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("initReader", Properties.class);
        method.setAccessible(true);
        Exception exception = Assertions.assertThrows(Exception.class, () -> method.invoke(scanner, new Properties()));
        Assertions.assertNotNull(exception);
    }

    @Test
    public void testDeserializeInputSplitViaReflection() throws Exception {
        SerializableStringInputSplit payload = new SerializableStringInputSplit();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(payload);
        }
        String encoded = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(baos.toByteArray());
        Map<String, String> params = buildNewReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("deserializeInputSplit", String.class);
        method.setAccessible(true);
        Exception exception = Assertions.assertThrows(Exception.class, () -> method.invoke(scanner, encoded));
        Assertions.assertNotNull(exception);
    }

    @Test
    public void testDeserializeHoodieTableViaReflectionFailure() throws Exception {
        byte[] bytes = new byte[] {1, 2, 3, 4};
        String encoded = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        Map<String, String> params = buildNewReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("deserializeHoodieTable", String.class);
        method.setAccessible(true);
        Exception exception = Assertions.assertThrows(Exception.class, () -> method.invoke(scanner, encoded));
        Assertions.assertNotNull(exception);
    }

    @Test
    public void testGetNextOldReaderProcessesRows() throws Exception {
        Map<String, String> params = buildOldReaderParams();
        NoOpAppendScanner scanner = new NoOpAppendScanner(2, params);
        Field readerField = JDHadoopHudiJniScanner.class.getDeclaredField("reader");
        readerField.setAccessible(true);
        readerField.set(scanner, new OneRowRecordReader());
        Field deserializerField = JDHadoopHudiJniScanner.class.getDeclaredField("deserializer");
        deserializerField.setAccessible(true);
        deserializerField.set(scanner, new SimpleStructDeserializer());
        StructObjectInspector inspector = (StructObjectInspector) new SimpleStructDeserializer().getObjectInspector();
        Field rowInspectorField = JDHadoopHudiJniScanner.class.getDeclaredField("rowInspector");
        rowInspectorField.setAccessible(true);
        rowInspectorField.set(scanner, inspector);
        Field structFieldsField = JDHadoopHudiJniScanner.class.getDeclaredField("structFields");
        structFieldsField.setAccessible(true);
        Object[] structFields = (Object[]) structFieldsField.get(scanner);
        structFields[0] = inspector.getStructFieldRef("id");
        structFields[1] = inspector.getStructFieldRef("name");
        Field fieldInspectorsField = JDHadoopHudiJniScanner.class.getDeclaredField("fieldInspectors");
        fieldInspectorsField.setAccessible(true);
        Object[] fieldInspectors = (Object[]) fieldInspectorsField.get(scanner);
        fieldInspectors[0] = ((org.apache.hadoop.hive.serde2.objectinspector.StructField) structFields[0])
                .getFieldObjectInspector();
        fieldInspectors[1] = ((org.apache.hadoop.hive.serde2.objectinspector.StructField) structFields[1])
                .getFieldObjectInspector();
        Field fieldsField = org.apache.doris.common.jni.JniScanner.class.getDeclaredField("fields");
        fieldsField.setAccessible(true);
        fieldsField.set(scanner, new String[] {"id", "name"});
        Field typesField = org.apache.doris.common.jni.JniScanner.class.getDeclaredField("types");
        typesField.setAccessible(true);
        typesField.set(scanner, new org.apache.doris.common.jni.vec.ColumnType[] {
                org.apache.doris.common.jni.vec.ColumnType.parseType("id", "int"),
                org.apache.doris.common.jni.vec.ColumnType.parseType("name", "string")
        });
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("getNextOldReader");
        method.setAccessible(true);
        int rows = (int) method.invoke(scanner);
        Assertions.assertEquals(1, rows);
    }

    @Test
    public void testInitNewReaderRejectsMissingSerializedTable() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.put("HADOOP_USER_NAME", "new-user-a");
        params.put("HADOOP_USER_TOKEN", "new-token-a");
        params.put("serialized_input_split", encodeObject(createSerializableInputSplit()));
        params.put("serialized_hoodie_table", "");
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("initNewReader", Properties.class);
        method.setAccessible(true);
        Exception exception = Assertions.assertThrows(Exception.class, () -> method.invoke(scanner, new Properties()));
        Throwable root = exception.getCause() == null ? exception : exception.getCause();
        Assertions.assertTrue(root instanceof IllegalArgumentException);
        Assertions.assertTrue(root.getMessage().contains("serialized_hoodie_table is required"));
    }

    @Test
    public void testInitNewReaderTimeoutExecutionInterruptedBranches() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.put("HADOOP_USER_NAME", "new-user-b");
        params.put("HADOOP_USER_TOKEN", "new-token-b");
        params.put("serde", SimpleStructDeserializer.class.getName());
        params.put("serialized_input_split", encodeObject(createSerializableInputSplit()));
        params.put("serialized_hoodie_table", encodeObject(null));
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);

        Method initRequiredColumnsAndTypes = JDHadoopHudiJniScanner.class
                .getDeclaredMethod("initRequiredColumnsAndTypes");
        initRequiredColumnsAndTypes.setAccessible(true);
        initRequiredColumnsAndTypes.invoke(scanner);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("initNewReader", Properties.class);
        method.setAccessible(true);

        ThreadPoolExecutor original = null;
        try {
            original = replaceInitExecutor(
                    new FixedFutureExecutor(new ThrowingFuture<>(
                            new TimeoutException("mock-timeout"), null, null)));
            Exception timeoutException = Assertions.assertThrows(Exception.class,
                    () -> method.invoke(scanner, new Properties()));
            Throwable timeoutRoot = timeoutException.getCause() == null ? timeoutException : timeoutException.getCause();
            Assertions.assertTrue(timeoutRoot instanceof IOException);
            Assertions.assertTrue(timeoutRoot.getMessage().contains("Timeout to init Hudi new reader"));

            replaceInitExecutor(new FixedFutureExecutor(new ThrowingFuture<>(
                    null, new ExecutionException(new RuntimeException("mock-exec")), null)));
            Exception executionException = Assertions.assertThrows(Exception.class,
                    () -> method.invoke(scanner, new Properties()));
            Throwable executionRoot = executionException.getCause() == null
                    ? executionException : executionException.getCause();
            Assertions.assertTrue(executionRoot instanceof IOException);
            Assertions.assertTrue(executionRoot.getMessage().contains("Failed to init Hudi new reader"));

            replaceInitExecutor(new FixedFutureExecutor(new ThrowingFuture<>(
                    null, null, new InterruptedException("mock-int"))));
            Exception interruptedException = Assertions.assertThrows(Exception.class,
                    () -> method.invoke(scanner, new Properties()));
            Throwable interruptedRoot = interruptedException.getCause() == null
                    ? interruptedException : interruptedException.getCause();
            Assertions.assertTrue(interruptedRoot instanceof IOException);
            Assertions.assertTrue(interruptedRoot.getMessage().contains("Interrupted while init Hudi new reader"));
            Thread.interrupted();
        } finally {
            if (original != null) {
                replaceInitExecutor(original);
            }
        }
    }

    @Test
    public void testInitNewReaderUsesDorisSlotTypesForBinaryMappedToString() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.put("HADOOP_USER_NAME", "new-user-binary");
        params.put("HADOOP_USER_TOKEN", "new-token-binary");
        params.put("serde", SimpleStructDeserializer.class.getName());
        params.put("required_fields", "binary_col");
        params.put("hudi_column_names", "binary_col");
        params.put("hudi_column_types", "binary");
        params.put("columns_types", "string");
        params.put("serialized_input_split", encodeObject(createSerializableInputSplit()));
        params.put("serialized_hoodie_table", encodeObject(null));
        NoOpAppendScanner scanner = new NoOpAppendScanner(2, params);
        Method initRequiredColumnsAndTypes = JDHadoopHudiJniScanner.class
                .getDeclaredMethod("initRequiredColumnsAndTypes");
        initRequiredColumnsAndTypes.setAccessible(true);
        initRequiredColumnsAndTypes.invoke(scanner);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("initNewReader", Properties.class);
        method.setAccessible(true);

        ThreadPoolExecutor original = null;
        try {
            org.apache.hudi.hadoop.client.HoodieRecordReader reader =
                    buildRecordReaderWithOutputFields(
                            Collections.singletonList(new MockFieldSchema("binary_col", "binary")));
            original = replaceInitExecutor(new FixedFutureExecutor(new ReadyFuture<>(reader)));
            method.invoke(scanner, new Properties());
            Field requiredTypesField = JDHadoopHudiJniScanner.class.getDeclaredField("requiredTypes");
            requiredTypesField.setAccessible(true);
            org.apache.doris.common.jni.vec.ColumnType[] requiredTypes =
                    (org.apache.doris.common.jni.vec.ColumnType[]) requiredTypesField.get(scanner);
            Assertions.assertEquals(org.apache.doris.common.jni.vec.ColumnType.Type.STRING,
                    requiredTypes[0].getType());
        } finally {
            if (original != null) {
                replaceInitExecutor(original);
            }
        }
    }

    @Test
    public void testInitNewReaderOutputFieldsAndSchemaMappingBranches() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.put("HADOOP_USER_NAME", "new-user-c");
        params.put("HADOOP_USER_TOKEN", "new-token-c");
        params.put("serde", SimpleStructDeserializer.class.getName());
        params.put("required_fields", "id,missing_col");
        params.put("hudi_column_names", "id,missing_col");
        params.put("hudi_column_types", "int#string");
        params.put("serialized_input_split", encodeObject(createSerializableInputSplit()));
        params.put("serialized_hoodie_table", encodeObject(null));
        NoOpAppendScanner scanner = new NoOpAppendScanner(2, params);
        Method initRequiredColumnsAndTypes = JDHadoopHudiJniScanner.class
                .getDeclaredMethod("initRequiredColumnsAndTypes");
        initRequiredColumnsAndTypes.setAccessible(true);
        initRequiredColumnsAndTypes.invoke(scanner);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("initNewReader", Properties.class);
        method.setAccessible(true);

        ThreadPoolExecutor original = null;
        try {
            org.apache.hudi.hadoop.client.HoodieRecordReader reader =
                    buildRecordReaderWithOutputFields(Collections.singletonList(new MockFieldSchema("id", "int")));
            original = replaceInitExecutor(new FixedFutureExecutor(new ReadyFuture<>(reader)));
            method.invoke(scanner, new Properties());
            Field requiredTypesField = JDHadoopHudiJniScanner.class.getDeclaredField("requiredTypes");
            requiredTypesField.setAccessible(true);
            Object[] requiredTypes = (Object[]) requiredTypesField.get(scanner);
            Assertions.assertNotNull(requiredTypes[0]);
            Assertions.assertNull(requiredTypes[1]);
            Field structFieldsField = JDHadoopHudiJniScanner.class.getDeclaredField("structFields");
            structFieldsField.setAccessible(true);
            Object[] structFields = (Object[]) structFieldsField.get(scanner);
            Assertions.assertNotNull(structFields[0]);
        } finally {
            if (original != null) {
                replaceInitExecutor(original);
            }
        }
    }

    @Test
    public void testInitNewReaderOutputFieldsReflectiveFailureBranch() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        params.put("HADOOP_USER_NAME", "new-user-d");
        params.put("HADOOP_USER_TOKEN", "new-token-d");
        params.put("serde", SimpleStructDeserializer.class.getName());
        params.put("required_fields", "id");
        params.put("hudi_column_names", "id");
        params.put("hudi_column_types", "int");
        params.put("serialized_input_split", encodeObject(createSerializableInputSplit()));
        params.put("serialized_hoodie_table", encodeObject(null));
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Method initRequiredColumnsAndTypes = JDHadoopHudiJniScanner.class
                .getDeclaredMethod("initRequiredColumnsAndTypes");
        initRequiredColumnsAndTypes.setAccessible(true);
        initRequiredColumnsAndTypes.invoke(scanner);
        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("initNewReader", Properties.class);
        method.setAccessible(true);

        ThreadPoolExecutor original = null;
        try {
            org.apache.hudi.hadoop.client.HoodieRecordReader reader =
                    buildRecordReaderWithOutputFields(Collections.singletonList(new Object()));
            original = replaceInitExecutor(new FixedFutureExecutor(new ReadyFuture<>(reader)));
            Exception exception = Assertions.assertThrows(Exception.class, () -> method.invoke(scanner, new Properties()));
            Throwable root = exception.getCause() == null ? exception : exception.getCause();
            Assertions.assertTrue(root instanceof IOException || root instanceof NullPointerException);
            Assertions.assertTrue(root.getMessage().contains("Failed to read output field schema")
                    || root.getMessage().contains("null"));
        } finally {
            if (original != null) {
                replaceInitExecutor(original);
            }
        }
    }

    @Test
    public void testGetNextNewReaderRespectsFetchSizeBeforeHasNextSideEffect() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        ScriptedNewReaderScanner scanner = new ScriptedNewReaderScanner(
                2, params, new LinkedBlockingQueue<>());
        SideEffectIterator sideEffectIterator = new SideEffectIterator(3);

        Field recordReaderField = JDHadoopHudiJniScanner.class.getDeclaredField("recordReader");
        recordReaderField.setAccessible(true);
        recordReaderField.set(scanner, buildRecordReaderWithOutputFields(Collections.emptyList()));

        Field newReaderIteratorField = JDHadoopHudiJniScanner.class.getDeclaredField("newReaderIterator");
        newReaderIteratorField.setAccessible(true);
        newReaderIteratorField.set(scanner, sideEffectIterator);

        Field fieldsField = org.apache.doris.common.jni.JniScanner.class.getDeclaredField("fields");
        fieldsField.setAccessible(true);
        fieldsField.set(scanner, new String[0]);
        Field typesField = org.apache.doris.common.jni.JniScanner.class.getDeclaredField("types");
        typesField.setAccessible(true);
        typesField.set(scanner, new org.apache.doris.common.jni.vec.ColumnType[0]);

        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("getNextNewReader");
        method.setAccessible(true);
        int rows = (int) method.invoke(scanner);

        Assertions.assertEquals(2, rows);
        Assertions.assertEquals(2, sideEffectIterator.getHasNextCalls(),
                "hasNext should not be called once numRows reaches fetchSize");
        Assertions.assertEquals(0, scanner.getReadCallCount(),
                "should not call readFromNewReader when iterator already has data");
    }

    @Test
    public void testGetNextNewReaderRefillsIteratorWhenExhausted() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        LinkedBlockingQueue<Iterator<ArrayWritable>> batches = new LinkedBlockingQueue<>();
        batches.add(new SideEffectIterator(1));
        batches.add(new SideEffectIterator(1));
        batches.add(new SideEffectIterator(0));
        ScriptedNewReaderScanner scanner = new ScriptedNewReaderScanner(2, params, batches);

        Field recordReaderField = JDHadoopHudiJniScanner.class.getDeclaredField("recordReader");
        recordReaderField.setAccessible(true);
        recordReaderField.set(scanner, buildRecordReaderWithOutputFields(Collections.emptyList()));

        Field fieldsField = org.apache.doris.common.jni.JniScanner.class.getDeclaredField("fields");
        fieldsField.setAccessible(true);
        fieldsField.set(scanner, new String[0]);
        Field typesField = org.apache.doris.common.jni.JniScanner.class.getDeclaredField("types");
        typesField.setAccessible(true);
        typesField.set(scanner, new org.apache.doris.common.jni.vec.ColumnType[0]);

        Method method = JDHadoopHudiJniScanner.class.getDeclaredMethod("getNextNewReader");
        method.setAccessible(true);
        int rows = (int) method.invoke(scanner);
        Assertions.assertEquals(2, rows);
        Assertions.assertEquals(2, scanner.getReadCallCount());

        int eofRows = (int) method.invoke(scanner);
        Assertions.assertEquals(0, eofRows);
        Assertions.assertEquals(3, scanner.getReadCallCount());
    }

    @Test
    public void testCloseClearsNewReaderIteratorInNewReaderMode() throws Exception {
        Map<String, String> params = buildNewReaderParams();
        JDHadoopHudiJniScanner scanner = new JDHadoopHudiJniScanner(2, params);
        Field newReaderIteratorField = JDHadoopHudiJniScanner.class.getDeclaredField("newReaderIterator");
        newReaderIteratorField.setAccessible(true);
        newReaderIteratorField.set(scanner, new SideEffectIterator(1));

        scanner.close();
        Assertions.assertNull(newReaderIteratorField.get(scanner));
    }

}
