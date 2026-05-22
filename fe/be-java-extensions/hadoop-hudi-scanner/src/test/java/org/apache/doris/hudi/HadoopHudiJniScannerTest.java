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

import com.google.common.collect.Maps;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class HadoopHudiJniScannerTest {

    @Test
    public void testHadoopHudiJniScannerConstructor() {
        Map<String, String> params = Maps.newHashMap();
        params.put("base_path", "hdfs://ns0001:9000/hudi/test");
        params.put("data_file_path", "hdfs://ns0001:9000/hudi/test/file_path");
        params.put("data_file_length", "1024");
        params.put("instant_time", "1672531200");
        params.put("serde", "org.apache.hadoop.hive.serde2.OpenCSVSerde");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");
        params.put("hudi_column_names", "id,name,age");
        params.put("hudi_column_types", "int#string#int");
        params.put("hudi_primary_keys", "id");
        params.put("hadoop_conf.test", "test");
        params.put("HADOOP_USER_NAME", "test");
        params.put("HADOOP_USER_TOKEN", "xxxxxxx");
        HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(1024, params);
        Assertions.assertEquals("hdfs://ns0001:9000/hudi/test", scanner.getBasePath());
        Assertions.assertEquals("hdfs://ns0001:9000/hudi/test/file_path", scanner.getDataFilePath());
        Assertions.assertEquals(1024, scanner.getDataFileLength());
        Assertions.assertEquals(0, scanner.getDeltaFilePaths().length);
        Assertions.assertEquals("1672531200", scanner.getInstantTime());
        Assertions.assertEquals("org.apache.hadoop.hive.serde2.OpenCSVSerde", scanner.getSerde());
        Assertions.assertEquals("org.apache.hadoop.hive.ql.io.HiveInputFormat", scanner.getInputFormat());
        Assertions.assertEquals("id,name,age", scanner.getHudiColumnNames());
        Assertions.assertEquals(new String[]{"int", "string", "int"}.length, scanner.getHudiColumnTypes().length);
        Assertions.assertEquals(new String[]{"id"}[0], scanner.getRequiredFields()[0]);
        Assertions.assertEquals("test", scanner.getFsOptionsProps().get("test"));
        Assertions.assertEquals(ZoneId.systemDefault(), scanner.getColumnValue().getZoneId());
        Assertions.assertEquals(1024, scanner.getFetchSize());
        Assertions.assertEquals("test", scanner.getHadoopUserName());
        Assertions.assertEquals("xxxxxxx", scanner.getHadoopUserToken());
        params.put("delta_file_paths", "hdfs://ns0001:9000/hudi/test/delta_file_paths");
        params.put("required_fields", "id,name");
        params.put("time_zone", "Asia/Shanghai");
        HadoopHudiJniScanner scanner2 = new HadoopHudiJniScanner(1024, params);
        Assertions.assertEquals(new String[]{"hdfs://ns0001:9000/hudi/test/delta_file_paths"}[0],
                scanner2.getDeltaFilePaths()[0]);
        Assertions.assertEquals(new String[]{"id", "name"}[0], scanner2.getRequiredFields()[0]);
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"), scanner2.getColumnValue().getZoneId());
    }

    private Map<String, String> buildMinimalParams() {
        Map<String, String> params = Maps.newHashMap();
        params.put("base_path", "/tmp/test");
        params.put("data_file_path", "/tmp/test.parquet");
        params.put("data_file_length", "1024");
        params.put("delta_file_paths", "");
        params.put("instant_time", "000");
        params.put("serde", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        params.put("input_format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");
        params.put("hudi_column_names", "id,name");
        params.put("hudi_column_types", "int#string");
        params.put("hudi_primary_keys", "id");
        params.put("required_fields", "id,name");
        params.put("time_zone", "UTC");
        params.put("HADOOP_USER_NAME", "test");
        params.put("HADOOP_USER_TOKEN", "token");
        return params;
    }

    @Test
    public void testInitReaderTimeoutMs_default() throws Exception {
        Map<String, String> params = buildMinimalParams();
        HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(1024, params);

        Field timeoutField = HadoopHudiJniScanner.class.getDeclaredField("initReaderTimeoutMs");
        timeoutField.setAccessible(true);
        long timeout = (Long) timeoutField.get(scanner);

        Assertions.assertEquals(30000L, timeout);
    }

    @Test
    public void testInitReaderTimeoutMs_custom() throws Exception {
        Map<String, String> params = buildMinimalParams();
        params.put("hudi_init_reader_timeout_ms", "5000");
        HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(1024, params);

        Field timeoutField = HadoopHudiJniScanner.class.getDeclaredField("initReaderTimeoutMs");
        timeoutField.setAccessible(true);
        long timeout = (Long) timeoutField.get(scanner);

        Assertions.assertEquals(5000L, timeout);
    }

    @Test
    public void testInitReaderTimeoutMs_timeoutBehavior() throws Exception {
        Map<String, String> params = buildMinimalParams();
        params.put("hudi_init_reader_timeout_ms", "1000");
        params.put("input_format", "org.apache.doris.hudi.HadoopHudiJniScannerTest$SlowInputFormat");

        // 测试超时行为 - 由于 SlowInputFormat 需要2秒，但超时只有1秒，应该抛出超时异常
        IOException exception = Assertions.assertThrows(IOException.class, () -> {
            new HadoopHudiJniScanner(1024, params).open();
        }, "Should throw exception when operation exceeds timeout");

        Assertions.assertTrue(
                exception.getCause() instanceof TimeoutException,
                String.format("Expect cause TimeoutException, but got: %s",
                        exception.getCause() == null ? "null" : exception.getCause().getClass().getSimpleName())
        );
    }

    @Test
    public void testInitReaderTimeoutMs_noTimeout() throws Exception {
        Map<String, String> params = buildMinimalParams();
        params.put("hudi_init_reader_timeout_ms", "5000");
        params.put("input_format", "org.apache.doris.hudi.HadoopHudiJniScannerTest$SlowInputFormat");

        HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(1024, params);

        Field timeoutField = HadoopHudiJniScanner.class.getDeclaredField("initReaderTimeoutMs");
        timeoutField.setAccessible(true);
        long timeout = (Long) timeoutField.get(scanner);
        Assertions.assertEquals(5000L, timeout);

        // 测试无超时行为 - 由于 SlowInputFormat 需要2秒，而超时是5秒，应该成功
        Assertions.assertDoesNotThrow(() -> {
            scanner.open();
        }, "Should not throw any exception when timeout is sufficient");

        scanner.close();
    }

    @Test
    public void testHoodieMemorySpillableMapPath() {
        Map<String, String> params = buildMinimalParams();
        params.put("hoodie_memory_spillable_map_path", "/custom/hudi/spill/path");

        HadoopHudiJniScanner scanner = new HadoopHudiJniScanner(1024, params);
        try {
            Map<String, String> fsOptionsProps = scanner.getFsOptionsProps();
            Assertions.assertEquals("/custom/hudi/spill/path", fsOptionsProps.get("hoodie.memory.spillable.map.path"));
        } finally {
            try {
                scanner.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    public static class SlowInputFormat implements InputFormat<NullWritable, ArrayWritable> {

        private static final long DEFAULT_DELAY_MS = 2000;

        @Override
        public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
            return new InputSplit[0];
        }

        @Override
        public RecordReader<NullWritable, ArrayWritable> getRecordReader(
                InputSplit split, JobConf job, Reporter reporter) throws IOException {

            long delayMs = job.getLong("slow.inputformat.delay.ms", DEFAULT_DELAY_MS);

            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during sleep", e);
            }

            return new SlowRecordReader();
        }

        private static class SlowRecordReader implements RecordReader<NullWritable, ArrayWritable> {

            @Override
            public boolean next(NullWritable key, ArrayWritable value) throws IOException {
                return false;
            }

            @Override
            public NullWritable createKey() {
                return NullWritable.get();
            }

            @Override
            public ArrayWritable createValue() {
                return new ArrayWritable(Text.class);
            }

            @Override
            public long getPos() throws IOException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                // No-op
            }

            @Override
            public float getProgress() throws IOException {
                return 0.0f;
            }
        }
    }
}
