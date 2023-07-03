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


import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.jni.vec.TableSchema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The hudi JniScanner
 */
public class HudiJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(HudiJniScanner.class);
    private final HudiScanParam hudiScanParam;

    UserGroupInformation ugi = null;
    private RecordReader<NullWritable, ArrayWritable> reader;
    private StructObjectInspector rowInspector;
    private Deserializer deserializer;
    private final ClassLoader classLoader;

    private long getRecordReaderTimeNs = 0;

    public HudiJniScanner(int fetchSize, Map<String, String> params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Hudi JNI params:\n" + params.entrySet().stream().map(kv -> kv.getKey() + "=" + kv.getValue())
                    .collect(Collectors.joining("\n")));
        }
        this.hudiScanParam = new HudiScanParam(fetchSize, params);
        this.classLoader = this.getClass().getClassLoader();
    }


    @Override
    public void open() throws IOException {
        try {
            initTableInfo(hudiScanParam.getRequiredTypes(), hudiScanParam.getRequiredFields(), null,
                    hudiScanParam.getFetchSize());
            Properties properties = hudiScanParam.createProperties();
            JobConf jobConf = HudiScanUtils.createJobConf(properties);
            ugi = Utils.getUserGroupInformation(jobConf);
            init(jobConf, properties);
        } catch (Exception e) {
            close();
            throw new IOException("Failed to open the hudi reader.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close the hudi reader.", e);
            throw e;
        }
    }

    @Override
    public int getNext() throws IOException {
        try {
            NullWritable key = reader.createKey();
            ArrayWritable value = reader.createValue();

            int readRowNumbers = 0;
            while (readRowNumbers < getBatchSize()) {
                if (!reader.next(key, value)) {
                    break;
                }
                Object rowData = deserializer.deserialize(value);

                for (int i = 0; i < hudiScanParam.getRequiredFields().length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowData, hudiScanParam.getStructFields()[i]);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new HudiColumnValue(hudiScanParam.getFieldInspectors()[i], fieldData,
                                hudiScanParam.getRequiredTypes()[i].getPrecision());
                        appendData(i, fieldValue);
                    }
                }
                readRowNumbers++;
            }
            return readRowNumbers;
        } catch (Exception e) {
            close();
            throw new IOException("Failed to get the next batch of hudi.", e);
        }
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    private void init(JobConf jobConf, Properties properties) throws Exception {
        String basePath = hudiScanParam.getBasePath();
        String dataFilePath = hudiScanParam.getDataFilePath();
        long dataFileLength = hudiScanParam.getDataFileLength();
        String[] deltaFilePaths = hudiScanParam.getDeltaFilePaths();
        String[] requiredFields = hudiScanParam.getRequiredFields();

        String realtimePath = dataFilePath.isEmpty() ? deltaFilePaths[0] : dataFilePath;
        long realtimeLength = dataFileLength > 0 ? dataFileLength : 0;

        Path path = new Path(realtimePath);

        FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, (String[]) null);
        List<HoodieLogFile> logFiles = Arrays.stream(deltaFilePaths).map(HoodieLogFile::new)
                .collect(Collectors.toList());

        FileSplit hudiSplit =
                new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, hudiScanParam.getInstantTime(), false,
                        Option.empty());

        InputFormat<?, ?> inputFormatClass = HudiScanUtils.createInputFormat(jobConf, hudiScanParam.getInputFormat());

        // org.apache.hudi.common.util.SerializationUtils$KryoInstantiator.newKryo
        // throws error like `java.lang.IllegalArgumentException: classLoader cannot be null`.
        // Set the default class loader
        Thread.currentThread().setContextClassLoader(classLoader);

        // RecordReader will use ProcessBuilder to start a hotspot process, which may be stuck,
        // so use another process to kill this stuck process.
        // TODO(gaoxin): better way to solve the stuck process?
        AtomicBoolean isKilled = new AtomicBoolean(false);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
            if (!isKilled.get()) {
                synchronized (HudiJniScanner.class) {
                    List<Long> pids = Utils.getChildProcessIds(
                            Utils.getCurrentProcId());
                    for (long pid : pids) {
                        String cmd = Utils.getCommandLine(pid);
                        if (cmd != null && cmd.contains("org.openjdk.jol.vm.sa.AttachMain")) {
                            Utils.killProcess(pid);
                            isKilled.set(true);
                            LOG.info("Kill hotspot debugger process " + pid);
                        }
                    }
                }
            }
        }, 100, 1000, TimeUnit.MILLISECONDS);

        long startTime = System.nanoTime();
        if (ugi != null) {
            reader = ugi.doAs((PrivilegedExceptionAction<RecordReader<NullWritable, ArrayWritable>>) () -> {
                RecordReader<NullWritable, ArrayWritable> ugiReader
                        = (RecordReader<NullWritable, ArrayWritable>) inputFormatClass.getRecordReader(hudiSplit,
                        jobConf, Reporter.NULL);
                return ugiReader;
            });
        } else {
            reader = (RecordReader<NullWritable, ArrayWritable>) inputFormatClass
                    .getRecordReader(hudiSplit, jobConf, Reporter.NULL);
        }
        getRecordReaderTimeNs += System.nanoTime() - startTime;
        isKilled.set(true);
        executorService.shutdownNow();

        deserializer = HudiScanUtils.getDeserializer(jobConf, properties, hudiScanParam.getSerde());

        rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            hudiScanParam.getStructFields()[i] = field;
            hudiScanParam.getFieldInspectors()[i] = field.getFieldObjectInspector();
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        return Collections.singletonMap("timer:GetRecordReaderTime", String.valueOf(getRecordReaderTimeNs));
    }
}
