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

import org.apache.doris.jni.JniScanner;
import org.apache.doris.jni.vec.ColumnValue;

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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * The hudi JniScanner
 */
public class HudiJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(HudiJniScanner.class);
    private HudiScanParam hudiScanParam;

    private RecordReader<NullWritable, ArrayWritable> reader;
    private StructObjectInspector rowInspector;
    private Deserializer deserializer;
    private final ClassLoader classLoader;

    public HudiJniScanner(int fetchSize, Map<String, String> params) {
        LOG.debug("fetchSize:" + fetchSize + " , params: " + params);
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
                        ColumnValue fieldValue = new HudiColumnValue(hudiScanParam.getFieldInspectors()[i], fieldData);
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


    private void init(JobConf jobConf, Properties properties) throws Exception {
        String basePath = hudiScanParam.getBasePath();
        String dataFilePath = hudiScanParam.getDataFilePath();
        long dataFileLength = hudiScanParam.getDataFileLength();
        String[] deltaFilePaths = hudiScanParam.getDeltaFilePaths();
        String[] requiredFields = hudiScanParam.getRequiredFields();

        String realtimePath = dataFileLength != -1 ? dataFilePath : deltaFilePaths[0];
        long realtimeLength = dataFileLength != -1 ? dataFileLength : 0;

        Path path = new Path(realtimePath);

        FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, (String[]) null);
        List<HoodieLogFile> logFiles = Arrays.stream(deltaFilePaths).map(HoodieLogFile::new)
                .collect(Collectors.toList());

        FileSplit hudiSplit =
                new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, hudiScanParam.getInstantTime(), false,
                        Option.empty());

        InputFormat<?, ?> inputFormatClass = HudiScanUtils.createInputFormat(jobConf, hudiScanParam.getInputFormat());

        reader = (RecordReader<NullWritable, ArrayWritable>) inputFormatClass
                .getRecordReader(hudiSplit, jobConf, Reporter.NULL);

        deserializer = HudiScanUtils.getDeserializer(jobConf, properties, hudiScanParam.getSerde());

        rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            hudiScanParam.getStructFields()[i] = field;
            hudiScanParam.getFieldInspectors()[i] = field.getFieldObjectInspector();
        }
    }

}
