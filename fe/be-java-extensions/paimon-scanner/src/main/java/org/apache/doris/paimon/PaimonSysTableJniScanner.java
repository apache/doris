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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * JNI-based scanner for reading Apache Paimon system tables
 */
public class PaimonSysTableJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonSysTableJniScanner.class);

    private static final String HADOOP_OPTION_PREFIX = "hadoop.";
    private static final String PAIMON_OPTION_PREFIX = "paimon.";

    private final Map<String, String> params;
    private final Map<String, String> hadoopOptionParams;
    private final Map<String, String> paimonOptionParams;

    private final ClassLoader classLoader;
    private final Split paimonSplit;
    private Table table;
    private RecordReader<InternalRow> reader;
    private final PaimonColumnValue columnValue = new PaimonColumnValue();
    private List<DataType> paimonDataTypeList;
    private List<String> paimonAllFieldNames;
    private final PreExecutionAuthenticator preExecutionAuthenticator;
    private RecordReader.RecordIterator<InternalRow> recordIterator = null;

    /**
     * Constructs a new PaimonSysTableJniScanner for reading Paimon system tables.
     */
    public PaimonSysTableJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        if (LOG.isDebugEnabled()) {
            LOG.debug("params:{}", params);
        }
        this.params = params;
        String[] requiredFields = params.get("required_fields").split(",");
        String[] requiredTypes = params.get("required_types").split("#");
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        initTableInfo(columnTypes, requiredFields, batchSize);
        this.table = PaimonUtils.deserialize(params.get("serialized_table"));
        this.paimonAllFieldNames = PaimonUtils.getFieldNames(this.table.rowType());
        if (LOG.isDebugEnabled()) {
            LOG.debug("paimonAllFieldNames:{}", paimonAllFieldNames);
        }
        this.paimonSplit = PaimonUtils.deserialize(params.get("serialized_split"));
        this.hadoopOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(HADOOP_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(HADOOP_OPTION_PREFIX.length()),
                                Entry::getValue));
        this.paimonOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(PAIMON_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(PAIMON_OPTION_PREFIX.length()),
                                Entry::getValue));
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopOptionParams);
    }

    @Override
    public void open() {
        try {
            // When the user does not specify hive-site.xml, Paimon will look for the file from the classpath:
            //    org.apache.paimon.hive.HiveCatalog.createHiveConf:
            //        `Thread.currentThread().getContextClassLoader().getResource(HIVE_SITE_FILE)`
            // so we need to provide a classloader, otherwise it will cause NPE.
            Thread.currentThread().setContextClassLoader(classLoader);
            preExecutionAuthenticator.execute(() -> {
                initReader();
                return null;
            });
            resetDatetimeV2Precision();

        } catch (Throwable e) {
            LOG.warn("Failed to open paimon_scanner: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected int getNext() throws IOException {
        try {
            return preExecutionAuthenticator.execute(this::readAndProcessNextBatch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initReader() throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        if (this.fields.length > this.paimonAllFieldNames.size()) {
            throw new IOException(
                    String.format(
                            "The jni reader fields' size {%s} is not matched with paimon fields' size {%s}."
                                    + " Please refresh table and try again",
                            fields.length, paimonAllFieldNames.size()));
        }
        int[] projected = getProjected();
        readBuilder.withProjection(projected);
        reader = readBuilder.newRead().executeFilter().createReader(paimonSplit);
        paimonDataTypeList =
                Arrays.stream(projected).mapToObj(i -> table.rowType().getTypeAt(i)).collect(Collectors.toList());
    }

    private int[] getProjected() {
        return Arrays.stream(fields).mapToInt(paimonAllFieldNames::indexOf).toArray();
    }

    private void resetDatetimeV2Precision() {
        for (int i = 0; i < types.length; i++) {
            if (types[i].isDateTimeV2()) {
                // paimon support precision > 6, but it has been reset as 6 in FE
                // try to get the right precision for datetimev2
                int index = paimonAllFieldNames.indexOf(fields[i]);
                if (index != -1) {
                    DataType dataType = table.rowType().getTypeAt(index);
                    if (dataType instanceof TimestampType) {
                        types[i].setPrecision(((TimestampType) dataType).getPrecision());
                    }
                }
            }
        }
    }

    private int readAndProcessNextBatch() throws IOException {
        int rows = 0;
        try {
            if (recordIterator == null) {
                recordIterator = reader.readBatch();
            }

            while (recordIterator != null) {
                InternalRow record;
                while ((record = recordIterator.next()) != null) {
                    columnValue.setOffsetRow(record);
                    for (int i = 0; i < fields.length; i++) {
                        columnValue.setIdx(i, types[i], paimonDataTypeList.get(i));
                        long l = System.nanoTime();
                        appendData(i, columnValue);
                        appendDataTime += System.nanoTime() - l;
                    }
                    rows++;
                    if (rows >= batchSize) {
                        return rows;
                    }
                }
                recordIterator.releaseBatch();
                recordIterator = reader.readBatch();
            }
        } catch (Exception e) {
            close();
            LOG.warn("Failed to get the next batch of paimon. "
                            + "split: {}, requiredFieldNames: {}, paimonAllFieldNames: {}, dataType: {}",
                    paimonSplit, params.get("required_fields"), paimonAllFieldNames, paimonDataTypeList, e);
            throw new IOException(e);
        }
        return rows;
    }
}
