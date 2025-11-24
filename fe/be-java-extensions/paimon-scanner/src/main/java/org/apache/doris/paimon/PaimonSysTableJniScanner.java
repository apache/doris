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

import com.google.common.base.Preconditions;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * JNI-based scanner for reading Apache Paimon system tables
 * TODO: unify this with PaimonJniScanner in future
 */
public class PaimonSysTableJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonSysTableJniScanner.class);

    private static final String HADOOP_OPTION_PREFIX = "hadoop.";

    private final ClassLoader classLoader;
    private final Table table;
    private final Iterator<Split> paimonSplits;
    private final PaimonColumnValue columnValue = new PaimonColumnValue();
    private final List<String> paimonAllFieldNames;
    private final int[] projected;
    private final List<DataType> paimonDataTypeList;
    private final PreExecutionAuthenticator preExecutionAuthenticator;
    private RecordReader<InternalRow> reader;
    private RecordReader.RecordIterator<InternalRow> recordIterator;

    /**
     * Constructs a new PaimonSysTableJniScanner for reading Paimon system tables.
     */
    public PaimonSysTableJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        if (LOG.isDebugEnabled()) {
            LOG.debug("params:{}", params);
        }
        String[] requiredFields = params.get("required_fields").split(",");
        String[] requiredTypes = params.get("required_types").split("#");
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        String timeZone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        columnValue.setTimeZone(timeZone);
        initTableInfo(columnTypes, requiredFields, batchSize);
        this.table = PaimonUtils.deserialize(params.get("serialized_table"));
        this.paimonAllFieldNames = PaimonUtils.getFieldNames(this.table.rowType());
        if (LOG.isDebugEnabled()) {
            LOG.debug("paimonAllFieldNames:{}", paimonAllFieldNames);
        }
        resetDatetimeV2Precision();
        this.projected = Arrays.stream(fields).mapToInt(paimonAllFieldNames::indexOf).toArray();
        this.paimonDataTypeList = Arrays.stream(projected).mapToObj(i -> table.rowType().getTypeAt(i))
                .collect(Collectors.toList());
        this.paimonSplits = Arrays.stream(params.get("serialized_splits").split(","))
                .map(PaimonUtils::deserialize).map(obj -> (Split) obj)
                .collect(Collectors.toList()).iterator();
        Map<String, String> hadoopOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(HADOOP_OPTION_PREFIX))
                .collect(Collectors.toMap(kv1 -> kv1.getKey().substring(HADOOP_OPTION_PREFIX.length()),
                        Entry::getValue));
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopOptionParams);
    }

    @Override
    public void open() throws IOException {
        Thread.currentThread().setContextClassLoader(classLoader);
        if (!paimonSplits.hasNext()) {
            throw new IOException("Failed to open PaimonSysTableJniScanner: No valid splits found");
        }
        nextReader();
    }

    private void nextReader() throws IOException {
        Preconditions.checkArgument(paimonSplits.hasNext(), "No more splits available");
        Split paimonSplit = paimonSplits.next();
        ReadBuilder readBuilder = table.newReadBuilder();
        readBuilder.withProjection(projected);
        try {
            preExecutionAuthenticator.execute(() -> {
                reader = readBuilder.newRead().executeFilter().createReader(paimonSplit);
                Preconditions.checkArgument(recordIterator == null);
                recordIterator = reader.readBatch();
                return null;
            });
        } catch (Exception e) {
            this.close();
            String msg = String.format("Failed to open next paimonSplit: %s", paimonSplits);
            LOG.error(msg, e);
            throw new IOException(msg, e);
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
            throw new IOException("Failed to getNext in PaimonSysTableJniScanner", e);
        }
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
            if (recordIterator == null && paimonSplits.hasNext()) {
                // try to get next reader
                nextReader();
            }
        }
        return rows;
    }
}
