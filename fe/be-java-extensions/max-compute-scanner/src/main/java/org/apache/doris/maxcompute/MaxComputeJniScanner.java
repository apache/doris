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

package org.apache.doris.maxcompute;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.google.common.base.Strings;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MaxComputeJ JniScanner. BE will read data from the scanner object.
 */
public class MaxComputeJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(MaxComputeJniScanner.class);


    private static final String ACCESS_KEY = "access_key";
    private static final String SECRET_KEY = "secret_key";
    private static final String ENDPOINT = "endpoint";
    private static final String QUOTA = "quota";
    private static final String PROJECT = "project";
    private static final String TABLE = "table";

    private static final String START_OFFSET = "start_offset";
    private static final String SPLIT_SIZE = "split_size";
    private static final String SESSION_ID = "session_id";
    private static final String SCAN_SERIALIZER = "scan_serializer";


    private enum SplitType {
        BYTE_SIZE,
        ROW_OFFSET
    }

    private SplitType splitType;
    private TableBatchReadSession scan;
    public  String sessionId;

    private String project; //final ???
    private String table;

    private SplitReader<VectorSchemaRoot> currentSplitReader;
    private MaxComputeColumnValue columnValue;

    private Map<String, Integer> readColumnsToId;

    private long startOffset = -1L;
    private long splitSize = -1L;
    public EnvironmentSettings settings;


    public MaxComputeJniScanner(int batchSize, Map<String, String> params) {
        String[] requiredFields = params.get("required_fields").split(",");
        String[] types = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[types.length];
        for (int i = 0; i < types.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], types[i]);
        }
        initTableInfo(columnTypes, requiredFields, batchSize);

        if (!Strings.isNullOrEmpty(params.get(START_OFFSET))
                && !Strings.isNullOrEmpty(params.get(SPLIT_SIZE))) {
            startOffset = Long.parseLong(params.get(START_OFFSET));
            splitSize = Long.parseLong(params.get(SPLIT_SIZE));
            if (splitSize == -1) {
                splitType = SplitType.BYTE_SIZE;
            } else {
                splitType = SplitType.ROW_OFFSET;
            }
        }

        String accessKey = Objects.requireNonNull(params.get(ACCESS_KEY), "required property '" + ACCESS_KEY + "'.");
        String secretKey = Objects.requireNonNull(params.get(SECRET_KEY), "required property '" + SECRET_KEY + "'.");
        String endpoint = Objects.requireNonNull(params.get(ENDPOINT), "required property '" + ENDPOINT + "'.");
        String quota = Objects.requireNonNull(params.get(QUOTA), "required property '" + QUOTA + "'.");
        String scanSerializer = Objects.requireNonNull(params.get(SCAN_SERIALIZER),
                "required property '" + SCAN_SERIALIZER + "'.");
        project = Objects.requireNonNull(params.get(PROJECT), "required property '" + PROJECT + "'.");
        table = Objects.requireNonNull(params.get(TABLE), "required property '" + TABLE + "'.");
        sessionId = Objects.requireNonNull(params.get(SESSION_ID), "required property '" + SESSION_ID + "'.");


        Account account = new AliyunAccount(accessKey, secretKey);
        Odps odps = new Odps(account);

        odps.setDefaultProject(project);
        odps.setEndpoint(endpoint);

        Credentials credentials = Credentials.newBuilder().withAccount(odps.getAccount())
                .withAppAccount(odps.getAppAccount()).build();

        settings = EnvironmentSettings.newBuilder()
                .withCredentials(credentials)
                .withServiceEndpoint(odps.getEndpoint())
                .withQuotaName(quota)
                .build();

        try {
            scan = (TableBatchReadSession) deserialize(scanSerializer);
        } catch (Exception e) {
            LOG.info("deserialize TableBatchReadSession failed.", e);
        }
    }


    @Override
    protected void initTableInfo(ColumnType[] requiredTypes, String[] requiredFields, int batchSize) {
        super.initTableInfo(requiredTypes, requiredFields, batchSize);
        readColumnsToId = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            if (!Strings.isNullOrEmpty(fields[i])) {
                readColumnsToId.put(fields[i], i);
            }
        }
    }

    @Override
    public void open() throws IOException {
        try {
            InputSplit split;
            if (splitType == SplitType.BYTE_SIZE) {
                split = new IndexedInputSplit(sessionId, (int) startOffset);
            } else {
                split = new RowRangeInputSplit(sessionId, startOffset, splitSize);
            }

            currentSplitReader = scan.createArrowReader(split, ReaderOptions.newBuilder().withSettings(settings)
                    .withCompressionCodec(CompressionCodec.ZSTD)
                    .withReuseBatch(true)
                    .build());

        } catch (IOException e) {
            LOG.info("createArrowReader failed.", e);
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        startOffset = -1;
        splitSize = -1;
        currentSplitReader = null;
        settings = null;
        scan = null;
        readColumnsToId.clear();
    }

    @Override
    protected int getNext() throws IOException {
        if (currentSplitReader == null) {
            return 0;
        }
        columnValue = new MaxComputeColumnValue();
        int expectedRows = batchSize;
        return readVectors(expectedRows);
    }

    private int readVectors(int expectedRows) throws IOException {
        int curReadRows = 0;
        while (curReadRows < expectedRows) {
            try {
                if (!currentSplitReader.hasNext()) {
                    currentSplitReader.close();
                    currentSplitReader = null;
                    break;
                }
            } catch (Exception e) {
                LOG.info("currentSplitReader hasNext fail", e);
                break;
            }

            try {
                VectorSchemaRoot data = currentSplitReader.get();
                if (data.getRowCount() == 0) {
                    break;
                }

                List<FieldVector> fieldVectors = data.getFieldVectors();
                int batchRows = 0;
                for (FieldVector column : fieldVectors) {
                    Integer readColumnId = readColumnsToId.get(column.getName());
                    batchRows = column.getValueCount();
                    if (readColumnId == null) {
                        continue;
                    }
                    columnValue.reset(column);
                    for (int j = 0; j < batchRows; j++) {
                        appendData(readColumnId, columnValue);
                    }
                }
                curReadRows += batchRows;
            } catch (Exception e) {
                throw new RuntimeException("Fail to read arrow data, reason: " + e.getMessage(), e);
            }
        }
        return curReadRows;
    }

    private static Object deserialize(String serializedString) throws IOException, ClassNotFoundException {
        byte[] serializedBytes = Base64.getDecoder().decode(serializedString);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return objectInputStream.readObject();
    }
}
