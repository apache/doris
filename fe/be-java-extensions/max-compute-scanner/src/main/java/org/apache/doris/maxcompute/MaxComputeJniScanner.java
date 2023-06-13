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
import org.apache.doris.common.jni.vec.ScanPredicate;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.base.Strings;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MaxComputeJ JniScanner. BE will read data from the scanner object.
 */
public class MaxComputeJniScanner extends JniScanner {
    private Odps odps;
    private TableTunnel tunnel;

    private static final Logger LOG = Logger.getLogger(MaxComputeJniScanner.class);
    private static final String odpsUrlTemplate = "http://service.{}.maxcompute.aliyun.com/api";
    private static final String tunnelUrlTemplate = "http://dt.{}.maxcompute.aliyun-inc.com";
    private static final String REGION = "region";
    private static final String PROJECT = "project";
    private static final String TABLE = "table";
    private static final String ACCESS_KEY = "access_key";
    private static final String SECRET_KEY = "secret_key";
    private static final String START_OFFSET = "start_offset";
    private static final String SPLIT_SIZE = "split_size";
    private static final String PUBLIC_ACCESS = "public_access";
    private final String project;
    private final String table;
    private MaxComputeColumnValue columnValue;
    private long remainBatchRows = 0;
    private long totalRows = 0;
    private TableTunnel.DownloadSession session;
    private ArrowRecordReader curReader;
    private List<Column> columns;
    private Map<String, Integer> readColumnsId;
    private long startOffset = -1L;
    private long splitSize = -1L;

    public MaxComputeJniScanner(int batchSize, Map<String, String> params) {
        String region = Objects.requireNonNull(params.get(REGION), "required property '" + REGION + "'.");
        project = Objects.requireNonNull(params.get(PROJECT), "required property '" + PROJECT + "'.");
        table = Objects.requireNonNull(params.get(TABLE), "required property '" + TABLE + "'.");
        if (!Strings.isNullOrEmpty(params.get(START_OFFSET))
                && !Strings.isNullOrEmpty(params.get(SPLIT_SIZE))) {
            startOffset = Long.parseLong(params.get(START_OFFSET));
            splitSize = Long.parseLong(params.get(SPLIT_SIZE));
        }
        String accessKey = Objects.requireNonNull(params.get(ACCESS_KEY), "required property '" + ACCESS_KEY + "'.");
        String secretKey = Objects.requireNonNull(params.get(SECRET_KEY), "required property '" + SECRET_KEY + "'.");
        odps = new Odps(new AliyunAccount(accessKey, secretKey));
        odps.setEndpoint(odpsUrlTemplate.replace("{}", region));
        odps.setDefaultProject(project);
        tunnel = new TableTunnel(odps);
        String tunnelUrl = tunnelUrlTemplate.replace("{}", region);
        boolean enablePublicAccess = Boolean.parseBoolean(params.getOrDefault(PUBLIC_ACCESS, "false"));
        if (enablePublicAccess) {
            tunnelUrl = tunnelUrlTemplate.replace("-inc", "");
        }
        tunnel.setEndpoint(tunnelUrl);
        String[] requiredFields = params.get("required_fields").split(",");
        String[] types = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[types.length];
        for (int i = 0; i < types.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], types[i]);
        }
        ScanPredicate[] predicates = new ScanPredicate[0];
        if (params.containsKey("push_down_predicates")) {
            long predicatesAddress = Long.parseLong(params.get("push_down_predicates"));
            if (predicatesAddress != 0) {
                predicates = ScanPredicate.parseScanPredicates(predicatesAddress, columnTypes);
                LOG.info("MaxComputeJniScanner gets pushed-down predicates:  " + ScanPredicate.dump(predicates));
            }
        }
        initTableInfo(columnTypes, requiredFields, predicates, batchSize);
    }

    @Override
    protected void initTableInfo(ColumnType[] requiredTypes, String[] requiredFields, ScanPredicate[] predicates,
                                 int batchSize) {
        super.initTableInfo(requiredTypes, requiredFields, predicates, batchSize);
        columns = new ArrayList<>();
        readColumnsId = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            if (!Strings.isNullOrEmpty(fields[i])) {
                columns.add(createOdpsColumn(i, types[i]));
                readColumnsId.put(fields[i], i);
            }
        }
        // reorder columns
        List<Column> columnList = odps.tables().get(table).getSchema().getColumns();
        Map<String, Integer> columnRank = new HashMap<>();
        for (int i = 0; i < columnList.size(); i++) {
            columnRank.put(columnList.get(i).getName(), i);
        }
        // Downloading columns data from Max compute only supports the order of table metadata.
        // We might get an error message if no sort here: Column reorder is not supported in legacy arrow mode.
        columns.sort((Comparator.comparing(o -> columnRank.get(o.getName()))));
    }

    @Override
    public void open() throws IOException {
        if (columns.isEmpty()) {
            return;
        }
        try {
            session = tunnel.createDownloadSession(project, table);
            if (splitSize > 0) {
                totalRows = Math.min(splitSize, session.getRecordCount());
            } else {
                totalRows = session.getRecordCount();
            }
            long start = startOffset == -1L ? 0 : startOffset;
            curReader = session.openArrowRecordReader(start, totalRows, columns);
        } catch (Exception e) {
            throw new IOException(e);
        }
        remainBatchRows = totalRows;
    }

    private Column createOdpsColumn(int colIdx, ColumnType dorisType) {
        TypeInfo odpsType;
        switch (dorisType.getType()) {
            case BOOLEAN:
                odpsType = TypeInfoFactory.BOOLEAN;
                break;
            case TINYINT:
                odpsType = TypeInfoFactory.TINYINT;
                break;
            case SMALLINT:
                odpsType = TypeInfoFactory.SMALLINT;
                break;
            case INT:
                odpsType = TypeInfoFactory.INT;
                break;
            case BIGINT:
                odpsType = TypeInfoFactory.BIGINT;
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMALV2:
                odpsType = TypeInfoFactory.getDecimalTypeInfo(dorisType.getPrecision(), dorisType.getScale());
                break;
            case FLOAT:
                odpsType = TypeInfoFactory.FLOAT;
                break;
            case DOUBLE:
                odpsType = TypeInfoFactory.DOUBLE;
                break;
            case DATETIMEV2:
                odpsType = TypeInfoFactory.DATETIME;
                break;
            case DATEV2:
                odpsType = TypeInfoFactory.DATE;
                break;
            case CHAR:
                odpsType = TypeInfoFactory.getCharTypeInfo(dorisType.getLength());
                break;
            case VARCHAR:
                odpsType = TypeInfoFactory.getVarcharTypeInfo(dorisType.getLength());
                break;
            case STRING:
                odpsType = TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING);
                break;
            default:
                throw new RuntimeException("Unsupported transform for column type: " + dorisType.getType());
        }
        return new Column(fields[colIdx], odpsType);
    }

    @Override
    public void close() throws IOException {
        remainBatchRows = 0;
        totalRows = 0;
        startOffset = -1;
        splitSize = -1;
        if (curReader != null) {
            curReader.close();
        }
    }

    @Override
    protected int getNext() throws IOException {
        if (curReader == null) {
            return 0;
        }
        columnValue = new MaxComputeColumnValue();
        int expectedRows = (int) Math.min(batchSize, remainBatchRows);
        int realRows = readVectors(expectedRows);
        if (remainBatchRows <= 0) {
            return 0;
        }
        remainBatchRows -= realRows;
        return realRows;
    }

    private int readVectors(int expectedRows) throws IOException {
        VectorSchemaRoot batch;
        int curReadRows = 0;
        while (curReadRows < expectedRows && (batch = curReader.read()) != null) {
            List<FieldVector> fieldVectors = batch.getFieldVectors();
            int batchRows = 0;
            for (FieldVector column : fieldVectors) {
                columnValue.reset(column);
                // LOG.warn("MCJNI read getClass: " + column.getClass());
                batchRows = column.getValueCount();
                for (int j = 0; j < batchRows; j++) {
                    appendData(readColumnsId.get(column.getName()), columnValue);
                }
            }
            curReadRows += batchRows;
        }
        return curReadRows;
    }
}
