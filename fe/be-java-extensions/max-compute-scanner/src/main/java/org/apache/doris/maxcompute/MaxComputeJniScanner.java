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

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.base.Strings;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * MaxComputeJ JniScanner. BE will read data from the scanner object.
 */
public class MaxComputeJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(MaxComputeJniScanner.class);
    private static final String REGION = "region";
    private static final String PROJECT = "project";
    private static final String PARTITION_SPEC = "partition_spec";
    private static final String TABLE = "table";
    private static final String ACCESS_KEY = "access_key";
    private static final String SECRET_KEY = "secret_key";
    private static final String ODPS_URL = "odps_url";
    private static final String TUNNEL_URL = "tunnel_url";
    private static final String START_OFFSET = "start_offset";
    private static final String SPLIT_SIZE = "split_size";
    private static final String PUBLIC_ACCESS = "public_access";
    private final Map<String, MaxComputeTableScan> tableScans = new ConcurrentHashMap<>();
    private final String region;
    private final String project;
    private final String table;
    private RootAllocator arrowAllocator;
    private PartitionSpec partitionSpec;
    private Set<String> partitionColumns;
    private MaxComputeTableScan curTableScan;
    private MaxComputeColumnValue columnValue;
    private long remainBatchRows = 0;
    private long totalRows = 0;
    private ArrowRecordReader curReader;
    private List<Column> readColumns;
    private Map<String, Integer> readColumnsToId;
    private long startOffset = -1L;
    private int retryCount = 2;
    private long splitSize = -1L;
    private final Map<String, String> refreshParams;

    public MaxComputeJniScanner(int batchSize, Map<String, String> params) {
        region = Objects.requireNonNull(params.get(REGION), "required property '" + REGION + "'.");
        project = Objects.requireNonNull(params.get(PROJECT), "required property '" + PROJECT + "'.");
        table = Objects.requireNonNull(params.get(TABLE), "required property '" + TABLE + "'.");
        refreshParams = params;
        tableScans.putIfAbsent(tableUniqKey(), newTableScan(params));
        curTableScan = tableScans.get(tableUniqKey());
        String partitionSpec = params.get(PARTITION_SPEC);
        if (StringUtils.isNotEmpty(partitionSpec)) {
            this.partitionSpec = new PartitionSpec(partitionSpec);
        }
        String[] requiredFields = params.get("required_fields").split(",");
        String[] types = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[types.length];
        for (int i = 0; i < types.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], types[i]);
        }
        initTableInfo(columnTypes, requiredFields, batchSize);
    }

    public void refreshTableScan() {
        curTableScan = newTableScan(refreshParams);
        tableScans.put(tableUniqKey(), curTableScan);
    }

    private MaxComputeTableScan newTableScan(Map<String, String> params) {
        if (!Strings.isNullOrEmpty(params.get(START_OFFSET))
                && !Strings.isNullOrEmpty(params.get(SPLIT_SIZE))) {
            startOffset = Long.parseLong(params.get(START_OFFSET));
            splitSize = Long.parseLong(params.get(SPLIT_SIZE));
        }
        String accessKey = Objects.requireNonNull(params.get(ACCESS_KEY), "required property '" + ACCESS_KEY + "'.");
        String secretKey = Objects.requireNonNull(params.get(SECRET_KEY), "required property '" + SECRET_KEY + "'.");
        boolean enablePublicAccess = Boolean.parseBoolean(params.getOrDefault(PUBLIC_ACCESS, "false"));
        return new MaxComputeTableScan(params.get(ODPS_URL), params.get(TUNNEL_URL), region, project, table,
                accessKey, secretKey, enablePublicAccess);
    }

    public String tableUniqKey() {
        return region + "#" + project + "." + table;
    }

    @Override
    protected void initTableInfo(ColumnType[] requiredTypes, String[] requiredFields, int batchSize) {
        super.initTableInfo(requiredTypes, requiredFields, batchSize);
        readColumns = new ArrayList<>();
        readColumnsToId = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            if (!Strings.isNullOrEmpty(fields[i])) {
                readColumns.add(createOdpsColumn(i, types[i]));
                readColumnsToId.put(fields[i], i);
            }
        }
    }

    @Override
    public void open() throws IOException {
        // reorder columns
        List<Column> columnList = curTableScan.getSchema().getColumns();
        columnList.addAll(curTableScan.getSchema().getPartitionColumns());
        Map<String, Integer> columnRank = new HashMap<>();
        for (int i = 0; i < columnList.size(); i++) {
            columnRank.put(columnList.get(i).getName(), i);
        }
        // Downloading columns data from Max compute only supports the order of table metadata.
        // We might get an error message if no sort here: Column reorder is not supported in legacy arrow mode.
        readColumns.sort((Comparator.comparing(o -> columnRank.get(o.getName()))));
        if (readColumns.isEmpty()) {
            return;
        }
        try {
            TableTunnel.DownloadSession session;
            if (partitionSpec != null) {
                session = curTableScan.openDownLoadSession(partitionSpec);
            } else {
                session = curTableScan.openDownLoadSession();
            }
            long start = startOffset == -1L ? 0 : startOffset;
            long recordCount = session.getRecordCount();
            totalRows = splitSize > 0 ? Math.min(splitSize, recordCount) : recordCount;
            partitionColumns = session.getSchema().getPartitionColumns().stream()
                    .map(Column::getName)
                    .collect(Collectors.toSet());
            List<Column> pushDownColumns = new ArrayList<>(readColumns);
            pushDownColumns.removeIf(e -> partitionColumns.contains(e.getName()));
            if (pushDownColumns.isEmpty() && !partitionColumns.isEmpty()) {
                // query columns required non-null, when query partition table
                pushDownColumns.add(session.getSchema().getColumn(0));
            }
            arrowAllocator = new RootAllocator(Integer.MAX_VALUE);
            curReader = session.openArrowRecordReader(start, totalRows, pushDownColumns, arrowAllocator);
            remainBatchRows = totalRows;
        } catch (TunnelException e) {
            if (retryCount > 0 && e.getErrorMsg().contains("TableModified")) {
                retryCount--;
                // try to refresh table scan and re-open odps
                refreshTableScan();
                open();
            } else {
                retryCount = 2;
                throw new IOException(e);
            }
        } catch (Exception e) {
            close();
            throw new IOException(e);
        }
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
            case DATETIME:
            case DATETIMEV2:
                odpsType = TypeInfoFactory.DATETIME;
                break;
            case DATE:
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
        String tableName = tableUniqKey();
        MaxComputeTableScan scan = tableScans.get(tableName);
        if (scan != null && scan.endOfScan()) {
            tableScans.remove(tableName);
        }
        remainBatchRows = 0;
        totalRows = 0;
        startOffset = -1;
        splitSize = -1;
        if (curReader != null) {
            arrowAllocator.close();
            arrowAllocator = null;
            curReader.close();
            curReader = null;
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
        curTableScan.increaseReadRows(realRows);
        return realRows;
    }

    private int readVectors(int expectedRows) throws IOException {
        VectorSchemaRoot batch;
        int curReadRows = 0;
        while (curReadRows < expectedRows) {
            batch = curReader.read();
            if (batch == null) {
                break;
            }
            try {
                List<FieldVector> fieldVectors = batch.getFieldVectors();
                int batchRows = 0;
                for (FieldVector column : fieldVectors) {
                    Integer readColumnId = readColumnsToId.get(column.getName());
                    if (readColumnId == null) {
                        // use for partition if no column need to read.
                        batchRows = column.getValueCount();
                        continue;
                    }
                    columnValue.reset(column);
                    batchRows = column.getValueCount();
                    for (int j = 0; j < batchRows; j++) {
                        appendData(readColumnId, columnValue);
                    }
                }
                if (partitionSpec != null) {
                    for (String partitionColumn : partitionColumns) {
                        String partitionValue = partitionSpec.get(partitionColumn);
                        Integer readColumnId = readColumnsToId.get(partitionColumn);
                        if (readColumnId != null && partitionValue != null) {
                            MaxComputePartitionValue value = new MaxComputePartitionValue(partitionValue);
                            for (int i = 0; i < batchRows; i++) {
                                appendData(readColumnId, value);
                            }
                        }
                    }
                }
                curReadRows += batchRows;
            } catch (Exception e) {
                throw new RuntimeException("Fail to read arrow data, reason: " + e.getMessage(), e);
            } finally {
                batch.close();
            }
        }
        return curReadRows;
    }
}
