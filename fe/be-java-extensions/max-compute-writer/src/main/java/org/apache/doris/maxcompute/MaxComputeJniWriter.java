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

import org.apache.doris.common.jni.JniWriter;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorTable;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TableTunnel;
import com.google.common.base.Strings;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MaxComputeJniWriter writes C++ Block data to MaxCompute tables via Tunnel SDK.
 * Loaded by C++ as: org/apache/doris/maxcompute/MaxComputeJniWriter
 */
public class MaxComputeJniWriter extends JniWriter {
    private static final Logger LOG = Logger.getLogger(MaxComputeJniWriter.class);

    private static final String ACCESS_KEY = "access_key";
    private static final String SECRET_KEY = "secret_key";
    private static final String ENDPOINT = "endpoint";
    private static final String QUOTA = "quota";
    private static final String PROJECT = "project";
    private static final String TABLE = "table";
    private static final String SESSION_ID = "session_id";
    private static final String BLOCK_ID_START = "block_id_start";
    private static final String BLOCK_ID_COUNT = "block_id_count";
    private static final String PARTITION_SPEC = "partition_spec";
    private static final String CONNECT_TIMEOUT = "connect_timeout";
    private static final String READ_TIMEOUT = "read_timeout";
    private static final String RETRY_COUNT = "retry_count";

    private final String accessKey;
    private final String secretKey;
    private final String endpoint;
    private final String project;
    private final String tableName;
    private final String quota;
    private String sessionId;
    private long blockIdStart;
    private long blockIdCount;
    private String partitionSpec;
    private int connectTimeout;
    private int readTimeout;
    private int retryCount;

    // SDK objects
    private Odps odps;
    private TableTunnel tunnel;
    private TableTunnel.UploadSession uploadSession;
    private RecordWriter recordWriter;
    private TableSchema tableSchema;
    private long currentBlockId;
    private List<Long> committedBlockIds = new ArrayList<>();

    // Statistics
    private long writtenRows = 0;
    private long writtenBytes = 0;

    public MaxComputeJniWriter(int batchSize, Map<String, String> params) {
        super(batchSize, params);
        this.accessKey = Objects.requireNonNull(params.get(ACCESS_KEY), "required property '" + ACCESS_KEY + "'.");
        this.secretKey = Objects.requireNonNull(params.get(SECRET_KEY), "required property '" + SECRET_KEY + "'.");
        this.endpoint = Objects.requireNonNull(params.get(ENDPOINT), "required property '" + ENDPOINT + "'.");
        this.project = Objects.requireNonNull(params.get(PROJECT), "required property '" + PROJECT + "'.");
        this.tableName = Objects.requireNonNull(params.get(TABLE), "required property '" + TABLE + "'.");
        this.quota = params.getOrDefault(QUOTA, "");
        this.sessionId = params.getOrDefault(SESSION_ID, "");
        this.blockIdStart = Long.parseLong(params.getOrDefault(BLOCK_ID_START, "0"));
        this.blockIdCount = Long.parseLong(params.getOrDefault(BLOCK_ID_COUNT, "20000"));
        this.partitionSpec = params.getOrDefault(PARTITION_SPEC, "");
        this.connectTimeout = Integer.parseInt(params.getOrDefault(CONNECT_TIMEOUT, "10"));
        this.readTimeout = Integer.parseInt(params.getOrDefault(READ_TIMEOUT, "120"));
        this.retryCount = Integer.parseInt(params.getOrDefault(RETRY_COUNT, "4"));
    }

    @Override
    public void open() throws IOException {
        try {
            Account account = new AliyunAccount(accessKey, secretKey);
            odps = new Odps(account);
            odps.setDefaultProject(project);
            odps.setEndpoint(endpoint);

            tunnel = new TableTunnel(odps);
            if (!Strings.isNullOrEmpty(quota)) {
                tunnel.getConfig().setQuotaName(quota);
            }

            if (!Strings.isNullOrEmpty(sessionId)) {
                // Restore existing session created by FE (non-partitioned / static partition)
                if (!Strings.isNullOrEmpty(partitionSpec)) {
                    uploadSession = tunnel.getUploadSession(project, tableName,
                            new PartitionSpec(partitionSpec), sessionId);
                } else {
                    uploadSession = tunnel.getUploadSession(project, tableName, sessionId);
                }
            } else {
                // Create new session (dynamic partition scenario)
                if (!Strings.isNullOrEmpty(partitionSpec)) {
                    uploadSession = tunnel.createUploadSession(project, tableName,
                            new PartitionSpec(partitionSpec));
                } else {
                    uploadSession = tunnel.createUploadSession(project, tableName);
                }
                sessionId = uploadSession.getId();
            }

            tableSchema = uploadSession.getSchema();
            currentBlockId = blockIdStart;
            recordWriter = uploadSession.openRecordWriter(currentBlockId);
            committedBlockIds.add(currentBlockId);

            LOG.info("MaxComputeJniWriter opened: project=" + project + ", table=" + tableName
                    + ", sessionId=" + sessionId + ", partitionSpec=" + partitionSpec
                    + ", blockIdStart=" + blockIdStart + ", blockIdCount=" + blockIdCount);
        } catch (Exception e) {
            String errorMsg = "Failed to open MaxCompute upload session for table " + project + "." + tableName;
            LOG.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        }
    }

    @Override
    protected void writeInternal(VectorTable inputTable) throws IOException {
        int numRows = inputTable.getNumRows();
        int numCols = inputTable.getNumColumns();
        if (numRows == 0) {
            return;
        }

        try {
            Object[][] data = inputTable.getMaterializedData();
            List<Column> columns = tableSchema.getColumns();

            for (int row = 0; row < numRows; row++) {
                Record record = uploadSession.newRecord();
                for (int col = 0; col < numCols && col < columns.size(); col++) {
                    Object val = data[col][row];
                    if (val == null) {
                        record.set(col, null);
                        continue;
                    }
                    setRecordValue(record, col, columns.get(col), val);
                }
                recordWriter.write(record);
            }

            writtenRows += numRows;
        } catch (Exception e) {
            String errorMsg = "Failed to write data to MaxCompute table " + project + "." + tableName;
            LOG.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        }
    }

    private void setRecordValue(Record record, int idx, Column column, Object val) {
        OdpsType type = column.getTypeInfo().getOdpsType();
        switch (type) {
            case BOOLEAN:
                record.setBoolean(idx, (Boolean) val);
                break;
            case TINYINT:
                record.set(idx, ((Number) val).byteValue());
                break;
            case SMALLINT:
                record.set(idx, ((Number) val).shortValue());
                break;
            case INT:
                record.set(idx, ((Number) val).intValue());
                break;
            case BIGINT:
                record.setBigint(idx, ((Number) val).longValue());
                break;
            case FLOAT:
                record.set(idx, ((Number) val).floatValue());
                break;
            case DOUBLE:
                record.setDouble(idx, ((Number) val).doubleValue());
                break;
            case DECIMAL:
                if (val instanceof BigDecimal) {
                    record.setDecimal(idx, (BigDecimal) val);
                } else {
                    record.setDecimal(idx, new BigDecimal(val.toString()));
                }
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                if (val instanceof byte[]) {
                    record.setString(idx, new String((byte[]) val));
                } else {
                    record.setString(idx, val.toString());
                }
                break;
            case DATE:
                if (val instanceof LocalDate) {
                    java.sql.Date sqlDate = java.sql.Date.valueOf((LocalDate) val);
                    record.setDatetime(idx, sqlDate);
                } else {
                    record.setString(idx, val.toString());
                }
                break;
            case DATETIME:
            case TIMESTAMP:
                if (val instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) val;
                    Timestamp ts = Timestamp.valueOf(ldt);
                    record.setDatetime(idx, ts);
                } else if (val instanceof Timestamp) {
                    record.setDatetime(idx, (Timestamp) val);
                } else {
                    record.setString(idx, val.toString());
                }
                break;
            case BINARY:
                if (val instanceof byte[]) {
                    record.setString(idx, new String((byte[]) val));
                } else {
                    record.setString(idx, val.toString());
                }
                break;
            default:
                record.setString(idx, val.toString());
                break;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (recordWriter != null) {
                recordWriter.close();
                recordWriter = null;
            }
            LOG.info("MaxComputeJniWriter closed: sessionId=" + sessionId
                    + ", partitionSpec=" + partitionSpec
                    + ", writtenRows=" + writtenRows
                    + ", committedBlockIds=" + committedBlockIds);
        } catch (Exception e) {
            String errorMsg = "Failed to close MaxCompute record writer";
            LOG.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> stats = new HashMap<>();
        stats.put("mc_session_id", sessionId);
        stats.put("mc_partition_spec", partitionSpec != null ? partitionSpec : "");
        StringBuilder blockIdsStr = new StringBuilder();
        for (int i = 0; i < committedBlockIds.size(); i++) {
            if (i > 0) {
                blockIdsStr.append(",");
            }
            blockIdsStr.append(committedBlockIds.get(i));
        }
        stats.put("mc_block_ids", blockIdsStr.toString());
        stats.put("counter:WrittenRows", String.valueOf(writtenRows));
        stats.put("bytes:WrittenBytes", String.valueOf(writtenBytes));
        stats.put("timer:WriteTime", String.valueOf(writeTime));
        stats.put("timer:ReadTableTime", String.valueOf(readTableTime));
        return stats;
    }
}
