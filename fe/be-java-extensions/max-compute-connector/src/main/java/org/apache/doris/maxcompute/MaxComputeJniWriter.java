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
import org.apache.doris.common.jni.vec.VectorTable;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ArrowOptions.TimestampUnit;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.type.TypeInfo;
import com.google.common.base.Strings;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MaxComputeJniWriter writes C++ Block data to MaxCompute tables via Storage API (Arrow).
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
    private static final String WRITE_SESSION_ID = "write_session_id";
    private static final String BLOCK_ID = "block_id";
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
    private String writeSessionId;
    private long blockId;
    private String partitionSpec;
    private int connectTimeout;
    private int readTimeout;
    private int retryCount;

    // Storage API objects
    private TableBatchWriteSession writeSession;
    private BatchWriter<VectorSchemaRoot> batchWriter;
    private BufferAllocator allocator;
    private List<TypeInfo> columnTypeInfos;
    private List<String> columnNames;
    private WriterCommitMessage commitMessage;

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
        this.writeSessionId = Objects.requireNonNull(params.get(WRITE_SESSION_ID),
                "required property '" + WRITE_SESSION_ID + "'.");
        this.blockId = Long.parseLong(params.getOrDefault(BLOCK_ID, "0"));
        this.partitionSpec = params.getOrDefault(PARTITION_SPEC, "");
        this.connectTimeout = Integer.parseInt(params.getOrDefault(CONNECT_TIMEOUT, "10"));
        this.readTimeout = Integer.parseInt(params.getOrDefault(READ_TIMEOUT, "120"));
        this.retryCount = Integer.parseInt(params.getOrDefault(RETRY_COUNT, "4"));
    }

    @Override
    public void open() throws IOException {
        try {
            Account account = new AliyunAccount(accessKey, secretKey);
            Odps odps = new Odps(account);
            odps.setDefaultProject(project);
            odps.setEndpoint(endpoint);

            Credentials credentials = Credentials.newBuilder().withAccount(odps.getAccount())
                    .withAppAccount(odps.getAppAccount()).build();

            RestOptions restOptions = RestOptions.newBuilder()
                    .withConnectTimeout(connectTimeout)
                    .withReadTimeout(readTimeout)
                    .withRetryTimes(retryCount).build();

            EnvironmentSettings settings = EnvironmentSettings.newBuilder()
                    .withCredentials(credentials)
                    .withServiceEndpoint(odps.getEndpoint())
                    .withQuotaName(Strings.isNullOrEmpty(quota) ? null : quota)
                    .withRestOptions(restOptions)
                    .build();

            // Restore the write session created by FE
            writeSession = new TableWriteSessionBuilder()
                    .identifier(com.aliyun.odps.table.TableIdentifier.of(project, tableName))
                    .withSessionId(writeSessionId)
                    .withSettings(settings)
                    .buildBatchWriteSession();

            // SDK skips ArrowOptions when restoring session via withSessionId,
            // set it via reflection to avoid NPE in ArrowWriterImpl
            ArrowOptions arrowOptions = ArrowOptions.newBuilder()
                    .withDatetimeUnit(TimestampUnit.MILLI)
                    .withTimestampUnit(TimestampUnit.MILLI)
                    .build();
            java.lang.reflect.Field arrowField = writeSession.getClass()
                    .getSuperclass().getDeclaredField("arrowOptions");
            arrowField.setAccessible(true);
            arrowField.set(writeSession, arrowOptions);

            // Get schema info for type mapping
            com.aliyun.odps.table.DataSchema dataSchema = writeSession.requiredSchema();
            columnTypeInfos = new java.util.ArrayList<>();
            columnNames = new java.util.ArrayList<>();
            for (com.aliyun.odps.Column col : dataSchema.getColumns()) {
                columnTypeInfos.add(col.getTypeInfo());
                columnNames.add(col.getName());
            }

            allocator = new RootAllocator(Long.MAX_VALUE);

            // Create Arrow writer for this block
            WriterOptions writerOptions = WriterOptions.newBuilder()
                    .withSettings(settings)
                    .build();
            batchWriter = writeSession.createArrowWriter(blockId,
                    WriterAttemptId.of(0), writerOptions);

            LOG.info("MaxComputeJniWriter opened: project=" + project + ", table=" + tableName
                    + ", writeSessionId=" + writeSessionId + ", partitionSpec=" + partitionSpec
                    + ", blockId=" + blockId);
        } catch (Exception e) {
            String errorMsg = "Failed to open MaxCompute write session for table " + project + "." + tableName;
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

            // Get a pre-allocated VectorSchemaRoot from the batch writer
            VectorSchemaRoot root = batchWriter.newElement();
            root.setRowCount(numRows);

            for (int col = 0; col < numCols && col < columnTypeInfos.size(); col++) {
                OdpsType odpsType = columnTypeInfos.get(col).getOdpsType();
                fillArrowVector(root, col, odpsType, data[col], numRows);
            }

            batchWriter.write(root);
            writtenRows += numRows;
        } catch (Exception e) {
            String errorMsg = "Failed to write data to MaxCompute table " + project + "." + tableName;
            LOG.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        }
    }

    private void fillArrowVector(VectorSchemaRoot root, int colIdx, OdpsType odpsType,
                                  Object[] colData, int numRows) {
        switch (odpsType) {
            case BOOLEAN: {
                BitVector vec = (BitVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, (Boolean) colData[i] ? 1 : 0);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case TINYINT: {
                TinyIntVector vec = (TinyIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) colData[i]).byteValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case SMALLINT: {
                SmallIntVector vec = (SmallIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) colData[i]).shortValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case INT: {
                IntVector vec = (IntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) colData[i]).intValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case BIGINT: {
                BigIntVector vec = (BigIntVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) colData[i]).longValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case FLOAT: {
                Float4Vector vec = (Float4Vector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) colData[i]).floatValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DOUBLE: {
                Float8Vector vec = (Float8Vector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        vec.set(i, ((Number) colData[i]).doubleValue());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DECIMAL: {
                DecimalVector vec = (DecimalVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        BigDecimal bd = (colData[i] instanceof BigDecimal)
                                ? (BigDecimal) colData[i]
                                : new BigDecimal(colData[i].toString());
                        vec.set(i, bd);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case STRING:
            case VARCHAR:
            case CHAR: {
                VarCharVector vec = (VarCharVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        byte[] bytes;
                        if (colData[i] instanceof byte[]) {
                            bytes = (byte[]) colData[i];
                        } else {
                            bytes = colData[i].toString().getBytes(StandardCharsets.UTF_8);
                        }
                        vec.setSafe(i, bytes);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DATE: {
                DateDayVector vec = (DateDayVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else if (colData[i] instanceof LocalDate) {
                        vec.set(i, (int) ((LocalDate) colData[i]).toEpochDay());
                    } else {
                        vec.set(i, (int) LocalDate.parse(colData[i].toString()).toEpochDay());
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case DATETIME:
            case TIMESTAMP: {
                TimeStampMilliVector vec = (TimeStampMilliVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else if (colData[i] instanceof LocalDateTime) {
                        long millis = ((LocalDateTime) colData[i])
                                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        vec.set(i, millis);
                    } else if (colData[i] instanceof java.sql.Timestamp) {
                        vec.set(i, ((java.sql.Timestamp) colData[i]).getTime());
                    } else {
                        long millis = LocalDateTime.parse(colData[i].toString())
                                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        vec.set(i, millis);
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case BINARY: {
                VarBinaryVector vec = (VarBinaryVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else if (colData[i] instanceof byte[]) {
                        vec.setSafe(i, (byte[]) colData[i]);
                    } else {
                        vec.setSafe(i, colData[i].toString().getBytes(StandardCharsets.UTF_8));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
            case ARRAY: {
                ListVector listVec = (ListVector) root.getVector(colIdx);
                listVec.allocateNew();
                FieldVector dataVec = listVec.getDataVector();
                int elemIdx = 0;
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        listVec.setNull(i);
                    } else {
                        List<?> list = (List<?>) colData[i];
                        listVec.startNewValue(i);
                        for (Object elem : list) {
                            writeListElement(dataVec, elemIdx++, elem);
                        }
                        listVec.endValue(i, list.size());
                    }
                }
                listVec.setValueCount(numRows);
                dataVec.setValueCount(elemIdx);
                break;
            }
            case MAP: {
                MapVector mapVec = (MapVector) root.getVector(colIdx);
                mapVec.allocateNew();
                StructVector structVec = (StructVector) mapVec.getDataVector();
                FieldVector keyVec = structVec.getChildrenFromFields().get(0);
                FieldVector valVec = structVec.getChildrenFromFields().get(1);
                int elemIdx = 0;
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        mapVec.setNull(i);
                    } else {
                        Map<?, ?> map = (Map<?, ?>) colData[i];
                        mapVec.startNewValue(i);
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            structVec.setIndexDefined(elemIdx);
                            writeListElement(keyVec, elemIdx, entry.getKey());
                            writeListElement(valVec, elemIdx, entry.getValue());
                            elemIdx++;
                        }
                        mapVec.endValue(i, map.size());
                    }
                }
                mapVec.setValueCount(numRows);
                structVec.setValueCount(elemIdx);
                keyVec.setValueCount(elemIdx);
                valVec.setValueCount(elemIdx);
                break;
            }
            case STRUCT: {
                StructVector structVec = (StructVector) root.getVector(colIdx);
                structVec.allocateNew();
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        structVec.setNull(i);
                    } else {
                        structVec.setIndexDefined(i);
                        Map<?, ?> struct = (Map<?, ?>) colData[i];
                        for (FieldVector childVec : structVec.getChildrenFromFields()) {
                            Object val = struct.get(childVec.getName());
                            writeListElement(childVec, i, val);
                        }
                    }
                }
                structVec.setValueCount(numRows);
                for (FieldVector childVec : structVec.getChildrenFromFields()) {
                    childVec.setValueCount(numRows);
                }
                break;
            }
            default: {
                // Fallback: write as VarChar
                VarCharVector vec = (VarCharVector) root.getVector(colIdx);
                vec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    if (colData[i] == null) {
                        vec.setNull(i);
                    } else {
                        vec.setSafe(i, colData[i].toString().getBytes(StandardCharsets.UTF_8));
                    }
                }
                vec.setValueCount(numRows);
                break;
            }
        }
    }

    private void writeListElement(FieldVector vec, int idx, Object elem) {
        if (elem == null) {
            if (vec instanceof BaseFixedWidthVector) {
                ((BaseFixedWidthVector) vec).setNull(idx);
            } else if (vec instanceof BaseVariableWidthVector) {
                ((BaseVariableWidthVector) vec).setNull(idx);
            } else if (vec instanceof StructVector) {
                ((StructVector) vec).setNull(idx);
            } else if (vec instanceof MapVector) {
                ((MapVector) vec).setNull(idx);
            } else if (vec instanceof ListVector) {
                ((ListVector) vec).setNull(idx);
            }
            return;
        }
        if (vec instanceof VarCharVector) {
            byte[] bytes = elem instanceof byte[] ? (byte[]) elem
                    : elem.toString().getBytes(StandardCharsets.UTF_8);
            ((VarCharVector) vec).setSafe(idx, bytes);
        } else if (vec instanceof IntVector) {
            ((IntVector) vec).setSafe(idx, ((Number) elem).intValue());
        } else if (vec instanceof BigIntVector) {
            ((BigIntVector) vec).setSafe(idx, ((Number) elem).longValue());
        } else if (vec instanceof Float8Vector) {
            ((Float8Vector) vec).setSafe(idx, ((Number) elem).doubleValue());
        } else if (vec instanceof Float4Vector) {
            ((Float4Vector) vec).setSafe(idx, ((Number) elem).floatValue());
        } else if (vec instanceof SmallIntVector) {
            ((SmallIntVector) vec).setSafe(idx, ((Number) elem).shortValue());
        } else if (vec instanceof TinyIntVector) {
            ((TinyIntVector) vec).setSafe(idx, ((Number) elem).byteValue());
        } else if (vec instanceof BitVector) {
            ((BitVector) vec).setSafe(idx, (Boolean) elem ? 1 : 0);
        } else if (vec instanceof DecimalVector) {
            BigDecimal bd = elem instanceof BigDecimal ? (BigDecimal) elem
                    : new BigDecimal(elem.toString());
            ((DecimalVector) vec).setSafe(idx, bd);
        } else if (vec instanceof StructVector) {
            StructVector structVec = (StructVector) vec;
            structVec.setIndexDefined(idx);
            Map<?, ?> struct = (Map<?, ?>) elem;
            for (FieldVector childVec : structVec.getChildrenFromFields()) {
                writeListElement(childVec, idx, struct.get(childVec.getName()));
            }
        } else if (vec instanceof MapVector) {
            MapVector mapVec = (MapVector) vec;
            StructVector entryVec = (StructVector) mapVec.getDataVector();
            FieldVector keyVec = entryVec.getChildrenFromFields().get(0);
            FieldVector valVec = entryVec.getChildrenFromFields().get(1);
            Map<?, ?> map = (Map<?, ?>) elem;
            int offset = mapVec.startNewValue(idx);
            int j = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                entryVec.setIndexDefined(offset + j);
                writeListElement(keyVec, offset + j, entry.getKey());
                writeListElement(valVec, offset + j, entry.getValue());
                j++;
            }
            mapVec.endValue(idx, map.size());
            entryVec.setValueCount(offset + j);
            keyVec.setValueCount(offset + j);
            valVec.setValueCount(offset + j);
        } else if (vec instanceof ListVector) {
            ListVector listVec = (ListVector) vec;
            FieldVector dataVec = listVec.getDataVector();
            List<?> list = (List<?>) elem;
            int offset = listVec.startNewValue(idx);
            for (int j = 0; j < list.size(); j++) {
                writeListElement(dataVec, offset + j, list.get(j));
            }
            listVec.endValue(idx, list.size());
            dataVec.setValueCount(offset + list.size());
        } else {
            byte[] bytes = elem.toString().getBytes(StandardCharsets.UTF_8);
            ((VarCharVector) vec).setSafe(idx, bytes);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (batchWriter != null) {
                commitMessage = batchWriter.commit();
                batchWriter = null;
            }
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
            LOG.info("MaxComputeJniWriter closed: writeSessionId=" + writeSessionId
                    + ", partitionSpec=" + partitionSpec
                    + ", writtenRows=" + writtenRows
                    + ", blockId=" + blockId);
        } catch (Exception e) {
            String errorMsg = "Failed to close MaxCompute arrow writer";
            LOG.error(errorMsg, e);
            throw new IOException(errorMsg, e);
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> stats = new HashMap<>();
        stats.put("mc_partition_spec", partitionSpec != null ? partitionSpec : "");

        // Serialize WriterCommitMessage to Base64
        if (commitMessage != null) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(commitMessage);
                oos.close();
                stats.put("mc_commit_message", Base64.getEncoder().encodeToString(baos.toByteArray()));
            } catch (IOException e) {
                LOG.error("Failed to serialize WriterCommitMessage", e);
            }
        }

        stats.put("counter:WrittenRows", String.valueOf(writtenRows));
        stats.put("bytes:WrittenBytes", String.valueOf(writtenBytes));
        stats.put("timer:WriteTime", String.valueOf(writeTime));
        stats.put("timer:ReadTableTime", String.valueOf(readTableTime));
        return stats;
    }
}
