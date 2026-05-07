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

import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.holders.NullableTimeStampMicroHolder;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonJniWriter {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonJniWriter.class);

    private BatchTableWrite writer;
    private final BufferAllocator allocator;
    private final CommitMessageSerializer serializer = new CommitMessageSerializer();
    private String tableLocation;
    private static final String PAIMON_OPTION_PREFIX = "paimon.";
    private static final String HADOOP_OPTION_PREFIX = "hadoop.";

    private PreExecutionAuthenticator preExecutionAuthenticator;
    private final ClassLoader classLoader;

    private Map<String, String> paimonOptionParams;
    private Map<String, String> hadoopOptionParams;

    private List<DataField> paimonFields;
    private Map<String, DataField> paimonFieldMap;
    private DataType[] targetTypes;

    private IOManager ioManager;
    private HeapMemorySegmentPool memorySegmentPool;
    private boolean isDynamicBucketMode;

    public PaimonJniWriter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.classLoader = this.getClass().getClassLoader();
        try {
            setWarnLevel("org.apache.paimon.shade.org.apache.parquet");
            setWarnLevel("org.apache.paimon");
        } catch (Throwable t) {
            LOG.warn(t.getMessage(), t);
        }
    }

    private void setWarnLevel(String loggerName) {
        org.slf4j.Logger targetLogger = org.slf4j.LoggerFactory.getLogger(loggerName);
        try {
            Class<?> logbackLoggerClass = Class.forName("ch.qos.logback.classic.Logger");
            Class<?> levelClass = Class.forName("ch.qos.logback.classic.Level");
            if (!logbackLoggerClass.isInstance(targetLogger)) {
                return;
            }
            Object warnLevel = levelClass.getField("WARN").get(null);
            logbackLoggerClass.getMethod("setLevel", levelClass).invoke(targetLogger, warnLevel);
        } catch (Throwable t) {
            LOG.debug("set logger level skipped for {}", loggerName, t);
        }
    }

    /**
     * Opens the table and initializes the Writer
     */
    public void open(String tableLocation, Map<String, String> options, String[] columnNames) throws Exception {
        this.tableLocation = tableLocation;
        Thread.currentThread().setContextClassLoader(classLoader);
        paimonOptionParams = options.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(PAIMON_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(PAIMON_OPTION_PREFIX.length()),
                                kv1 -> kv1.getValue()));
        hadoopOptionParams = options.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(HADOOP_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(HADOOP_OPTION_PREFIX.length()),
                                kv1 -> kv1.getValue()));
        if (!paimonOptionParams.containsKey("warehouse") && tableLocation != null) {
            paimonOptionParams.put("warehouse", tableLocation);
        }
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(options);
        preExecutionAuthenticator.execute(() -> {
            try {
                LOG.info("paimon: opening writer, location={}, opts_size={}", tableLocation, options.size());
                Table table;
                if (options.containsKey("serialized_table")) {
                    table = PaimonUtils.deserialize(options.get("serialized_table"));
                } else {
                    Catalog catalog = createCatalog(paimonOptionParams, hadoopOptionParams);
                    String dbName = options.getOrDefault("db_name", "default");
                    String tblName = options.getOrDefault("table_name", "paimon_table");
                    table = catalog.getTable(Identifier.create(dbName, tblName));
                }
                Map<String, String> dynamicOptions = new HashMap<>();
                if (options.containsKey("write-buffer-size")) {
                    dynamicOptions.put("write-buffer-size", options.get("write-buffer-size"));
                }
                copyIfPresent(options, dynamicOptions, "write-buffer-spillable");
                copyIfPresent(options, dynamicOptions, "write-buffer-spill.max-disk-size");
                copyIfPresent(options, dynamicOptions, "sort-spill-buffer-size");
                copyIfPresent(options, dynamicOptions, "sort-spill-threshold");
                copyIfPresent(options, dynamicOptions, "spill-compression");
                boolean enableJniCompact = Boolean.parseBoolean(
                        options.getOrDefault("paimon_use_jni_compact", "false"));
                boolean enableInlineCompact = enableJniCompact;
                if (enableJniCompact && table instanceof org.apache.paimon.table.FileStoreTable) {
                    int tableBuckets = ((org.apache.paimon.table.FileStoreTable) table).schema().numBuckets();
                    enableInlineCompact = tableBuckets > 0;
                }
                if (enableInlineCompact) {
                    LOG.info("paimon: enabling inline compaction for JNI writer");
                    dynamicOptions.put("write-only", "false");
                    dynamicOptions.put("num-sorted-run.compaction-trigger", "10");
                    dynamicOptions.put("compaction.max.worker-num", "8");
                } else {
                    LOG.info("paimon: write-only mode, compaction disabled");
                    dynamicOptions.put("write-only", "true");
                }
                table = table.copy(dynamicOptions);
                LOG.info("paimon: applied dynamic options to table: {}", dynamicOptions);
                this.paimonFields = table.rowType().getFields();
                this.paimonFieldMap = new HashMap<>();
                for (DataField f : this.paimonFields) {
                    this.paimonFieldMap.put(f.name(), f);
                }
                this.targetTypes = buildTargetTypes(columnNames);
                this.writer = table.newBatchWriteBuilder().newWrite();
                if (table instanceof org.apache.paimon.table.FileStoreTable) {
                    this.isDynamicBucketMode =
                            ((org.apache.paimon.table.FileStoreTable) table).schema().numBuckets() == -1;
                }
                boolean spillEnabled = Boolean.parseBoolean(options.getOrDefault("write-buffer-spillable", "false"));
                if (spillEnabled) {
                    String spillDir = options.get("paimon_jni_spill_dir");
                    if (spillDir != null && !spillDir.isEmpty()) {
                        File spillFile = new File(spillDir);
                        if (!spillFile.exists()) {
                            spillFile.mkdirs();
                        }
                        this.ioManager = new IOManagerImpl(spillDir);
                    } else {
                        this.ioManager = new IOManagerImpl(System.getProperty("java.io.tmpdir"));
                    }
                    long globalPoolSize = Long.parseLong(options.getOrDefault(
                            "paimon_global_memory_pool_size", "1073741824"));
                    this.memorySegmentPool = new HeapMemorySegmentPool(globalPoolSize, 32 * 1024);
                    this.writer.withIOManager(ioManager).withMemoryPool(memorySegmentPool);
                    LOG.info("paimon: spill enabled, spill_dir={}", spillDir);
                }
                return null;
            } catch (Throwable t) {
                throw contextException("open", "options_size=" + options.size(), t);
            }
        });
    }

    private void copyIfPresent(Map<String, String> options, Map<String, String> dynamicOptions, String key) {
        String value = options.get(key);
        if (value != null) {
            dynamicOptions.put(key, value);
        }
    }

    /**
     * Receives Arrow IPC memory address from C++, deserializes and writes to Paimon
     */
    public void write(long address, int length) throws Exception {
        preExecutionAuthenticator.execute(() -> {
            try {
                ByteBuffer directBuffer = getDirectBuffer(address, length);
                try (ArrowStreamReader reader = new ArrowStreamReader(
                        new DirectBufInputStream(directBuffer), allocator)) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    while (reader.loadNextBatch()) {
                        writeBatch(root);
                    }
                }
                return null;
            } catch (Throwable t) {
                throw contextException("write", "address=" + address + ", length=" + length, t);
            }
        });
    }

    private void writeBatch(VectorSchemaRoot root) throws Exception {
        int rowCount = root.getRowCount();
        if (rowCount == 0) {
            return;
        }
        List<Field> fields = root.getSchema().getFields();
        List<FieldVector> vectors = root.getFieldVectors();
        int colCount = fields.size();
        DataType[] currentTargetTypes = resolveTargetTypes(fields);
        GenericRow reusedRow = new GenericRow(colCount);
        for (int i = 0; i < rowCount; i++) {
            for (int col = 0; col < colCount; col++) {
                Field arrowField = fields.get(col);
                try {
                    reusedRow.setField(col, readArrowValue(vectors.get(col), i, arrowField, currentTargetTypes[col]));
                } catch (Throwable t) {
                    String fieldName = arrowField == null ? "null" : arrowField.getName();
                    String currentTargetType = currentTargetTypes[col] == null
                            ? "null"
                            : currentTargetTypes[col].asSQLString();
                    throw contextException("writeBatch.convert",
                            "row=" + i + ", col=" + col + ", field=" + fieldName + ", targetType="
                                    + currentTargetType + ", rowCount=" + rowCount + ", colCount=" + colCount,
                            t);
                }
            }
            try {
                if (isDynamicBucketMode) {
                    writer.write(reusedRow, 0);
                } else {
                    writer.write(reusedRow);
                }
            } catch (Throwable t) {
                throw contextException("writeBatch.write",
                        "row=" + i + ", rowCount=" + rowCount + ", colCount=" + colCount,
                        t);
            }
        }
    }

    private Object readArrowValue(FieldVector vector, int row, Field arrowField, DataType targetType) {
        if (vector == null || vector.isNull(row)) {
            return null;
        }
        if (targetType instanceof BinaryType || targetType instanceof VarBinaryType) {
            if (vector instanceof VarBinaryVector) {
                return ((VarBinaryVector) vector).get(row);
            }
            if (vector instanceof VarCharVector) {
                return ((VarCharVector) vector).get(row);
            }
        } else {
            if (vector instanceof VarCharVector) {
                return BinaryString.fromBytes(((VarCharVector) vector).get(row));
            }
        }
        if (vector instanceof BitVector) {
            return ((BitVector) vector).get(row) == 1;
        }
        if (vector instanceof TinyIntVector) {
            return ((TinyIntVector) vector).get(row);
        }
        if (vector instanceof SmallIntVector) {
            return ((SmallIntVector) vector).get(row);
        }
        if (vector instanceof IntVector) {
            return ((IntVector) vector).get(row);
        }
        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(row);
        }
        if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(row);
        }
        if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(row);
        }
        if (vector instanceof DateDayVector) {
            return ((DateDayVector) vector).get(row);
        }
        if (vector instanceof TimeStampMicroVector) {
            NullableTimeStampMicroHolder holder = new NullableTimeStampMicroHolder();
            ((TimeStampMicroVector) vector).get(row, holder);
            return Timestamp.fromMicros(holder.value);
        }
        if (vector instanceof DecimalVector) {
            return readDecimal((DecimalVector) vector, row);
        }
        Object val = vector.getObject(row);
        return convertToPaimonType(val, arrowField, targetType);
    }

    private Decimal readDecimal(DecimalVector vector, int row) {
        BigDecimal bd = getBigDecimalFromArrowBuf(vector.getDataBuffer(), row,
                vector.getScale(), DecimalVector.TYPE_WIDTH);
        return Decimal.fromBigDecimal(bd, vector.getPrecision(), vector.getScale());
    }

    private Object convertToPaimonType(Object val, Field arrowField, DataType targetType) {
        if (val == null) {
            return null;
        }

        if (targetType instanceof BinaryType || targetType instanceof VarBinaryType) {
            if (val instanceof byte[]) {
                return val;
            } else if (val instanceof BinaryString) {
                return ((BinaryString) val).toBytes();
            } else if (val instanceof org.apache.arrow.vector.util.Text) {
                return ((org.apache.arrow.vector.util.Text) val).copyBytes();
            } else if (val instanceof org.apache.hadoop.io.Text) {
                org.apache.hadoop.io.Text t = (org.apache.hadoop.io.Text) val;
                byte[] bytes = new byte[t.getLength()];
                System.arraycopy(t.getBytes(), 0, bytes, 0, t.getLength());
                return bytes;
            } else if (val instanceof String) {
                return ((String) val).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            } else {
                return val.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        }

        if (val instanceof BinaryString) {
            return val;
        }
        if (val instanceof byte[]) {
            return BinaryString.fromBytes((byte[]) val);
        }
        if (val instanceof org.apache.arrow.vector.util.Text) {
            return BinaryString.fromBytes(((org.apache.arrow.vector.util.Text) val).copyBytes());
        }
        if (val instanceof org.apache.hadoop.io.Text) {
            org.apache.hadoop.io.Text t = (org.apache.hadoop.io.Text) val;
            return BinaryString.fromBytes(t.getBytes(), 0, t.getLength());
        }
        if (val instanceof CharSequence) {
            return BinaryString.fromString(val.toString());
        }


        ArrowType arrowType = arrowField != null ? arrowField.getType() : null;
        ArrowType.ArrowTypeID typeID = arrowType != null ? arrowType.getTypeID() : null;

        if (val instanceof LocalDateTime) {
            return Timestamp.fromLocalDateTime((LocalDateTime) val);
        }
        if (val instanceof Long && typeID == ArrowType.ArrowTypeID.Timestamp) {
            return Timestamp.fromMicros((Long) val);
        }
        if (val instanceof java.time.LocalDate) {
            return (int) ((java.time.LocalDate) val).toEpochDay();
        }
        if (val instanceof Integer && typeID == ArrowType.ArrowTypeID.Date) {
            return val;
        }
        if (val instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) val;
            return Decimal.fromBigDecimal(bd, bd.precision(), bd.scale());
        }
        if (targetType instanceof RowType && val instanceof Map) {
            RowType rowType = (RowType) targetType;
            Map<?, ?> mapVal = (Map<?, ?>) val;
            List<DataField> childFields = rowType.getFields();
            GenericRow structRow = new GenericRow(childFields.size());

            for (int i = 0; i < childFields.size(); i++) {
                DataField childField = childFields.get(i);
                Object childVal = mapVal.get(childField.name());

                Field childArrowField = null;
                if (arrowField != null && arrowField.getChildren() != null) {
                    for (Field f : arrowField.getChildren()) {
                        if (f.getName().equals(childField.name())) {
                            childArrowField = f;
                            break;
                        }
                    }
                }
                structRow.setField(i, convertToPaimonType(childVal, childArrowField, childField.type()));
            }
            return structRow;
        }
        if (targetType instanceof MapType && val instanceof List) {
            MapType mapType = (MapType) targetType;
            List<?> list = (List<?>) val;
            Map<Object, Object> convertedMap = new HashMap<>();

            Field keyArrowField = null;
            Field valueArrowField = null;
            if (arrowField != null && !arrowField.getChildren().isEmpty()) {
                Field entriesStruct = arrowField.getChildren().get(0);
                if (entriesStruct.getChildren().size() >= 2) {
                    keyArrowField = entriesStruct.getChildren().get(0);
                    valueArrowField = entriesStruct.getChildren().get(1);
                }
            }

            String keyName = keyArrowField != null ? keyArrowField.getName() : "key";
            String valueName = valueArrowField != null ? valueArrowField.getName() : "value";

            for (Object element : list) {
                if (element instanceof Map) { // Arrow 的内部 Struct 会被转成 Java Map
                    Map<?, ?> kvStruct = (Map<?, ?>) element;
                    Object k = convertToPaimonType(kvStruct.get(keyName), keyArrowField, mapType.getKeyType());
                    Object v = convertToPaimonType(kvStruct.get(valueName), valueArrowField, mapType.getValueType());
                    convertedMap.put(k, v);
                }
            }
            return new GenericMap(convertedMap);
        }
        if (targetType instanceof ArrayType && val instanceof List) {
            ArrayType arrayType = (ArrayType) targetType;
            List<?> list = (List<?>) val;
            Object[] convertedArray = new Object[list.size()];
            Field childArrowField = (arrowField != null && !arrowField.getChildren().isEmpty())
                    ? arrowField.getChildren().get(0) : null;

            for (int i = 0; i < list.size(); i++) {
                convertedArray[i] = convertToPaimonType(list.get(i), childArrowField, arrayType.getElementType());
            }
            return new GenericArray(convertedArray);
        }

        if (val instanceof byte[]) {
            return val;
        }

        return val;
    }

    public byte[][] prepareCommit() throws Exception {
        if (writer == null) {
            return new byte[0][];
        }
        return preExecutionAuthenticator.execute(() -> {
            try {
                List<CommitMessage> messages = writer.prepareCommit();

                if (messages == null || messages.isEmpty()) {
                    LOG.info("paimon: prepareCommit returns empty, location={}", tableLocation);
                    return new byte[0][];
                }
                LOG.info("paimon: prepareCommit returns {} messages", messages.size());
                final int maxPayloadBytes = 8 * 1024 * 1024;
                int chunkSize = 512;
                java.util.ArrayList<byte[]> payloads = new java.util.ArrayList<>();
                int i = 0;
                while (i < messages.size()) {
                    int end = Math.min(i + chunkSize, messages.size());
                    DataOutputSerializer outputView = new DataOutputSerializer(1024);
                    serializer.serializeList(messages.subList(i, end), outputView);
                    byte[] data = outputView.getCopyOfBuffer();
                    int len = data.length;
                    int version = serializer.getVersion();
                    byte[] payload = new byte[12 + len];
                    payload[0] = 'D';
                    payload[1] = 'P';
                    payload[2] = 'C';
                    payload[3] = 'M';
                    payload[4] = (byte) ((version >>> 24) & 0xFF);
                    payload[5] = (byte) ((version >>> 16) & 0xFF);
                    payload[6] = (byte) ((version >>> 8) & 0xFF);
                    payload[7] = (byte) (version & 0xFF);
                    payload[8] = (byte) ((len >>> 24) & 0xFF);
                    payload[9] = (byte) ((len >>> 16) & 0xFF);
                    payload[10] = (byte) ((len >>> 8) & 0xFF);
                    payload[11] = (byte) (len & 0xFF);
                    System.arraycopy(data, 0, payload, 12, len);
                    if (payload.length > maxPayloadBytes && chunkSize > 1) {
                        chunkSize = Math.max(1, chunkSize / 2);
                        continue;
                    }
                    payloads.add(payload);
                    i = end;
                }
                return payloads.toArray(new byte[0][]);
            } catch (Throwable t) {
                throw contextException("prepareCommit", "tableLocation=" + tableLocation, t);
            }
        });
    }

    public void abort() {
        try {
            if (preExecutionAuthenticator != null) {
                preExecutionAuthenticator.execute(() -> {
                    closeWriterResources(false);
                    return null;
                });
            }
        } catch (Exception e) {
            LOG.error("Failed to abort paimon writer", e);
        }
    }

    public void close() throws Exception {
        try {
            if (preExecutionAuthenticator != null) {
                preExecutionAuthenticator.execute(() -> {
                    closeWriterResources(true);
                    return null;
                });
            }
        } catch (Exception e) {
            LOG.warn("Error while closing PaimonJniWriter", e);
            throw e;
        }
    }

    private void closeWriterResources(boolean closeAllocator) throws Exception {
        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            if (ioManager != null) {
                ioManager.close();
                ioManager = null;
            }
            memorySegmentPool = null;
            if (closeAllocator && allocator != null) {
                allocator.close();
            }
        } catch (Throwable t) {
            throw contextException("close", "closeAllocator=" + closeAllocator, t);
        }
    }

    private RuntimeException contextException(String phase, String detail, Throwable cause) {
        return new RuntimeException("Paimon JNI writer failed in phase=" + phase + ", detail={" + detail + "}, state={"
                + currentStateSummary() + "}", cause);
    }

    private DataType[] buildTargetTypes(String[] columnNames) {
        if (columnNames == null) {
            return null;
        }
        DataType[] result = new DataType[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            DataField field = paimonFieldMap.get(columnNames[i]);
            if (field != null) {
                result[i] = field.type();
            } else if (i < paimonFields.size()) {
                result[i] = paimonFields.get(i).type();
            }
        }
        return result;
    }

    private DataType[] resolveTargetTypes(List<Field> fields) {
        if (targetTypes != null && targetTypes.length == fields.size()) {
            return targetTypes;
        }
        DataType[] result = new DataType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field arrowField = fields.get(i);
            DataField field = paimonFieldMap.get(arrowField.getName());
            if (field != null) {
                result[i] = field.type();
            } else if (i < paimonFields.size()) {
                result[i] = paimonFields.get(i).type();
            }
        }
        return result;
    }

    private BigDecimal getBigDecimalFromArrowBuf(org.apache.arrow.memory.ArrowBuf byteBuf,
            int index, int scale, int byteWidth) {
        byte[] value = new byte[byteWidth];
        byte temp;
        long startIndex = (long) index * byteWidth;
        byteBuf.getBytes(startIndex, value, 0, byteWidth);
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
            int stop = byteWidth / 2;
            for (int i = 0; i < stop; i++) {
                temp = value[i];
                int j = (byteWidth - 1) - i;
                value[i] = value[j];
                value[j] = temp;
            }
        }
        return new BigDecimal(new BigInteger(value), scale);
    }

    private String currentStateSummary() {
        return "tableLocation=" + tableLocation
                + ", writerNull=" + (writer == null)
                + ", ioManagerNull=" + (ioManager == null)
                + ", memoryPoolNull=" + (memorySegmentPool == null)
                + ", fieldCount=" + (paimonFields == null ? -1 : paimonFields.size())
                + ", fieldMapSize=" + (paimonFieldMap == null ? -1 : paimonFieldMap.size());
    }

    private ByteBuffer getDirectBuffer(long address, int length) throws Exception {
        Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
        java.lang.reflect.Constructor<?> ctor = cls.getDeclaredConstructor(long.class, int.class);
        ctor.setAccessible(true);
        return (ByteBuffer) ctor.newInstance(address, length);
    }

    private static class DirectBufInputStream extends InputStream {
        private final ByteBuffer buf;

        public DirectBufInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public int read() {
            return buf.hasRemaining() ? buf.get() & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (!buf.hasRemaining()) {
                return -1;
            }
            int toRead = Math.min(len, buf.remaining());
            buf.get(b, off, toRead);
            return toRead;
        }
    }

    private static Catalog createCatalog(
            Map<String, String> paimonOptionParams,
            Map<String, String> hadoopOptionParams) {
        Options options = new Options();
        paimonOptionParams.entrySet().stream().forEach(kv -> options.set(kv.getKey(), kv.getValue()));
        Configuration configuration;
        configuration = new Configuration();
        hadoopOptionParams.entrySet().stream().forEach(kv -> configuration.set(kv.getKey(), kv.getValue()));
        String hadoopConfigPath = options.getString("hadoop-conf-dir", (String) null);
        if (hadoopConfigPath != null) {
            String coreSiteFile = String.format("%score-site.xml", hadoopConfigPath);
            Path coreSitePath = new Path(coreSiteFile);
            String hdfsSiteFile = String.format("%shdfs-site.xml", hadoopConfigPath);
            Path hdfsSitePath = new Path(hdfsSiteFile);
            configuration.addResource(coreSitePath);
            configuration.addResource(hdfsSitePath);
        }
        String hiveConfigPath = options.getString("hive-conf-dir", (String) null);
        if (hiveConfigPath != null) {
            String hiveSiteFile = String.format("%shive-site.xml", hiveConfigPath);
            Path hiveSitePath = new Path(hiveSiteFile);
            configuration.addResource(hiveSitePath);
        }
        paimonOptionParams.entrySet().forEach(entry -> configuration.set(entry.getKey(), entry.getValue()));
        CatalogContext context = CatalogContext.create(options, configuration);
        return CatalogFactory.createCatalog(context);
    }

}
