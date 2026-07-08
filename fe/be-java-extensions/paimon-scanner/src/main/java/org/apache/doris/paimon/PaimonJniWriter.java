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
import org.apache.paimon.CoreOptions;
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
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JNI entry point for Paimon write operations.
 *
 * Called from C++ (VPaimonTableWriter) via JNI. Data path:
 *
 *   C++ Block → Arrow IPC Stream → direct memory address
 *   → PaimonJniWriter.write(address, length)
 *   → ArrowStreamReader → VectorSchemaRoot
 *   → convert row-by-row → Paimon GenericRow
 *   → BatchTableWrite.write(row) / write(row, bucket)
 *
 * Commit path:
 *
 *   VPaimonTableWriter::close() → JNI → PaimonJniWriter.prepareCommit()
 *   → CommitMessageSerializer → byte[][]
 *   → C++ collects TPaimonCommitMessage → FE PaimonTransaction
 */
public class PaimonJniWriter {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonJniWriter.class);

    private static final String OPTION_PREFIX_PAIMON = "paimon.";
    private static final String OPTION_PREFIX_HADOOP = "hadoop.";
    private static final String KEY_SERIALIZED_TABLE = "serialized_table";
    private static final String KEY_COMMIT_IDENTIFIER = "doris.commit_identifier";
    private static final String KEY_COMMIT_USER = "doris.commit_user";
    private static final String KEY_SPILL_DIR = "paimon_jni_spill_dir";

    static final int COMMIT_PAYLOAD_HEADER_BYTES = 12;
    static final int MAX_COMMIT_PAYLOAD_BYTES = 8 * 1024 * 1024;
    static final int DEFAULT_COMMIT_CHUNK_SIZE = 512;

    private BatchTableWrite writer;
    private TableWriteImpl<?> tableWrite;
    private RowKeyExtractor rowKeyExtractor;

    private final BufferAllocator allocator;
    private final CommitMessageSerializer serializer = new CommitMessageSerializer();
    private final ClassLoader classLoader;

    private String tableLocation;
    private PreExecutionAuthenticator preExecutionAuthenticator;

    private Map<String, DataField> paimonFieldMap;
    private DataType[] targetTypes;
    private BucketMode bucketMode;
    private boolean useExplicitBucketWrite;
    private long commitIdentifier;
    private int totalBuckets;

    private IOManager ioManager;
    private HeapMemorySegmentPool memorySegmentPool;

    private static volatile Constructor<?> directByteBufferConstructor;

    public PaimonJniWriter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.classLoader = this.getClass().getClassLoader();
        configureLogLevels();
    }

    // ────────────────────────────────────────────────────────────
    // JNI entry points (called from C++)
    // ────────────────────────────────────────────────────────────

    /**
     * Initialize the writer. Called once per BE fragment.
     *
     * @param tableLocation  Paimon table root path
     * @param options        merged options map (Paimon keys prefixed "paimon.",
     *                       Hadoop keys prefixed "hadoop.")

     * @param columnNames    output column names
     */
    public void open(String tableLocation, Map<String, String> options,
                     String[] columnNames) throws Exception {
        this.tableLocation = tableLocation;
        Thread.currentThread().setContextClassLoader(classLoader);

        Map<String, String> paimonOpts = extractPrefixed(options, OPTION_PREFIX_PAIMON);
        Map<String, String> hadoopOpts = extractPrefixed(options, OPTION_PREFIX_HADOOP);

        if (!paimonOpts.containsKey("warehouse") && tableLocation != null) {
            paimonOpts.put("warehouse", tableLocation);
        }

        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(options);
        preExecutionAuthenticator.execute(() -> {
            try {
                LOG.info("PaimonJniWriter opening: location={}, columns={}",
                        tableLocation, columnNames != null ? columnNames.length : 0);

                Table table = loadTable(options, paimonOpts, hadoopOpts);
                this.commitIdentifier = Long.parseLong(
                        options.getOrDefault(KEY_COMMIT_IDENTIFIER, "-1"));

                initFieldMap(table.rowType().getFields());
                RowType writeType = buildWriteType(columnNames);
                initWriter(table, options);

                if (!isFullRowWrite(table.rowType(), writeType)) {
                    if (!table.primaryKeys().isEmpty()) {
                        throw new UnsupportedOperationException(
                                "Paimon primary-key write requires full table row type");
                    }
                    this.writer.withWriteType(writeType);
                }
                return null;
            } catch (Throwable t) {
                throw new RuntimeException("PaimonJniWriter open failed: location="
                        + tableLocation, t);
            }
        });
    }

    /**
     * Write a batch of rows. Called once per Arrow IPC buffer from C++.
     *
     * @param address  native memory address of the Arrow IPC Stream bytes
     * @param length   number of bytes
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
                throw new RuntimeException("PaimonJniWriter write failed: addr="
                        + address + ", len=" + length, t);
            }
        });
    }

    /**
     * Prepare commit: flush all in-memory data, close files, serialize commit messages.
     *
     * @return byte[][]  each element is a DPCM-framed serialized CommitMessage chunk
     */
    public byte[][] prepareCommit() throws Exception {
        if (writer == null) {
            return new byte[0][];
        }
        return preExecutionAuthenticator.execute(() -> {
            try {
                List<CommitMessage> messages = doPrepareCommit();
                if (messages == null || messages.isEmpty()) {
                    LOG.info("PaimonJniWriter prepareCommit: empty, location={}", tableLocation);
                    return new byte[0][];
                }
                LOG.info("PaimonJniWriter prepareCommit: {} messages", messages.size());
                return serializeMessages(messages);
            } catch (Throwable t) {
                throw new RuntimeException("PaimonJniWriter prepareCommit failed: location="
                        + tableLocation, t);
            }
        });
    }

    /**
     * Abort: discard written data files. Called on error.
     */
    public void abort() throws Exception {
        try {
            if (preExecutionAuthenticator != null) {
                preExecutionAuthenticator.execute(() -> {
                    closeWriterResources(false);
                    return null;
                });
            }
        } catch (Exception e) {
            LOG.error("PaimonJniWriter abort failed", e);
            throw e;
        }
    }

    /**
     * Close: release all resources.
     */
    public void close() throws Exception {
        try {
            if (preExecutionAuthenticator != null) {
                preExecutionAuthenticator.execute(() -> {
                    closeWriterResources(true);
                    return null;
                });
            }
        } catch (Exception e) {
            LOG.warn("PaimonJniWriter close error", e);
            throw e;
        }
    }

    // ────────────────────────────────────────────────────────────
    // Initialization helpers
    // ────────────────────────────────────────────────────────────

    private static Map<String, String> extractPrefixed(Map<String, String> options,
                                                        String prefix) {
        return options.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        kv -> kv.getKey().substring(prefix.length()),
                        Map.Entry::getValue));
    }

    private Table loadTable(Map<String, String> options,
                            Map<String, String> paimonOpts,
                            Map<String, String> hadoopOpts) throws Exception {
        if (options.containsKey(KEY_SERIALIZED_TABLE)) {
            return PaimonUtils.deserialize(options.get(KEY_SERIALIZED_TABLE));
        }
        Catalog catalog = createCatalog(paimonOpts, hadoopOpts);
        String dbName = options.getOrDefault("db_name", "default");
        String tblName = options.getOrDefault("table_name", "paimon_table");
        return catalog.getTable(Identifier.create(dbName, tblName));
    }

    private void initFieldMap(List<DataField> fields) {
        this.paimonFieldMap = new HashMap<>();
        for (DataField field : fields) {
            this.paimonFieldMap.put(field.name(), field);
        }
    }

    private void initWriter(Table table, Map<String, String> options) throws Exception {
        if (!(table instanceof FileStoreTable)) {
            this.writer = table.newBatchWriteBuilder().newWrite();
            return;
        }

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        CoreOptions coreOptions = CoreOptions.fromMap(fileStoreTable.options());
        this.totalBuckets = coreOptions.bucket();

        String commitUser = options.get(KEY_COMMIT_USER);
        if (commitUser == null || commitUser.isEmpty()) {
            commitUser = coreOptions.createCommitUser();
        }

        this.tableWrite = fileStoreTable.newWrite(commitUser);
        this.writer = this.tableWrite;
        initBucketMode(fileStoreTable);
        initSpillIfNeeded(coreOptions, options);
    }

    private void initBucketMode(FileStoreTable fileStoreTable) {
        this.bucketMode = fileStoreTable.bucketMode();
        if (bucketMode == BucketMode.HASH_DYNAMIC
                || bucketMode == BucketMode.KEY_DYNAMIC
                || bucketMode == BucketMode.POSTPONE_MODE) {
            throw new UnsupportedOperationException(
                    "Unsupported Paimon bucket mode for write: " + bucketMode
                    + ". Only HASH_FIXED and BUCKET_UNAWARE are supported in v1.");
        }
        this.useExplicitBucketWrite = bucketMode == BucketMode.HASH_FIXED;
        if (useExplicitBucketWrite) {
            this.rowKeyExtractor = fileStoreTable.createRowKeyExtractor();
        }
    }

    private void initSpillIfNeeded(CoreOptions coreOptions, Map<String, String> options)
            throws Exception {
        if (!coreOptions.writeBufferSpillable()) {
            return;
        }
        String spillDir = options.get(KEY_SPILL_DIR);
        if (spillDir == null || spillDir.isEmpty()) {
            spillDir = System.getProperty("java.io.tmpdir");
        } else {
            Files.createDirectories(Paths.get(spillDir));
        }
        this.ioManager = new IOManagerImpl(spillDir);
        this.memorySegmentPool = new HeapMemorySegmentPool(
                coreOptions.writeBufferSize(), coreOptions.pageSize());
        this.writer.withIOManager(ioManager).withMemoryPool(memorySegmentPool);
        LOG.info("PaimonJniWriter spill enabled: dir={}", spillDir);
    }

    // ────────────────────────────────────────────────────────────
    // Data writing
    // ────────────────────────────────────────────────────────────

    private void writeBatch(VectorSchemaRoot root) throws Exception {
        int rowCount = root.getRowCount();
        if (rowCount == 0) {
            return;
        }
        List<Field> fields = root.getSchema().getFields();
        List<FieldVector> vectors = root.getFieldVectors();
        int colCount = fields.size();
        DataType[] currentTypes = resolveTargetTypes(fields);
        GenericRow reusedRow = new GenericRow(colCount);

        for (int i = 0; i < rowCount; i++) {
            for (int col = 0; col < colCount; col++) {
                try {
                    reusedRow.setField(col,
                            readArrowValue(vectors.get(col), i, fields.get(col), currentTypes[col]));
                } catch (Throwable t) {
                    throw new RuntimeException("PaimonJniWriter row conversion failed: row=" + i
                            + ", col=" + col + ", field=" + fields.get(col).getName()
                            + ", targetType=" + (currentTypes[col] != null
                                    ? currentTypes[col].asSQLString() : "null"), t);
                }
            }
            try {
                if (useExplicitBucketWrite) {
                    rowKeyExtractor.setRecord(reusedRow);
                    writer.write(reusedRow, rowKeyExtractor.bucket());
                } else {
                    writer.write(reusedRow);
                }
            } catch (Throwable t) {
                throw new RuntimeException("PaimonJniWriter write failed: row=" + i
                        + ", totalRows=" + rowCount, t);
            }
        }
    }

    // ────────────────────────────────────────────────────────────
    // Arrow → Paimon type conversion
    // ────────────────────────────────────────────────────────────

    private Object readArrowValue(FieldVector vector, int row, Field arrowField,
                                   DataType targetType) {
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object convertToPaimonType(Object val, Field arrowField, DataType targetType) {
        if (val == null) {
            return null;
        }
        if (targetType instanceof BinaryType || targetType instanceof VarBinaryType) {
            if (val instanceof byte[]) {
                return val;
            }
            if (val instanceof BinaryString) {
                return ((BinaryString) val).toBytes();
            }
            if (val instanceof org.apache.arrow.vector.util.Text) {
                return ((org.apache.arrow.vector.util.Text) val).copyBytes();
            }
            if (val instanceof String) {
                return ((String) val).getBytes(StandardCharsets.UTF_8);
            }
            return val.toString().getBytes(StandardCharsets.UTF_8);
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

        ArrowType.ArrowTypeID typeID = arrowField != null
                ? arrowField.getType().getTypeID() : null;

        if (val instanceof LocalDateTime) {
            return Timestamp.fromLocalDateTime((LocalDateTime) val);
        }
        if (val instanceof Long && typeID == ArrowType.ArrowTypeID.Timestamp) {
            return Timestamp.fromMicros((Long) val);
        }
        if (val instanceof Integer && typeID == ArrowType.ArrowTypeID.Date) {
            return val;
        }
        if (val instanceof java.time.LocalDate) {
            return (int) ((java.time.LocalDate) val).toEpochDay();
        }
        if (val instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) val;
            return Decimal.fromBigDecimal(bd, bd.precision(), bd.scale());
        }
        if (targetType instanceof RowType && val instanceof Map) {
            return convertStruct((Map<?, ?>) val, (RowType) targetType, arrowField);
        }
        if (targetType instanceof MapType && val instanceof List) {
            return convertMap((List<?>) val, (MapType) targetType, arrowField);
        }
        if (targetType instanceof ArrayType && val instanceof List) {
            return convertArray((List<?>) val, (ArrayType) targetType, arrowField);
        }
        if (val instanceof byte[]) {
            return val;
        }
        return val;
    }

    private GenericRow convertStruct(Map<?, ?> mapVal, RowType rowType, Field arrowField) {
        List<DataField> childFields = rowType.getFields();
        GenericRow structRow = new GenericRow(childFields.size());
        for (int i = 0; i < childFields.size(); i++) {
            DataField childField = childFields.get(i);
            Object childVal = mapVal.get(childField.name());
            Field childArrowField = findChildField(arrowField, childField.name());
            structRow.setField(i,
                    convertToPaimonType(childVal, childArrowField, childField.type()));
        }
        return structRow;
    }

    private GenericMap convertMap(List<?> list, MapType mapType, Field arrowField) {
        Field keyArrowField = null;
        Field valueArrowField = null;
        if (arrowField != null && !arrowField.getChildren().isEmpty()) {
            Field entries = arrowField.getChildren().get(0);
            if (entries.getChildren().size() >= 2) {
                keyArrowField = entries.getChildren().get(0);
                valueArrowField = entries.getChildren().get(1);
            }
        }
        String keyName = keyArrowField != null ? keyArrowField.getName() : "key";
        String valueName = valueArrowField != null ? valueArrowField.getName() : "value";

        Map<Object, Object> converted = new HashMap<>();
        for (Object element : list) {
            if (element instanceof Map) {
                Map<?, ?> kv = (Map<?, ?>) element;
                Object k = convertToPaimonType(kv.get(keyName), keyArrowField, mapType.getKeyType());
                Object v = convertToPaimonType(kv.get(valueName), valueArrowField,
                        mapType.getValueType());
                converted.put(k, v);
            }
        }
        return new GenericMap(converted);
    }

    private GenericArray convertArray(List<?> list, ArrayType arrayType, Field arrowField) {
        Object[] converted = new Object[list.size()];
        Field childArrowField = null;
        if (arrowField != null && !arrowField.getChildren().isEmpty()) {
            childArrowField = arrowField.getChildren().get(0);
        }
        for (int i = 0; i < list.size(); i++) {
            converted[i] = convertToPaimonType(list.get(i), childArrowField,
                    arrayType.getElementType());
        }
        return new GenericArray(converted);
    }

    private static Field findChildField(Field parent, String name) {
        if (parent == null || parent.getChildren() == null) {
            return null;
        }
        for (Field f : parent.getChildren()) {
            if (f.getName().equals(name)) {
                return f;
            }
        }
        return null;
    }

    // ────────────────────────────────────────────────────────────
    // Commit & serialization
    // ────────────────────────────────────────────────────────────

    private List<CommitMessage> doPrepareCommit() throws Exception {
        return tableWrite != null && commitIdentifier > 0
                ? tableWrite.prepareCommit(true, commitIdentifier)
                : writer.prepareCommit();
    }

    private byte[][] serializeMessages(List<CommitMessage> messages) throws Exception {
        int chunkSize = DEFAULT_COMMIT_CHUNK_SIZE;
        List<byte[]> payloads = new ArrayList<>();
        int i = 0;
        while (i < messages.size()) {
            int end = Math.min(i + chunkSize, messages.size());
            byte[] payload = serializeChunk(messages.subList(i, end));
            if (payload.length > MAX_COMMIT_PAYLOAD_BYTES && chunkSize > 1) {
                chunkSize = Math.max(1, chunkSize / 2);
                continue;
            }
            payloads.add(payload);
            i = end;
        }
        return payloads.toArray(new byte[0][]);
    }

    private byte[] serializeChunk(List<CommitMessage> messages) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(1024);
        serializer.serializeList(messages, out);
        byte[] data = out.getCopyOfBuffer();
        int len = data.length;
        int version = serializer.getVersion();

        byte[] payload = new byte[COMMIT_PAYLOAD_HEADER_BYTES + len];
        // Magic bytes "DPCM"
        payload[0] = 'D';
        payload[1] = 'P';
        payload[2] = 'C';
        payload[3] = 'M';
        // Version (big-endian)
        payload[4] = (byte) ((version >>> 24) & 0xFF);
        payload[5] = (byte) ((version >>> 16) & 0xFF);
        payload[6] = (byte) ((version >>> 8) & 0xFF);
        payload[7] = (byte) (version & 0xFF);
        // Length (big-endian)
        payload[8]  = (byte) ((len >>> 24) & 0xFF);
        payload[9]  = (byte) ((len >>> 16) & 0xFF);
        payload[10] = (byte) ((len >>> 8) & 0xFF);
        payload[11] = (byte) (len & 0xFF);
        System.arraycopy(data, 0, payload, COMMIT_PAYLOAD_HEADER_BYTES, len);
        return payload;
    }

    // ────────────────────────────────────────────────────────────
    // Schema & type resolution
    // ────────────────────────────────────────────────────────────

    private RowType buildWriteType(String[] columnNames) {
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException(
                    "PaimonJniWriter requires explicit column names");
        }
        List<DataField> writeFields = new ArrayList<>(columnNames.length);
        this.targetTypes = new DataType[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            DataField field = paimonFieldMap.get(columnNames[i]);
            if (field == null) {
                throw new IllegalArgumentException(
                        "Paimon column '" + columnNames[i] + "' not found in table schema");
            }
            writeFields.add(field);
            this.targetTypes[i] = field.type();
        }
        return new RowType(writeFields);
    }

    private DataType[] resolveTargetTypes(List<Field> fields) {
        if (targetTypes != null && targetTypes.length == fields.size()) {
            return targetTypes;
        }
        DataType[] result = new DataType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DataField df = paimonFieldMap.get(fields.get(i).getName());
            if (df == null) {
                throw new IllegalArgumentException(
                        "Paimon column '" + fields.get(i).getName() + "' not found");
            }
            result[i] = df.type();
        }
        return result;
    }

    static boolean isFullRowWrite(RowType tableRowType, RowType writeType) {
        return tableRowType.getFieldNames().equals(writeType.getFieldNames());
    }

    // ────────────────────────────────────────────────────────────
    // Resource management
    // ────────────────────────────────────────────────────────────

    private void closeWriterResources(boolean closeAllocator) throws Exception {
        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            tableWrite = null;
            rowKeyExtractor = null;
            useExplicitBucketWrite = false;
            if (ioManager != null) {
                ioManager.close();
                ioManager = null;
            }
            memorySegmentPool = null;
            if (closeAllocator && allocator != null) {
                allocator.close();
            }
        } catch (Throwable t) {
            throw new RuntimeException("PaimonJniWriter close failed", t);
        }
    }

    // ────────────────────────────────────────────────────────────
    // Utilities
    // ────────────────────────────────────────────────────────────

    private static BigDecimal getBigDecimalFromArrowBuf(
            org.apache.arrow.memory.ArrowBuf byteBuf,
            int index, int scale, int byteWidth) {
        byte[] value = new byte[byteWidth];
        long startIndex = (long) index * byteWidth;
        byteBuf.getBytes(startIndex, value, 0, byteWidth);
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
            int stop = byteWidth / 2;
            for (int i = 0; i < stop; i++) {
                byte temp = value[i];
                int j = (byteWidth - 1) - i;
                value[i] = value[j];
                value[j] = temp;
            }
        }
        return new BigDecimal(new BigInteger(value), scale);
    }

    private static ByteBuffer getDirectBuffer(long address, int length) throws Exception {
        Constructor<?> ctor = directByteBufferConstructor;
        if (ctor == null) {
            Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
            ctor = cls.getDeclaredConstructor(long.class, int.class);
            ctor.setAccessible(true);
            directByteBufferConstructor = ctor;
        }
        return (ByteBuffer) ctor.newInstance(address, length);
    }

    private static Catalog createCatalog(Map<String, String> paimonOpts,
                                          Map<String, String> hadoopOpts) {
        Options options = new Options();
        paimonOpts.forEach(options::set);

        Configuration conf = new Configuration();
        hadoopOpts.forEach(conf::set);

        String hadoopConfDir = options.getString("hadoop-conf-dir", (String) null);
        if (hadoopConfDir != null) {
            conf.addResource(new Path(hadoopConfDir + "core-site.xml"));
            conf.addResource(new Path(hadoopConfDir + "hdfs-site.xml"));
        }
        String hiveConfDir = options.getString("hive-conf-dir", (String) null);
        if (hiveConfDir != null) {
            conf.addResource(new Path(hiveConfDir + "hive-site.xml"));
        }
        paimonOpts.forEach(conf::set);

        CatalogContext ctx = CatalogContext.create(options, conf);
        return CatalogFactory.createCatalog(ctx);
    }

    private static void configureLogLevels() {
        try {
            for (String name : new String[] {
                    "org.apache.paimon.shade.org.apache.parquet",
                    "org.apache.paimon"
            }) {
                org.slf4j.Logger targetLogger = org.slf4j.LoggerFactory.getLogger(name);
                Class<?> logbackCls = Class.forName("ch.qos.logback.classic.Logger");
                Class<?> levelCls = Class.forName("ch.qos.logback.classic.Level");
                if (logbackCls.isInstance(targetLogger)) {
                    Object warnLevel = levelCls.getField("WARN").get(null);
                    logbackCls.getMethod("setLevel", levelCls).invoke(targetLogger, warnLevel);
                }
            }
        } catch (Throwable t) {
            LOG.debug("PaimonJniWriter: log level configuration skipped", t);
        }
    }

    @SuppressWarnings("unused")
    private String currentState() {
        return "location=" + tableLocation
                + ", writerNull=" + (writer == null)
                + ", bucketMode=" + bucketMode
                + ", explicitBucket=" + useExplicitBucketWrite;
    }

    /** InputStream over a direct ByteBuffer (no copy). */
    private static class DirectBufInputStream extends InputStream {
        private final ByteBuffer buf;

        DirectBufInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public int read() {
            if (buf.hasRemaining()) {
                return buf.get() & 0xFF;
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (!buf.hasRemaining()) {
                return -1;
            }
            int n = Math.min(len, buf.remaining());
            buf.get(b, off, n);
            return n;
        }
    }
}
