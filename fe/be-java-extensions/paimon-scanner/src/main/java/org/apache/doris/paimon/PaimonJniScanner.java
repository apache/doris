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
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.kerberos.PreExecutionAuthenticator;
import org.apache.doris.kerberos.PreExecutionAuthenticatorCache;

import com.google.common.base.Preconditions;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class PaimonJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonJniScanner.class);
    private static final String HADOOP_OPTION_PREFIX = "hadoop.";
    private static final String PAIMON_OPTION_PREFIX = "paimon.";
    private static final String ASYNC_READER_THREAD_NAME_PREFIX = "paimon-reader-async-thread";
    private static final String FILE_READER_ASYNC_THRESHOLD = "file-reader-async-threshold";
    private static final String SERIALIZED_TABLE = "serialized_table";
    private static final int MAX_MANIFEST_PARALLELISM = 256;
    static final String DORIS_MANIFEST_PARALLELISM_CAP =
            "doris.scan.manifest.parallelism-cap";
    static final String DORIS_SERIALIZED_SYSTEM_SOURCE = "doris.serialized-system-source";
    static final String DORIS_SYSTEM_TABLE_TYPE = "doris.system-table-type";
    private static final String SERIALIZED_SYSTEM_SOURCE =
            PAIMON_OPTION_PREFIX + DORIS_SERIALIZED_SYSTEM_SOURCE;
    static final String ENABLE_JNI_IO_MANAGER = "paimon.jni.enable_jni_io_manager";
    static final String JNI_IO_MANAGER_TMP_DIR = "paimon.jni.io_manager.tmp_dir";
    static final String JNI_IO_MANAGER_IMPL_CLASS = "paimon.jni.io_manager.impl_class";
    private static final AtomicInteger ACTIVE_SCANNERS = new AtomicInteger();
    // Scanner profiles are collected per split, but the asynchronous reader pool is JVM-global.
    // Share one sample so profile collection does not allocate ThreadInfo for every scanner.
    private static final CachedThreadCounter ASYNC_READER_THREAD_COUNTER = new CachedThreadCounter(
            TimeUnit.SECONDS.toNanos(1L), System::nanoTime,
            () -> countThreadsByNamePrefix(ASYNC_READER_THREAD_NAME_PREFIX));

    private final Map<String, String> params;
    private final Map<String, String> hadoopOptionParams;
    private final String paimonSplit;
    private final String paimonPredicate;
    private final String tableCacheKey;
    private Table table;
    private PaimonTableCache.TableCacheEntry tableCacheEntry;
    private RecordReader<InternalRow> reader;
    private IOManager ioManager;
    private String ioManagerTempDirs;
    private final PaimonColumnValue columnValue = new PaimonColumnValue();
    private List<String> paimonAllFieldNames;
    private List<DataType> paimonDataTypeList;
    private RecordReader.RecordIterator<InternalRow> recordIterator = null;
    private final ClassLoader classLoader;
    private PreExecutionAuthenticator preExecutionAuthenticator;
    private boolean scannerCounted;
    private long openTimeNanos;
    private long readBatchTimeNanos;
    private long readBatchCalls;
    private long emptyReadBatchCalls;
    private long rowsRead;

    public PaimonJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        this.params = params;
        boolean encodedSchema = usesEncodedSchema(params);
        String[] requiredFields = requiredFields(params);
        String[] requiredTypes = requiredTypes(params);
        Preconditions.checkArgument(requiredFields.length == requiredTypes.length,
                "required_fields size %s does not match columns_types size %s",
                requiredFields.length, requiredTypes.length);
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = encodedSchema
                    ? ColumnType.parseTypeWithEncodedStructFields(requiredFields[i], requiredTypes[i])
                    : ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        if (LOG.isDebugEnabled()) {
            // The raw map may carry storage or JDBC credentials; diagnostics must only use
            // values derived from a fixed non-sensitive whitelist.
            LOG.debug(buildDebugSummary(batchSize, requiredFields.length));
        }
        paimonSplit = params.get("paimon_split");
        paimonPredicate = params.get("paimon_predicate");
        tableCacheKey = params.get("serialized_table_cache_key");
        Preconditions.checkState(tableCacheKey != null && !tableCacheKey.isEmpty(),
                "Missing required Paimon scanner parameter: serialized_table_cache_key");
        String timeZone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        columnValue.setTimeZone(timeZone);
        initTableInfo(columnTypes, requiredFields, batchSize);
        hadoopOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(HADOOP_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(HADOOP_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopOptionParams);
    }

    @Override
    public void open() throws IOException {
        markScannerOpenedForMetrics();
        long startTime = System.nanoTime();
        try {
            // When the user does not specify hive-site.xml, Paimon will look for the file from the classpath:
            //    org.apache.paimon.hive.HiveCatalog.createHiveConf:
            //        `Thread.currentThread().getContextClassLoader().getResource(HIVE_SITE_FILE)`
            // so we need to provide a classloader, otherwise it will cause NPE.
            Thread.currentThread().setContextClassLoader(classLoader);
            preExecutionAuthenticator.execute(() -> {
                PaimonJdbcDriverUtils.registerDriverIfNeeded(params, classLoader);
                initTableAndReader();
                return null;
            });
            resetDatetimeV2Precision();

        } catch (Throwable e) {
            try {
                close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            LOG.warn("Failed to open paimon_scanner: " + e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            openTimeNanos += System.nanoTime() - startTime;
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
        readBuilder.withFilter(getPredicates());
        reader = newReadWithOptionalIOManager(readBuilder).executeFilter().createReader(getSplit());
        paimonDataTypeList =
                Arrays.stream(projected).mapToObj(i -> table.rowType().getTypeAt(i)).collect(Collectors.toList());
    }

    private TableRead newReadWithOptionalIOManager(ReadBuilder readBuilder) throws IOException {
        TableRead tableRead = readBuilder.newRead();
        if (!isIOManagerEnabled(params)) {
            return tableRead;
        }
        ioManagerTempDirs = getIOManagerTempDirs(params);
        ioManager = createIOManager(ioManagerTempDirs, getIOManagerImplClass(params));
        LOG.info("Enable Paimon JNI IOManager with temp dirs: {}, implementation: {}",
                ioManagerTempDirs, ioManager.getClass().getName());
        return tableRead.withIOManager(ioManager);
    }

    static boolean isIOManagerEnabled(Map<String, String> params) {
        return Boolean.parseBoolean(params.getOrDefault(ENABLE_JNI_IO_MANAGER, "false"));
    }

    static String getIOManagerTempDirs(Map<String, String> params) throws IOException {
        String tempDirs = params.get(JNI_IO_MANAGER_TMP_DIR);
        if (tempDirs == null || tempDirs.trim().isEmpty()) {
            throw new IOException("Paimon JNI IOManager is enabled but " + JNI_IO_MANAGER_TMP_DIR + " is not set");
        }
        return tempDirs.trim();
    }

    static String getIOManagerImplClass(Map<String, String> params) {
        String implClass = params.get(JNI_IO_MANAGER_IMPL_CLASS);
        return implClass == null || implClass.trim().isEmpty() ? null : implClass.trim();
    }

    static IOManager createIOManager(String tempDirs) throws IOException {
        return createIOManager(tempDirs, null);
    }

    static IOManager createIOManager(String tempDirs, String implClassName) throws IOException {
        String[] splitDirs = IOManagerImpl.splitPaths(tempDirs);
        if (splitDirs.length == 0) {
            throw new IOException("Paimon JNI IOManager temp dirs are empty");
        }
        for (String splitDir : splitDirs) {
            Files.createDirectories(Paths.get(splitDir));
        }
        if (implClassName == null) {
            return IOManager.create(splitDirs);
        }
        return createCustomIOManager(implClassName, splitDirs, tempDirs);
    }

    private static IOManager createCustomIOManager(String implClassName, String[] splitDirs, String tempDirs)
            throws IOException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = PaimonJniScanner.class.getClassLoader();
        }
        try {
            Class<?> implClass = Class.forName(implClassName, true, loader);
            if (!IOManager.class.isAssignableFrom(implClass)) {
                throw new IOException("Paimon JNI IOManager implementation " + implClassName
                        + " does not implement " + IOManager.class.getName());
            }
            return (IOManager) instantiateCustomIOManager(implClass, splitDirs, tempDirs);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to find Paimon JNI IOManager implementation: " + implClassName, e);
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to create Paimon JNI IOManager implementation: " + implClassName, e);
        }
    }

    private static Object instantiateCustomIOManager(Class<?> implClass, String[] splitDirs, String tempDirs)
            throws ReflectiveOperationException {
        try {
            Constructor<?> constructor = implClass.getConstructor(String[].class);
            return constructor.newInstance((Object) splitDirs);
        } catch (NoSuchMethodException e) {
            try {
                Constructor<?> constructor = implClass.getConstructor(String.class);
                return constructor.newInstance(tempDirs);
            } catch (NoSuchMethodException stringConstructorMissing) {
                Constructor<?> constructor = implClass.getConstructor();
                return constructor.newInstance();
            }
        } catch (InvocationTargetException e) {
            throw e;
        }
    }

    private int[] getProjected() {
        return Arrays.stream(fields).mapToInt(fieldName -> {
            int index = getFieldIndex(paimonAllFieldNames, fieldName);
            Preconditions.checkArgument(index >= 0, "RequiredField %s not found in schema", fieldName);
            return index;
        }).toArray();
    }

    static int getFieldIndex(List<String> fieldNames, String fieldName) {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldNames.get(i).equalsIgnoreCase(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private List<Predicate> getPredicates() {
        // Backstop for a missing paimon_predicate param (scan with no pushed-down filter): a null here means
        // "no filter", not an error. Guard the unconditional deserialize so the JNI reader never NPEs on
        // deserialize(null) ("encodedStr is null"). The FE producer also always emits an (empty) predicate now.
        if (paimonPredicate == null) {
            return Collections.emptyList();
        }
        List<Predicate> predicates = PaimonUtils.deserialize(paimonPredicate);
        if (LOG.isDebugEnabled()) {
            LOG.debug("predicates:{}", predicates);
        }
        return predicates;
    }

    private Split getSplit() {
        Split split = PaimonUtils.deserialize(paimonSplit);
        if (LOG.isDebugEnabled()) {
            LOG.debug("split:{}", split);
        }
        return split;
    }

    private void resetDatetimeV2Precision() {
        for (int i = 0; i < types.length; i++) {
            if (types[i].isDateTimeV2()) {
                // paimon support precision > 6, but it has been reset as 6 in FE
                // try to get the right precision for datetimev2
                int index = getFieldIndex(paimonAllFieldNames, fields[i]);
                if (index != -1) {
                    DataType dataType = table.rowType().getTypeAt(index);
                    if (dataType instanceof TimestampType) {
                        types[i].setPrecision(((TimestampType) dataType).getPrecision());
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        try {
            try {
                releaseRecordIterator();
            } catch (RuntimeException e) {
                exception = new IOException("Failed to release Paimon record iterator", e);
            }
            if (reader != null) {
                try {
                    reader.close();
                    reader = null;
                } catch (IOException e) {
                    if (exception == null) {
                        exception = e;
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }
            if (ioManager != null) {
                try {
                    ioManager.close();
                    ioManager = null;
                } catch (Exception e) {
                    LOG.warn("Failed to close Paimon JNI IOManager, temp dirs: {}", ioManagerTempDirs, e);
                    if (exception == null) {
                        exception = new IOException(e);
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }
        } finally {
            releaseCachedTable();
            markScannerClosedForMetrics();
        }
        if (exception != null) {
            throw exception;
        }
    }

    private void releaseRecordIterator() {
        if (recordIterator != null) {
            recordIterator.releaseBatch();
            recordIterator = null;
        }
    }

    private int readAndProcessNextBatch() throws IOException {
        int rows = 0;
        try {
            if (recordIterator == null) {
                recordIterator = readBatchWithMetrics();
            }

            while (recordIterator != null) {
                long startTime = System.nanoTime();

                InternalRow record;
                while ((record = recordIterator.next()) != null) {
                    rows++;
                    columnValue.setOffsetRow(record);
                    for (int i = 0; i < fields.length; i++) {
                        columnValue.setIdx(i, types[i], paimonDataTypeList.get(i));
                        appendData(i, columnValue);
                    }
                    if (rows >= batchSize) {
                        if (fields.length == 0) {
                            vectorTable.appendVirtualData(rows);
                        }
                        appendDataTime += System.nanoTime() - startTime;
                        rowsRead += rows;
                        return rows;
                    }
                }
                appendDataTime += System.nanoTime() - startTime;

                releaseRecordIterator();
                recordIterator = readBatchWithMetrics();
            }
            if (fields.length == 0 && rows > 0) {
                vectorTable.appendVirtualData(rows);
            }
            rowsRead += rows;
        } catch (Exception e) {
            close();
            LOG.warn("Failed to get the next batch of paimon. "
                            + "split: {}, requiredFieldNames: {}, paimonAllFieldNames: {}, dataType: {}",
                    getSplit(), params.get("required_fields"), paimonAllFieldNames, paimonDataTypeList, e);
            throw new IOException(e);
        }
        return rows;
    }

    private RecordReader.RecordIterator<InternalRow> readBatchWithMetrics() throws IOException {
        long startTime = System.nanoTime();
        try {
            RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
            if (iterator == null) {
                emptyReadBatchCalls++;
            }
            return iterator;
        } finally {
            readBatchCalls++;
            readBatchTimeNanos += System.nanoTime() - startTime;
        }
    }

    @Override
    protected int getNext() {
        try {
            return preExecutionAuthenticator.execute(this::readAndProcessNextBatch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> statistics = new HashMap<>();
        statistics.put("gauge:PaimonJniIOManagerEnabled", ioManager != null ? "1" : "0");
        statistics.put("gauge:PaimonJniActiveScannerCount", String.valueOf(ACTIVE_SCANNERS.get()));
        statistics.put("gauge:PaimonJniAsyncReaderThreadCount",
                String.valueOf(currentAsyncReaderThreadCount()));
        statistics.put("gauge:PaimonJniRequiredFieldCount", String.valueOf(fields.length));
        statistics.put("counter:PaimonJniSplitEncodedLength", String.valueOf(lengthOfParam("paimon_split")));
        statistics.put("counter:PaimonJniPredicateEncodedLength", String.valueOf(lengthOfParam("paimon_predicate")));
        statistics.put("gauge:PaimonJniAsyncThresholdConfigured",
                hasPaimonOption(FILE_READER_ASYNC_THRESHOLD) ? "1" : "0");
        parseDataSizeBytes(paimonOption(FILE_READER_ASYNC_THRESHOLD)).ifPresent(
                bytes -> statistics.put("bytes_gauge:PaimonJniAsyncThresholdBytes", String.valueOf(bytes)));
        statistics.put("counter:PaimonJniReadBatchCalls", String.valueOf(readBatchCalls));
        statistics.put("counter:PaimonJniEmptyReadBatchCalls", String.valueOf(emptyReadBatchCalls));
        statistics.put("counter:PaimonJniRowsRead", String.valueOf(rowsRead));
        statistics.put("timer:PaimonJniScannerOpenTime", String.valueOf(openTimeNanos));
        statistics.put("timer:PaimonJniReadBatchTime", String.valueOf(readBatchTimeNanos));
        putMemoryStatistics(statistics);
        return statistics;
    }

    private int lengthOfParam(String key) {
        String value = params.get(key);
        return value == null ? 0 : value.length();
    }

    private boolean hasPaimonOption(String key) {
        return paimonOption(key) != null;
    }

    private String paimonOption(String key) {
        if (table != null) {
            String tableOption = table.options().get(key);
            if (tableOption != null) {
                return tableOption;
            }
        }
        return params.get(PAIMON_OPTION_PREFIX + key);
    }

    private static void putMemoryStatistics(Map<String, String> statistics) {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();
        statistics.put("bytes_gauge:PaimonJniJvmHeapUsed", String.valueOf(nonNegative(heapUsage.getUsed())));
        statistics.put("bytes_gauge:PaimonJniJvmHeapCommitted", String.valueOf(nonNegative(heapUsage.getCommitted())));
        statistics.put("bytes_gauge:PaimonJniJvmHeapMax", String.valueOf(nonNegative(heapUsage.getMax())));
        statistics.put("bytes_gauge:PaimonJniJvmNonHeapUsed", String.valueOf(nonNegative(nonHeapUsage.getUsed())));
        statistics.put("bytes_gauge:PaimonJniJvmNonHeapCommitted",
                String.valueOf(nonNegative(nonHeapUsage.getCommitted())));
        statistics.put("bytes_gauge:PaimonJniJvmNonHeapMax", String.valueOf(nonNegative(nonHeapUsage.getMax())));
    }

    private static long nonNegative(long value) {
        return Math.max(value, 0L);
    }

    private static int currentAsyncReaderThreadCount() {
        return ASYNC_READER_THREAD_COUNTER.get();
    }

    static String buildDebugSummary(int batchSize, int requiredFieldCount) {
        return "Paimon JNI scanner configuration: batchSize=" + batchSize
                + ", requiredFieldCount=" + requiredFieldCount;
    }

    static String[] requiredFields(Map<String, String> params) {
        String encodedFields = params.get("required_fields_base64");
        if (encodedFields == null) {
            return splitParam(params.get("required_fields"), ",");
        }
        if (encodedFields.isEmpty()) {
            return new String[0];
        }
        // Each identifier is encoded independently, so delimiters in quoted identifiers cannot
        // change field cardinality. The legacy parameter remains the rolling-upgrade fallback.
        return decodeSchemaValues(encodedFields);
    }

    static String[] requiredTypes(Map<String, String> params) {
        String encodedTypes = params.get("columns_types_base64");
        return encodedTypes == null
                ? splitParam(params.get("columns_types"), "#")
                : decodeSchemaValues(encodedTypes);
    }

    private static boolean usesEncodedSchema(Map<String, String> params) {
        boolean hasFields = params.containsKey("required_fields_base64");
        boolean hasTypes = params.containsKey("columns_types_base64");
        // Both halves describe one schema version; accepting a mixed pair would reintroduce the
        // cardinality and nested-name ambiguity this protocol is intended to remove.
        Preconditions.checkArgument(hasFields == hasTypes,
                "required_fields_base64 and columns_types_base64 must be provided together");
        return hasFields;
    }

    private static String[] decodeSchemaValues(String encodedValues) {
        if (encodedValues.isEmpty()) {
            return new String[0];
        }
        return Arrays.stream(encodedValues.split(",", -1))
                .map(encoded -> {
                    // A marker on every token preserves list arity when the encoded value itself is empty.
                    Preconditions.checkArgument(encoded.startsWith("$"),
                            "Encoded JNI schema token is missing its version marker");
                    return new String(Base64.getDecoder().decode(encoded.substring(1)), StandardCharsets.UTF_8);
                })
                .toArray(String[]::new);
    }

    static int countThreadsByNamePrefix(String threadNamePrefix) {
        int count = 0;
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 0);
        for (ThreadInfo threadInfo : threadInfos) {
            if (threadInfo != null && threadInfo.getThreadName().startsWith(threadNamePrefix)) {
                count++;
            }
        }
        return count;
    }

    static final class CachedThreadCounter {
        private final long ttlNanos;
        private final LongSupplier ticker;
        private final IntSupplier sampler;
        private volatile long lastSampleNanos = Long.MIN_VALUE;
        private volatile int cachedValue;

        CachedThreadCounter(long ttlNanos, LongSupplier ticker, IntSupplier sampler) {
            this.ttlNanos = ttlNanos;
            this.ticker = ticker;
            this.sampler = sampler;
        }

        int get() {
            long now = ticker.getAsLong();
            long sampledAt = lastSampleNanos;
            if (sampledAt != Long.MIN_VALUE && now - sampledAt < ttlNanos) {
                return cachedValue;
            }
            synchronized (this) {
                now = ticker.getAsLong();
                if (lastSampleNanos == Long.MIN_VALUE || now - lastSampleNanos >= ttlNanos) {
                    cachedValue = sampler.getAsInt();
                    lastSampleNanos = now;
                }
                return cachedValue;
            }
        }
    }

    private void markScannerOpenedForMetrics() {
        if (!scannerCounted) {
            scannerCounted = true;
            ACTIVE_SCANNERS.incrementAndGet();
        }
    }

    private void markScannerClosedForMetrics() {
        if (scannerCounted) {
            scannerCounted = false;
            ACTIVE_SCANNERS.decrementAndGet();
        }
    }

    static Optional<Long> parseDataSizeBytes(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Optional.empty();
        }
        try {
            // Keep the BE guard's accepted grammar identical to the Paimon option parser that will
            // consume this value; accepting a superset lets invalid serialized options reach scans.
            return Optional.of(MemorySize.parse(value).getBytes());
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private void initTable() {
        Preconditions.checkState(params.containsKey(SERIALIZED_TABLE));
        table = PaimonUtils.deserialize(params.get(SERIALIZED_TABLE));
        params.remove(SERIALIZED_TABLE);
        String encodedSystemSource = params.get(SERIALIZED_SYSTEM_SOURCE);
        FileStoreTable systemSource = encodedSystemSource == null
                ? null : PaimonUtils.deserialize(encodedSystemSource);
        params.remove(SERIALIZED_SYSTEM_SOURCE);
        table = applyBackendManifestParallelism(table,
                params.get(PAIMON_OPTION_PREFIX + DORIS_MANIFEST_PARALLELISM_CAP),
                Runtime.getRuntime().availableProcessors(), systemSource,
                params.get(PAIMON_OPTION_PREFIX + DORIS_SYSTEM_TABLE_TYPE));
        validateSerializedReaderOptions(table);
        paimonAllFieldNames = PaimonUtils.getFieldNames(this.table.rowType());
        if (LOG.isDebugEnabled()) {
            LOG.debug("paimonAllFieldNames:{}", paimonAllFieldNames);
        }
    }

    static Table applyBackendManifestParallelism(
            Table table, String feParallelismCap, int localCapacity) {
        return applyBackendManifestParallelism(
                table, feParallelismCap, localCapacity, null, null);
    }

    static Table applyBackendManifestParallelism(
            Table table, String feParallelismCap, int localCapacity,
            FileStoreTable systemSource, String systemTableType) {
        // Old FEs do not send a cap, so the BE must still preserve the hardware-independent
        // ceiling that prevents one scan from growing Paimon's JVM-global executor beyond 256.
        int requestedBound = Math.min(localCapacity, MAX_MANIFEST_PARALLELISM);
        if (feParallelismCap != null) {
            requestedBound = Math.min(parsePositiveManifestParallelism(feParallelismCap), requestedBound);
        }
        final int safeBound = requestedBound;
        boolean materializeAbsent = localCapacity > safeBound;
        if (systemSource != null && systemTableType != null) {
            FileStoreTable cappedSource =
                    applyManifestParallelismBound(systemSource, safeBound, materializeAbsent);
            FileStoreTable planningSource = unwrapSystemPlanningSource(cappedSource);
            // Read-only wrappers hide the data table's option map. Rebuild from the transported
            // exact source so a smaller BE can lower that hidden planner without rewinding schema.
            Table rebuilt = SystemTableLoader.load(systemTableType, planningSource);
            if (rebuilt == null) {
                throw new IllegalArgumentException(
                        "Unsupported Paimon system table '" + systemTableType + "'");
            }
            return rebuilt;
        }
        return applyManifestParallelismBound(table, safeBound, materializeAbsent);
    }

    private static Table applyManifestParallelismBound(
            Table table, int safeBound, boolean materializeAbsent) {
        if (table instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) table;
            FileStoreTable main = applyManifestParallelismBound(
                    pair.wrapped(), safeBound, materializeAbsent);
            FileStoreTable fallback = applyManifestParallelismBound(
                    pair.fallback(), safeBound, materializeAbsent);
            if (main == pair.wrapped() && fallback == pair.fallback()) {
                return table;
            }
            // Each branch owns an independent planner setting; a smaller sibling is not an
            // execution ceiling and must never throttle the other branch.
            return new FallbackReadFileStoreTable(main, fallback);
        }

        if (table instanceof DelegatedFileStoreTable) {
            FileStoreTable wrapped = ((DelegatedFileStoreTable) table).wrapped();
            FileStoreTable normalized = applyManifestParallelismBound(
                    wrapped, safeBound, materializeAbsent);
            if (normalized == wrapped) {
                return table;
            }
            // FE planning already enforced privilege delegates. Keeping one here would hide the
            // fallback tree and force a single copied cap onto branches with independent values.
            return normalized;
        }

        Optional<FileStoreTable> hiddenSource = hiddenSystemSource(table);
        if (hiddenSource.isPresent()) {
            FileStoreTable normalized = applyManifestParallelismBound(
                    hiddenSource.get(), safeBound, materializeAbsent);
            FileStoreTable planningSource = unwrapSystemPlanningSource(normalized);
            return planningSource == hiddenSource.get()
                    ? table : rebuildHiddenSystemTable(table, planningSource);
        }

        if (!(table instanceof FileStoreTable)) {
            return table;
        }

        String configured = table.options().get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key());
        if (configured == null && !materializeAbsent) {
            return table;
        }
        if (configured != null && parsePositiveManifestParallelism(configured) <= safeBound) {
            return table;
        }
        // An absent option inherits Paimon's processor-count default, so materialize the stable
        // bound instead of assuming that an empty option map is already safe on large hosts.
        Map<String, String> cap = Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), String.valueOf(safeBound));
        return ((FileStoreTable) table).copyWithoutTimeTravel(cap);
    }

    private static Table rebuildHiddenSystemTable(Table wrapper, FileStoreTable normalizedSource) {
        try {
            // Old FE payloads have no type/source side channel. Every Paimon 1.3 system wrapper
            // owns a FileStoreTable constructor, which preserves the exact wrapper without a
            // broadcast copy that would flatten fallback branch preferences.
            Constructor<?> constructor = wrapper.getClass().getDeclaredConstructor(FileStoreTable.class);
            constructor.setAccessible(true);
            return (Table) constructor.newInstance(normalizedSource);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new IllegalArgumentException(
                    "Unable to rebuild serialized Paimon system table "
                            + wrapper.getClass().getName(), e);
        }
    }

    private static FileStoreTable applyManifestParallelismBound(
            FileStoreTable table, int safeBound, boolean materializeAbsent) {
        return (FileStoreTable) applyManifestParallelismBound(
                (Table) table, safeBound, materializeAbsent);
    }

    private static FileStoreTable unwrapSystemPlanningSource(FileStoreTable table) {
        FileStoreTable current = table;
        // System wrappers dispatch fallback reads only when the fallback pair is their direct
        // source; FE authorization has already run, so privilege-only delegates must not hide it.
        while (current instanceof DelegatedFileStoreTable
                && !(current instanceof FallbackReadFileStoreTable)) {
            current = ((DelegatedFileStoreTable) current).wrapped();
        }
        return current;
    }

    private static int parsePositiveManifestParallelism(String value) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 1) {
                throw new IllegalArgumentException("Paimon manifest parallelism cap must be positive.");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Paimon manifest parallelism cap must be an integer.", e);
        }
    }

    private static void validateSerializedReaderOptions(Table table) {
        validateSerializedReadBatchSize(table.options().get(CoreOptions.READ_BATCH_SIZE.key()));
        validateSerializedAsyncThreshold(table.options().get(CoreOptions.FILE_READER_ASYNC_THRESHOLD.key()));
        validateSerializedSplitTargetSize(table.options().get(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key()));
        if (table instanceof FallbackReadFileStoreTable) {
            validateSerializedReaderOptions(((FallbackReadFileStoreTable) table).fallback());
        }
        if (table instanceof DelegatedFileStoreTable) {
            validateSerializedReaderOptions(((DelegatedFileStoreTable) table).wrapped());
            return;
        }
        hiddenSystemSource(table).ifPresent(PaimonJniScanner::validateSerializedReaderOptions);
    }

    private static Optional<FileStoreTable> hiddenSystemSource(Table table) {
        for (Class<?> type = table.getClass(); type != null; type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                if (!FileStoreTable.class.isAssignableFrom(field.getType())) {
                    continue;
                }
                try {
                    // Old FE system-table payloads carry no source side channel, while Paimon
                    // intentionally hides the planner table behind a private wrapper field.
                    field.setAccessible(true);
                    return Optional.ofNullable((FileStoreTable) field.get(table));
                } catch (IllegalAccessException | RuntimeException e) {
                    throw new IllegalArgumentException(
                            "Unable to validate the serialized Paimon system table source", e);
                }
            }
        }
        return Optional.empty();
    }

    private static void validateSerializedSplitTargetSize(String value) {
        if (value == null) {
            return;
        }
        Optional<Long> bytes = parseDataSizeBytes(value);
        // Old FE payloads bypass the allowlist validator; zero disables bin packing and can turn
        // one deferred system-table plan into one split per data file.
        if (!bytes.isPresent() || bytes.get() < 1L) {
            throw new IllegalArgumentException("Paimon option '"
                    + CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key()
                    + "' must be positive, but was " + value);
        }
    }

    private static void validateSerializedAsyncThreshold(String value) {
        if (value == null) {
            return;
        }
        Optional<Long> bytes = parseDataSizeBytes(value);
        // A serialized table can come from an older FE, so enforce the reader-allocation contract
        // again before an unsafe threshold reaches Paimon's asynchronous read path.
        if (!bytes.isPresent() || bytes.get() < 1024L * 1024L
                || bytes.get() > 1024L * 1024L * 1024L) {
            throw new IllegalArgumentException("Paimon option '"
                    + CoreOptions.FILE_READER_ASYNC_THRESHOLD.key()
                    + "' must be between 1 MB and 1 GB, but was " + value);
        }
    }

    private static void validateSerializedReadBatchSize(String value) {
        if (value == null) {
            return;
        }
        try {
            int batchSize = Integer.parseInt(value);
            if (batchSize < 1 || batchSize > 65536) {
                throw new IllegalArgumentException("Paimon option '" + CoreOptions.READ_BATCH_SIZE.key()
                        + "' must be between 1 and 65536, but was " + value);
            }
        } catch (NumberFormatException e) {
            // Serialized tables can come from an older FE that did not validate reader options;
            // fail fast instead of allowing an invalid batch size to reach Paimon's read loop.
            throw new IllegalArgumentException("Paimon option '" + CoreOptions.READ_BATCH_SIZE.key()
                    + "' must be an integer between 1 and 65536, but was " + value, e);
        }
    }

    private boolean initTableFromCache() {
        PaimonTableCache.TableCacheEntry cachedEntry = PaimonTableCache.acquire(tableCacheKey);
        if (cachedEntry == null) {
            return false;
        }
        tableCacheEntry = cachedEntry;
        table = cachedEntry.table();
        paimonAllFieldNames = cachedEntry.fieldNames();
        params.remove(SERIALIZED_TABLE);
        params.remove(SERIALIZED_SYSTEM_SOURCE);
        return true;
    }

    private void initTableAndReader() throws IOException {
        if (initTableFromCache()) {
            initReader();
            return;
        }
        initTable();
        initReader();
        PaimonTableCache.TableCacheEntry candidate =
                new PaimonTableCache.TableCacheEntry(table, paimonAllFieldNames);
        if (PaimonTableCache.publish(tableCacheKey, candidate)) {
            tableCacheEntry = candidate;
        }
    }

    private void releaseCachedTable() {
        if (tableCacheEntry != null) {
            PaimonTableCache.release(tableCacheKey, tableCacheEntry);
            tableCacheEntry = null;
        }
    }

    private static String[] splitParam(String value, String delimiter) {
        if (value == null || value.isEmpty()) {
            return new String[0];
        }
        return value.split(delimiter);
    }
}
