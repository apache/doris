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
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;

import com.google.common.base.Preconditions;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.FilesTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PaimonJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonJniScanner.class);
    private static final String HADOOP_OPTION_PREFIX = "hadoop.";
    private static final String PAIMON_OPTION_PREFIX = "paimon.";
    private static final String ASYNC_READER_THREAD_NAME_PREFIX = "paimon-reader-async-thread";
    private static final String FILE_READER_ASYNC_THRESHOLD = "file-reader-async-threshold";
    static final String ENABLE_JNI_IO_MANAGER = "paimon.doris.enable_jni_io_manager";
    static final String JNI_IO_MANAGER_TMP_DIR = "paimon.doris.jni_io_manager.tmp_dir";
    static final String JNI_IO_MANAGER_IMPL_CLASS = "paimon.doris.jni_io_manager.impl_class";
    private static final AtomicInteger ACTIVE_SCANNERS = new AtomicInteger();
    static final String DORIS_ENABLE_FILE_READER_ASYNC = "paimon.jni.enable_file_reader_async";
    static final String DORIS_FILE_CREATION_TIME_LOCAL_MILLIS =
            "doris.scan.file-creation-time-local-millis";
    static final String MAX_ASYNC_READ_THRESHOLD = Long.MAX_VALUE + "b"; // max threshold means disable

    private final Map<String, String> params;
    private final Map<String, String> hadoopOptionParams;
    private final String paimonSplit;
    private final String paimonPredicate;
    private Table table;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("params:{}", params);
        }
        this.params = params;
        String[] requiredFields = splitParam(params.get("required_fields"), ",");
        String[] requiredTypes = splitParam(params.get("columns_types"), "#");
        Preconditions.checkArgument(requiredFields.length == requiredTypes.length,
                "required_fields size %s does not match columns_types size %s",
                requiredFields.length, requiredTypes.length);
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        paimonSplit = params.get("paimon_split");
        paimonPredicate = params.get("paimon_predicate");
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
                initTable();
                initReader();
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
        return countThreadsByNamePrefix(ASYNC_READER_THREAD_NAME_PREFIX);
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
        String normalized = value.trim().toLowerCase(Locale.ROOT).replace("_", "").replace(" ", "");
        int unitStart = 0;
        while (unitStart < normalized.length()
                && (Character.isDigit(normalized.charAt(unitStart)) || normalized.charAt(unitStart) == '.')) {
            unitStart++;
        }
        if (unitStart == 0) {
            return Optional.empty();
        }
        try {
            double number = Double.parseDouble(normalized.substring(0, unitStart));
            String unit = normalized.substring(unitStart);
            long multiplier;
            switch (unit) {
                case "":
                case "b":
                case "byte":
                case "bytes":
                    multiplier = 1L;
                    break;
                case "k":
                case "kb":
                case "kib":
                    multiplier = 1024L;
                    break;
                case "m":
                case "mb":
                case "mib":
                    multiplier = 1024L * 1024L;
                    break;
                case "g":
                case "gb":
                case "gib":
                    multiplier = 1024L * 1024L * 1024L;
                    break;
                case "t":
                case "tb":
                case "tib":
                    multiplier = 1024L * 1024L * 1024L * 1024L;
                    break;
                default:
                    return Optional.empty();
            }
            return Optional.of((long) (number * multiplier));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private void initTable() {
        Preconditions.checkState(params.containsKey("serialized_table"));
        table = PaimonUtils.deserialize(params.get("serialized_table"));
        table = applyFileCreationTimeLowerBound(
                table,
                params.get(PAIMON_OPTION_PREFIX + DORIS_FILE_CREATION_TIME_LOCAL_MILLIS),
                ZoneId.systemDefault());
        table = table.copy(buildTableOptions(table.options()));
        paimonAllFieldNames = PaimonUtils.getFieldNames(this.table.rowType());
        if (LOG.isDebugEnabled()) {
            LOG.debug("paimonAllFieldNames:{}", paimonAllFieldNames);
        }
    }

    private static String[] splitParam(String value, String delimiter) {
        if (value == null || value.isEmpty()) {
            return new String[0];
        }
        return value.split(delimiter);
    }

    private Map<String, String> buildTableOptions(Map<String, String> tableOptions) {
        Map<String, String> options = new HashMap<>(tableOptions);
        options.put(CoreOptions.READ_BATCH_SIZE.key(), String.valueOf(batchSize));
        if (Boolean.parseBoolean(params.getOrDefault(DORIS_ENABLE_FILE_READER_ASYNC, "true")) == false) {
            options.put(CoreOptions.FILE_READER_ASYNC_THRESHOLD.key(), MAX_ASYNC_READ_THRESHOLD);
        }
        return options;
    }

    static long toFileCreationTimeEpochMillis(long localMillis, ZoneId zoneId) {
        LocalDateTime localDateTime = Timestamp.fromEpochMillis(localMillis).toLocalDateTime();
        ZoneOffsetTransition transition = zoneId.getRules().getTransition(localDateTime);
        if (transition != null && transition.isGap()) {
            // No file can have a wall-clock time inside a DST gap. Using the transition instant
            // keeps the pushdown no stronger than the retained local-time predicate.
            return transition.getInstant().toEpochMilli();
        }
        // Paimon converts every file creation timestamp with the BE JVM's default zone, so the
        // cutoff must be converted in this same JVM when FE and BE zones differ.
        return localDateTime.atZone(zoneId).toInstant().toEpochMilli();
    }

    static Table applyFileCreationTimeLowerBound(Table table, String localMillis, ZoneId zoneId) {
        if (localMillis == null || !(table instanceof FilesTable)) {
            return table;
        }
        FileStoreTable storeTable = extractStoreTable((FilesTable) table);
        if (storeTable == null || containsFallbackRead(storeTable)
                || !supportsFileCreationTimeStartup(storeTable)) {
            return table;
        }

        long epochMillis = toFileCreationTimeEpochMillis(Long.parseLong(localMillis), zoneId);
        String existingMillis = storeTable.options().get(
                CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key());
        if (existingMillis != null) {
            // Read the cutoff from the exact serialized wrapper so a stale FE metadata handle
            // cannot weaken a restriction already attached to this scan revision.
            epochMillis = Math.max(epochMillis, Long.parseLong(existingMillis));
        }
        return table.copy(Collections.singletonMap(
                CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), String.valueOf(epochMillis)));
    }

    private static FileStoreTable extractStoreTable(FilesTable filesTable) {
        try {
            // Paimon 1.3 does not expose FilesTable's wrapped table; inspecting this exact object
            // avoids mixing independently refreshed catalog metadata into the scan.
            Field field = FilesTable.class.getDeclaredField("storeTable");
            field.setAccessible(true);
            return (FileStoreTable) field.get(filesTable);
        } catch (ReflectiveOperationException | RuntimeException e) {
            LOG.warn("Skip file creation time pushdown because the wrapped Paimon table is unavailable", e);
            return null;
        }
    }

    private static boolean containsFallbackRead(FileStoreTable table) {
        FileStoreTable current = table;
        while (current instanceof DelegatedFileStoreTable) {
            if (current instanceof FallbackReadFileStoreTable) {
                // Copying a cutoff to both branches changes which branch owns a partition.
                return true;
            }
            current = ((DelegatedFileStoreTable) current).wrapped();
        }
        return false;
    }

    private static boolean supportsFileCreationTimeStartup(FileStoreTable table) {
        CoreOptions coreOptions = table.coreOptions();
        CoreOptions.StartupMode configuredMode = coreOptions.toConfiguration().get(CoreOptions.SCAN_MODE);
        CoreOptions.StartupMode effectiveMode = coreOptions.startupMode();
        // Paimon rejects this cutoff when snapshot, timestamp, incremental, or an explicit
        // latest-full startup mode already owns the scan starting point.
        return effectiveMode == CoreOptions.StartupMode.FROM_FILE_CREATION_TIME
                || (configuredMode == CoreOptions.StartupMode.DEFAULT
                        && effectiveMode == CoreOptions.StartupMode.LATEST_FULL);
    }
}
