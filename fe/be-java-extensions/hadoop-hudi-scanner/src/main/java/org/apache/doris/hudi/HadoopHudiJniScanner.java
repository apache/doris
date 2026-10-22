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

package org.apache.doris.hudi;

import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.HadoopKerberosAuthenticator;
import org.apache.doris.kerberos.PreExecutionAuthenticator;
import org.apache.doris.kerberos.PreExecutionAuthenticatorCache;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * HadoopHudiJniScanner is a JniScanner implementation that reads Hudi data using hudi-hadoop-mr.
 */
public class HadoopHudiJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopHudiJniScanner.class);

    private static final String HADOOP_CONF_PREFIX = "hadoop_conf.";

    // fs.s3a.impl.disable.cache and its per-scheme siblings, as the FE emits them.
    private static final Pattern FS_DISABLE_CACHE = Pattern.compile("fs\\..+\\.impl\\.disable\\.cache");

    // Hudi data info
    private final String basePath;
    private final String dataFilePath;
    private final long dataFileLength;
    private final String[] deltaFilePaths;
    private final String instantTime;
    private final String serde;
    private final String inputFormat;

    // schema info
    private final String hudiColumnNames;
    private final String[] hudiColumnTypes;
    private final String[] requiredFields;
    private List<Integer> requiredColumnIds;
    private ColumnType[] requiredTypes;

    // Hadoop info
    private RecordReader<NullWritable, ArrayWritable> reader;
    private StructObjectInspector rowInspector;
    private final ObjectInspector[] fieldInspectors;
    private final StructField[] structFields;
    private Deserializer deserializer;
    private final Map<String, String> fsOptionsProps;

    // scanner info
    private final HadoopHudiColumnValue columnValue;
    private final int fetchSize;

    private final PreExecutionAuthenticator preExecutionAuthenticator;
    // The configuration this scanner reads under, digested, and the user name its scope carries;
    // both null when the scanner does not own its filesystems. See fileSystemScopeKey.
    private final String fileSystemScopeKey;
    private final String fileSystemScopeUser;
    // Held from open to close; null outside that window. See HudiFileSystemScopes.
    private HudiFileSystemScopes.Hold fileSystemScope;

    public HadoopHudiJniScanner(int fetchSize, Map<String, String> params) {
        this.basePath = params.get("base_path");
        this.dataFilePath = params.get("data_file_path");
        this.dataFileLength = Long.parseLong(params.get("data_file_length"));
        if (Strings.isNullOrEmpty(params.get("delta_file_paths"))) {
            this.deltaFilePaths = new String[0];
        } else {
            this.deltaFilePaths = params.get("delta_file_paths").split(",");
        }
        this.instantTime = params.get("instant_time");
        this.serde = params.get("serde");
        this.inputFormat = params.get("input_format");

        this.hudiColumnNames = params.get("hudi_column_names");
        this.hudiColumnTypes = params.get("hudi_column_types").split("#");
        // Required fields will be empty when only partition fields are selected
        // This is because partition fields are not stored in the data files
        if (!params.get("required_fields").equals("")) {
            this.requiredFields = params.get("required_fields").split(",");
        } else {
            this.requiredFields = new String[0];
        }
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.fsOptionsProps = Maps.newHashMap();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(HADOOP_CONF_PREFIX)) {
                fsOptionsProps.put(entry.getKey().substring(HADOOP_CONF_PREFIX.length()), entry.getValue());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get hudi params {}: {}", entry.getKey(), entry.getValue());
            }
        }
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(fsOptionsProps);
        this.fileSystemScopeUser = fileSystemScopeUser();
        this.fileSystemScopeKey = fileSystemScopeUser == null ? null : fileSystemScopeKey(fsOptionsProps);
        if (fileSystemScopeKey != null) {
            // Hadoop's FileSystem cache is the only thing standing between this scanner and a
            // filesystem leaked on every get(); the scope acquired in openInternal is what makes the
            // cached filesystems closable again.
            fsOptionsProps.replaceAll((key, value) -> FS_DISABLE_CACHE.matcher(key).matches() ? "false" : value);
        }

        ZoneId zoneId;
        if (Strings.isNullOrEmpty(params.get("time_zone"))) {
            zoneId = ZoneId.systemDefault();
        } else {
            zoneId = ZoneId.of(params.get("time_zone"));
        }
        // Hudi keeps one session zone for every timestamp encoding to preserve its JNI contract.
        this.columnValue = new HadoopHudiColumnValue(zoneId);
        this.fetchSize = fetchSize;
    }

    /**
     * The user name of the {@code UserGroupInformation} this scanner reads under, or null when it
     * reads under the authenticator's own.
     *
     * <p>Hadoop's {@link FileSystem} cache stays ON in this scanner, and that is a lifecycle decision
     * before it is anything else. The FE used to hand every S3-compatible catalog
     * {@code fs.s3a.impl.disable.cache=true}, and with the cache off every {@code FileSystem.get()}
     * inside hudi-hadoop-mr built a fresh S3AFileSystem and a fresh AWS SDK client that nobody closed:
     * one query reaches for the base file, each log file and the timeline through some two dozen of
     * those calls, and each SDK client leaves a scheduled executor its own worker threads keep alive
     * - about 118 threads per query, measured - until the JVM cannot start a thread and the BE aborts
     * with {@code std::system_error: thread constructor failed}.
     *
     * <p>With the cache on, what keeps two catalogs from sharing one filesystem is the cache key. This
     * plugin carries the Doris-patched {@code FileSystem} (hadoop-deps), whose key includes the
     * {@code doris.fs.cache.key.<scheme>} fingerprint FE derives from each catalog's storage
     * configuration - so same credentials reuse a filesystem and different ones cannot, whichever UGI
     * the scan runs under. What the per-configuration UGI adds is a handle to CLOSE with: the cached
     * filesystems are keyed on it, {@code FileSystem.closeAllForUGI} is the only way Hadoop offers to
     * let a set of them go, and {@link HudiFileSystemScopes} closes each configuration's set once no
     * scanner has read through it for a while. The UGI carries the SAME user name the simple
     * authenticator would have used - it is a second Subject for one user, not another user - so an
     * {@code hdfs://} warehouse still sees the identity it always saw.
     *
     * <p>Kerberos is the case this cannot serve: {@code createRemoteUser} would drop the credentials
     * the ticket carries. There the scanner keeps the authenticator's own UGI, which is cached per
     * principal, and the FE's cache setting is left alone; this returns null.
     */
    private String fileSystemScopeUser() {
        HadoopAuthenticator authenticator = preExecutionAuthenticator.getHadoopAuthenticator();
        if (authenticator == null || authenticator instanceof HadoopKerberosAuthenticator) {
            return null;
        }
        try {
            return authenticator.getUGI().getUserName();
        } catch (Exception e) {
            LOG.warn("failed to derive a FileSystem scope for the hudi scanner, keeping the shared one", e);
            return null;
        }
    }

    /**
     * Identifies a filesystem configuration - endpoint, credentials, everything the FE sent - so that
     * {@link HudiFileSystemScopes} hands the same UGI to two scanners exactly when they may share a
     * filesystem. Digested rather than used directly because the properties hold secrets and a map key
     * is easy to print by accident. Null when the digest cannot be computed, in which case the scanner
     * reads under the authenticator's UGI like a Kerberos one. Package-private so a test can name the
     * scope a scanner built from a given configuration holds.
     */
    static String fileSystemScopeKey(Map<String, String> fsOptionsProps) {
        StringBuilder canonical = new StringBuilder();
        new TreeMap<>(fsOptionsProps).forEach((k, v) -> canonical.append(k).append('=').append(v).append('\n'));
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("failed to derive a FileSystem scope for the hudi scanner, keeping the shared one", e);
            return null;
        }
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }

    /**
     * Runs {@code task} under {@link #fileSystemScope}. That replaces
     * {@code PreExecutionAuthenticator}'s doAs rather than nesting inside it, because nesting would not
     * work: the simple authenticator's own doAs would put its shared UGI back and the filesystem cache
     * would collapse onto one entry again. The two are equivalent otherwise - both run the task under a
     * {@code createRemoteUser} of the same name, and the simple authenticator adds nothing else.
     */
    private <T> T executeInFileSystemScope(Callable<T> task) throws Exception {
        HudiFileSystemScopes.Hold hold = fileSystemScope;
        if (hold == null) {
            return preExecutionAuthenticator.execute(task);
        }
        try {
            return hold.ugi().doAs((PrivilegedExceptionAction<T>) task::call);
        } catch (UndeclaredThrowableException e) {
            // doAs only wraps checked exceptions it does not declare; the callers report the cause.
            Throwable cause = e.getUndeclaredThrowable();
            throw cause instanceof Exception ? (Exception) cause : e;
        }
    }

    /**
     * Takes this scanner's hold on its configuration's scope. From here until {@link #closeInternal}
     * the filesystems it opens are cached under that scope's UGI and cannot be closed from under it;
     * see {@link HudiFileSystemScopes}.
     */
    private void acquireFileSystemScope() {
        if (fileSystemScopeKey != null && fileSystemScope == null) {
            fileSystemScope = HudiFileSystemScopes.shared().acquire(fileSystemScopeKey, fileSystemScopeUser);
        }
    }

    /** Gives the hold back. Idempotent, like the close that calls it. */
    private void releaseFileSystemScope() {
        HudiFileSystemScopes.Hold hold = fileSystemScope;
        fileSystemScope = null;
        if (hold != null) {
            hold.release();
        }
    }

    @Override
    protected void openInternal() throws IOException {
        // Acquired here and not in the constructor: BE only closes a scanner whose open succeeded, so
        // a hold taken before this point would have no release when construction failed midway.
        acquireFileSystemScope();
        try {
            executeInFileSystemScope(() -> {
                initRequiredColumnsAndTypes();
                initTableInfo(requiredTypes, requiredFields, fetchSize);
                Properties properties = getReaderProperties();
                initReader(properties);
                return null;
            });

        } catch (Exception e) {
            closeInternal();
            LOG.warn("failed to open hadoop hudi jni scanner", e);
            throw new IOException("failed to open hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    @Override
    protected int getNext() throws IOException {
        try {
            return executeInFileSystemScope(() -> {
                NullWritable key = reader.createKey();
                ArrayWritable value = reader.createValue();
                long startTime = System.nanoTime();
                int numRows = 0;
                for (; numRows < batchSize; numRows++) {
                    if (!reader.next(key, value)) {
                        break;
                    }
                    if (fields.length > 0) {
                        Object rowData = deserializer.deserialize(value);
                        for (int i = 0; i < fields.length; i++) {
                            Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
                            columnValue.setRow(fieldData);
                            columnValue.setField(types[i], fieldInspectors[i]);
                            appendData(i, columnValue);
                        }
                    }
                }
                // vectorTable is virtual
                if (fields.length == 0) {
                    vectorTable.appendVirtualData(numRows);
                }
                appendDataTime += System.nanoTime() - startTime;
                return numRows;
            });
        } catch (Exception e) {
            closeInternal();
            LOG.warn("failed to get next in hadoop hudi jni scanner", e);
            throw new IOException("failed to get next in hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    @Override
    protected void closeInternal() throws IOException {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.warn("failed to close hadoop hudi jni scanner", e);
            throw new IOException("failed to close hadoop hudi jni scanner: " + e.getMessage(), e);
        } finally {
            // After the reader: the reader reads through the filesystems the scope keeps open.
            releaseFileSystemScope();
        }
    }

    private void initRequiredColumnsAndTypes() {
        String[] splitHudiColumnNames = hudiColumnNames.split(",");

        Map<String, Integer> hudiColNameToIdx =
                IntStream.range(0, splitHudiColumnNames.length)
                        .boxed()
                        .collect(Collectors.toMap(i -> splitHudiColumnNames[i], i -> i));

        Map<String, String> hudiColNameToType =
                IntStream.range(0, splitHudiColumnNames.length)
                        .boxed()
                        .collect(Collectors.toMap(i -> splitHudiColumnNames[i], i -> hudiColumnTypes[i]));

        requiredTypes = new ColumnType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            String requiredField = requiredFields[i];
            if (!hudiColNameToType.containsKey(requiredField)) {
                throw new IllegalArgumentException(
                        "Required field " + requiredField + " not found in Hudi column names: " + splitHudiColumnNames);
            }
            requiredTypes[i] = ColumnType.parseType(requiredField, hudiColNameToType.get(requiredField));
        }

        requiredColumnIds = Arrays.stream(requiredFields)
                .mapToInt(hudiColNameToIdx::get)
                .boxed().collect(Collectors.toList());
    }

    private Properties getReaderProperties() {
        Properties properties = new Properties();
        properties.setProperty("hive.io.file.readcolumn.ids", Joiner.on(",").join(requiredColumnIds));
        properties.setProperty("hive.io.file.readcolumn.names", Joiner.on(",").join(this.requiredFields));
        properties.setProperty("columns", this.hudiColumnNames);
        properties.setProperty("columns.types", Joiner.on(",").join(hudiColumnTypes));
        properties.setProperty("serialization.lib", this.serde);
        properties.setProperty("hive.io.file.read.all.columns", "false");
        fsOptionsProps.forEach(properties::setProperty);
        return properties;
    }

    private void initReader(Properties properties) throws Exception {
        String realtimePath = dataFileLength != -1 ? dataFilePath : deltaFilePaths[0];
        long realtimeLength = dataFileLength != -1 ? dataFileLength : 0;
        Path path = new Path(realtimePath);
        FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, (String[]) null);
        List<HoodieLogFile> logFiles = Arrays.stream(deltaFilePaths).map(HoodieLogFile::new)
                .collect(Collectors.toList());
        FileSplit hudiSplit =
                new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, instantTime, false, Option.empty());

        JobConf jobConf = new JobConf(new Configuration());
        properties.stringPropertyNames().forEach(name -> jobConf.set(name, properties.getProperty(name)));
        InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf, inputFormat);
        reader = (RecordReader<NullWritable, ArrayWritable>) inputFormatClass
                .getRecordReader(hudiSplit, jobConf, Reporter.NULL);

        deserializer = getDeserializer(jobConf, properties, serde);
        rowInspector = getTableObjectInspector(deserializer);
        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
        }
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat) throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name)
            throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(name, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer) throws Exception {
        ObjectInspector inspector = deserializer.getObjectInspector();
        Preconditions.checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT,
                "expected STRUCT: %s", inspector.getCategory());
        return (StructObjectInspector) inspector;
    }
}
