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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TParquetMetadataParams;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Table-valued function parquet_meta for reading Parquet metadata.
 * Currently works in two modes:
 * - parquet_meta (mode parquet_metadata): row-group/column statistics similar to DuckDB parquet_metadata()
 * - parquet_schema: logical schema similar to DuckDB parquet_schema()
 * - parquet_file_metadata: file-level metadata aligned with DuckDB parquet_file_metadata()
 * - parquet_kv_metadata: file key/value metadata aligned with DuckDB parquet_kv_metadata()
 * - parquet_bloom_probe: row group bloom filter probe aligned with DuckDB parquet_bloom_probe()
 */
public class ParquetMetadataTableValuedFunction extends MetadataTableValuedFunction {

    public static final String NAME = "parquet_meta";
    public static final String NAME_FILE_METADATA = "parquet_file_metadata";
    public static final String NAME_KV_METADATA = "parquet_kv_metadata";
    public static final String NAME_BLOOM_PROBE = "parquet_bloom_probe";
    private static final String PATH = "path";
    private static final String MODE = "mode";
    private static final String COLUMN = "column";
    private static final String COLUMN_NAME = "column_name";
    private static final String VALUE = "value";

    private static final String MODE_METADATA = "parquet_metadata";
    private static final String MODE_SCHEMA = "parquet_schema";
    private static final String MODE_FILE_METADATA = "parquet_file_metadata";
    private static final String MODE_KV_METADATA = "parquet_kv_metadata";
    private static final String MODE_BLOOM_PROBE = "parquet_bloom_probe";
    private static final ImmutableSet<String> SUPPORTED_MODES =
            ImmutableSet.of(MODE_METADATA, MODE_SCHEMA, MODE_FILE_METADATA, MODE_KV_METADATA,
                    MODE_BLOOM_PROBE);

    private static final ImmutableList<Column> PARQUET_SCHEMA_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("column_name", PrimitiveType.STRING, true),
            new Column("column_path", PrimitiveType.STRING, true),
            new Column("physical_type", PrimitiveType.STRING, true),
            new Column("logical_type", PrimitiveType.STRING, true),
            new Column("repetition_level", PrimitiveType.INT, true),
            new Column("definition_level", PrimitiveType.INT, true),
            new Column("type_length", PrimitiveType.INT, true),
            new Column("precision", PrimitiveType.INT, true),
            new Column("scale", PrimitiveType.INT, true),
            new Column("is_nullable", PrimitiveType.BOOLEAN, true)
    );

    private static final ImmutableList<Column> PARQUET_METADATA_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("row_group_id", PrimitiveType.INT, true),
            new Column("column_id", PrimitiveType.INT, true),
            new Column("column_name", PrimitiveType.STRING, true),
            new Column("column_path", PrimitiveType.STRING, true),
            new Column("physical_type", PrimitiveType.STRING, true),
            new Column("logical_type", PrimitiveType.STRING, true),
            new Column("type_length", PrimitiveType.INT, true),
            new Column("converted_type", PrimitiveType.STRING, true),
            new Column("num_values", PrimitiveType.BIGINT, true),
            new Column("null_count", PrimitiveType.BIGINT, true),
            new Column("distinct_count", PrimitiveType.BIGINT, true),
            new Column("encodings", PrimitiveType.STRING, true),
            new Column("compression", PrimitiveType.STRING, true),
            new Column("data_page_offset", PrimitiveType.BIGINT, true),
            new Column("index_page_offset", PrimitiveType.BIGINT, true),
            new Column("dictionary_page_offset", PrimitiveType.BIGINT, true),
            new Column("total_compressed_size", PrimitiveType.BIGINT, true),
            new Column("total_uncompressed_size", PrimitiveType.BIGINT, true),
            new Column("statistics_min", ScalarType.createStringType(), true),
            new Column("statistics_max", ScalarType.createStringType(), true)
    );

    private static final ImmutableList<Column> PARQUET_FILE_METADATA_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("created_by", PrimitiveType.STRING, true),
            new Column("num_rows", PrimitiveType.BIGINT, true),
            new Column("num_row_groups", PrimitiveType.BIGINT, true),
            new Column("format_version", PrimitiveType.BIGINT, true),
            new Column("encryption_algorithm", PrimitiveType.STRING, true),
            new Column("footer_signing_key_metadata", PrimitiveType.STRING, true)
    );

    private static final ImmutableList<Column> PARQUET_KV_METADATA_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("key", ScalarType.createStringType(), true),
            new Column("value", ScalarType.createStringType(), true)
    );

    private static final ImmutableList<Column> PARQUET_BLOOM_PROBE_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("row_group_id", PrimitiveType.INT, true),
            new Column("bloom_filter_excludes", PrimitiveType.BOOLEAN, true)
    );

    private final List<String> paths;
    private final String mode;
    // File system info for remote Parquet access (e.g. S3).
    private final TFileType fileType;
    private final Map<String, String> properties;
    private final String bloomColumn;
    private final String bloomLiteral;

    public ParquetMetadataTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> normalizedParams = params.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toLowerCase(),
                Map.Entry::getValue,
                (value1, value2) -> value2
        ));
        String rawPath = normalizedParams.get(PATH);
        if (Strings.isNullOrEmpty(rawPath)) {
            throw new AnalysisException("Property 'path' is required for parquet_meta");
        }
        String parsedPath = rawPath.trim();
        if (parsedPath.isEmpty()) {
            throw new AnalysisException("Property 'path' must contain at least one location");
        }
        List<String> parsedPaths = Collections.singletonList(parsedPath);
        List<String> normalizedPaths = parsedPaths;

        String rawMode = normalizedParams.getOrDefault(MODE, MODE_METADATA);
        mode = rawMode.toLowerCase();
        if (!SUPPORTED_MODES.contains(mode)) {
            throw new AnalysisException("Unsupported mode '" + rawMode + "' for parquet_meta");
        }
        String tmpBloomColumn = null;
        String tmpBloomLiteral = null;
        if (MODE_BLOOM_PROBE.equals(mode)) {
            tmpBloomColumn = normalizedParams.get(COLUMN);
            if (Strings.isNullOrEmpty(tmpBloomColumn)) {
                tmpBloomColumn = normalizedParams.get(COLUMN_NAME);
            }
            tmpBloomLiteral = normalizedParams.get(VALUE);
            if (Strings.isNullOrEmpty(tmpBloomColumn) || Strings.isNullOrEmpty(tmpBloomLiteral)) {
                throw new AnalysisException(
                        "Missing 'column' or 'value' for mode parquet_bloom_probe");
            }
            tmpBloomColumn = tmpBloomColumn.trim();
            tmpBloomLiteral = tmpBloomLiteral.trim();
            if (tmpBloomColumn.isEmpty() || tmpBloomLiteral.isEmpty()) {
                throw new AnalysisException(
                        "Missing 'column' or 'value' for mode parquet_bloom_probe");
            }
        }

        String firstPath = parsedPaths.get(0);
        String scheme = null;
        try {
            scheme = new URI(firstPath).getScheme();
        } catch (URISyntaxException e) {
            scheme = null;
        }
        Map<String, String> storageParams = new HashMap<>(normalizedParams);
        // StorageProperties detects provider by "uri", we also hint support by scheme.
        if (!Strings.isNullOrEmpty(scheme)) {
            storageParams.put("uri", firstPath);
        }
        if (Strings.isNullOrEmpty(scheme) || "file".equalsIgnoreCase(scheme)) {
            storageParams.put(StorageProperties.FS_LOCAL_SUPPORT, "true");
        } else {
            forceStorageSupport(storageParams, scheme.toLowerCase());
        }
        StorageProperties storageProperties;
        try {
            storageProperties = StorageProperties.createPrimary(storageParams);
        } catch (RuntimeException e) {
            throw new AnalysisException(
                    "Failed to parse storage properties for parquet_meta: " + e.getMessage(), e);
        }
        this.fileType = mapToFileType(storageProperties.getType());
        Map<String, String> backendProps = storageProperties.getBackendConfigProperties();
        try {
            List<String> tmpPaths = new ArrayList<>(parsedPaths.size());
            for (String path : parsedPaths) {
                tmpPaths.add(storageProperties.validateAndNormalizeUri(path));
            }
            normalizedPaths = tmpPaths;
        } catch (UserException e) {
            throw new AnalysisException(
                    "Failed to normalize parquet_meta paths: " + e.getMessage(), e);
        }
        if (this.fileType == TFileType.FILE_HTTP && !backendProps.containsKey("uri")) {
            backendProps = new HashMap<>(backendProps);
            backendProps.put("uri", normalizedPaths.get(0));
        }
        this.properties = backendProps;

        // Expand any glob patterns (e.g. *.parquet) to concrete file paths.
        normalizedPaths = expandGlobPaths(normalizedPaths, storageProperties, storageParams, this.fileType);

        this.paths = ImmutableList.copyOf(normalizedPaths);
        this.bloomColumn = tmpBloomColumn;
        this.bloomLiteral = tmpBloomLiteral;
    }

    /**
     * Expand wildcard paths to matching files.
     */
    private static List<String> expandGlobPaths(List<String> inputPaths,
                                                StorageProperties storageProperties,
                                                Map<String, String> storageParams,
                                                TFileType fileType) throws AnalysisException {
        if (inputPaths == null || inputPaths.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> expanded = new ArrayList<>();
        for (String path : inputPaths) {
            if (!containsWildcards(path)) {
                expanded.add(path);
                continue;
            }
            expanded.addAll(expandSingleGlob(path, storageProperties, storageParams, fileType));
        }
        if (expanded.isEmpty()) {
            throw new AnalysisException("No files matched parquet_meta path patterns: " + inputPaths);
        }
        return expanded;
    }

    private static boolean containsWildcards(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return false;
        }
        return path.contains("*") || path.contains("[") || path.contains("{");
    }

    private static List<String> expandSingleGlob(String pattern,
                                                 StorageProperties storageProperties,
                                                 Map<String, String> storageParams,
                                                 TFileType fileType) throws AnalysisException {
        if (fileType == TFileType.FILE_LOCAL) {
            return globLocal(pattern);
        }
        if (fileType == TFileType.FILE_HTTP) {
            throw new AnalysisException("Glob patterns are not supported for file type: " + fileType);
        }
        if (storageProperties == null) {
            throw new AnalysisException("Storage properties is required for glob pattern: " + pattern);
        }
        if (fileType == TFileType.FILE_S3 || fileType == TFileType.FILE_HDFS) {
            return globRemoteWithBroker(pattern, storageParams);
        }
        throw new AnalysisException("Glob patterns are not supported for file type: " + fileType);
    }

    private static List<String> globRemoteWithBroker(String pattern,
                                                     Map<String, String> storageParams) throws AnalysisException {
        List<TBrokerFileStatus> remoteFiles = new ArrayList<>();
        BrokerDesc brokerDesc = new BrokerDesc("ParquetMetaTvf", storageParams);
        try {
            BrokerUtil.parseFile(pattern, brokerDesc, remoteFiles);
        } catch (UserException e) {
            throw new AnalysisException("Failed to expand glob pattern '" + pattern + "': "
                    + e.getMessage(), e);
        }
        return remoteFiles.stream()
                .filter(file -> !file.isIsDir())
                .map(TBrokerFileStatus::getPath)
                .collect(Collectors.toList());
    }

    /**
     * Local glob expansion is executed on an arbitrary alive backend, because FE may not
     * have access to BE local file system.
     */
    private static List<String> globLocal(String pattern) throws AnalysisException {
        Backend backend;
        try {
            backend = Env.getCurrentSystemInfo().getBackendsByCurrentCluster().values().stream()
                    .filter(Backend::isAlive)
                    .findFirst()
                    .orElse(null);
        } catch (AnalysisException e) {
            throw e;
        }
        if (backend == null) {
            throw new AnalysisException("No alive backends to expand local glob pattern");
        }
        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        String patternForBe = pattern;
        // BE safe_glob prepends user_files_secure_path (default ${DORIS_HOME}); strip it to avoid double prefix.
        List<String> securePrefixCandidates = new ArrayList<>();
        String envSecure = System.getenv("USER_FILES_SECURE_PATH");
        if (!Strings.isNullOrEmpty(envSecure)) {
            securePrefixCandidates.add(envSecure);
        }
        String envDorisHome = System.getenv("DORIS_HOME");
        if (!Strings.isNullOrEmpty(envDorisHome)) {
            securePrefixCandidates.add(envDorisHome);
        }
        // 兜底：用路径所在目录作为前缀尝试剥离（针对在安全目录内的绝对路径）
        int lastSlash = pattern.lastIndexOf('/');
        if (pattern.startsWith("/") && lastSlash > 0) {
            securePrefixCandidates.add(pattern.substring(0, lastSlash));
        }
        for (String prefix : securePrefixCandidates) {
            if (Strings.isNullOrEmpty(prefix)) {
                continue;
            }
            String normalized = prefix.endsWith("/") ? prefix : prefix + "/";
            if (pattern.startsWith(normalized)) {
                patternForBe = pattern.substring(normalized.length());
                break;
            }
        }
        InternalService.PGlobRequest.Builder requestBuilder = InternalService.PGlobRequest.newBuilder();
        requestBuilder.setPattern(patternForBe);
        long timeoutS = ConnectContext.get() == null ? 60
                : Math.min(ConnectContext.get().getQueryTimeoutS(), 60);
        try {
            Future<InternalService.PGlobResponse> response =
                    proxy.glob(backend.getBrpcAddress(), requestBuilder.build());
            InternalService.PGlobResponse globResponse =
                    response.get(timeoutS, TimeUnit.SECONDS);
            if (globResponse.getStatus().getStatusCode() != 0) {
                throw new AnalysisException("Expand local glob pattern failed: "
                        + globResponse.getStatus().getErrorMsgsList());
            }
            List<String> result = new ArrayList<>();
            for (InternalService.PGlobResponse.PFileInfo fileInfo : globResponse.getFilesList()) {
                result.add(fileInfo.getFile().trim());
            }
            return result;
        } catch (Exception e) {
            throw new AnalysisException("Failed to expand local glob pattern '" + pattern + "': "
                    + e.getMessage(), e);
        }
    }

    /**
     * Add fs.* support hints so StorageProperties can be created from URI-only inputs.
     */
    private static void forceStorageSupport(Map<String, String> params, String scheme) {
        if ("s3".equals(scheme) || "s3a".equals(scheme) || "s3n".equals(scheme)) {
            params.put(StorageProperties.FS_S3_SUPPORT, "true");
        } else if ("oss".equals(scheme)) {
            params.put(StorageProperties.FS_OSS_SUPPORT, "true");
        } else if ("obs".equals(scheme)) {
            params.put(StorageProperties.FS_OBS_SUPPORT, "true");
        } else if ("cos".equals(scheme)) {
            params.put(StorageProperties.FS_COS_SUPPORT, "true");
        } else if ("gs".equals(scheme) || "gcs".equals(scheme)) {
            params.put(StorageProperties.FS_GCS_SUPPORT, "true");
        } else if ("minio".equals(scheme)) {
            params.put(StorageProperties.FS_MINIO_SUPPORT, "true");
        } else if ("azure".equals(scheme)) {
            params.put(StorageProperties.FS_AZURE_SUPPORT, "true");
        } else if ("http".equals(scheme) || "https".equals(scheme) || "hf".equals(scheme)) {
            params.put(StorageProperties.FS_HTTP_SUPPORT, "true");
        }
    }

    /**
     * Map FE storage type to BE file type.
     */
    private static TFileType mapToFileType(StorageProperties.Type type) throws AnalysisException {
        switch (type) {
            case HDFS:
            case OSS_HDFS:
                return TFileType.FILE_HDFS;
            case HTTP:
                return TFileType.FILE_HTTP;
            case LOCAL:
                return TFileType.FILE_LOCAL;
            case S3:
            case OSS:
            case OBS:
            case COS:
            case GCS:
            case MINIO:
            case AZURE:
                return TFileType.FILE_S3;
            default:
                throw new AnalysisException("Unsupported storage type for parquet_meta: " + type);
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.PARQUET;
    }

    @Override
    public TMetaScanRange getMetaScanRange(List<String> requiredFields) {
        TParquetMetadataParams parquetParams = new TParquetMetadataParams();
        parquetParams.setPaths(paths);
        parquetParams.setMode(mode);
        parquetParams.setFileType(fileType);
        parquetParams.setProperties(properties);
        if (MODE_BLOOM_PROBE.equals(mode)) {
            parquetParams.setBloomColumn(bloomColumn);
            parquetParams.setBloomLiteral(bloomLiteral);
        }

        TMetaScanRange scanRange = new TMetaScanRange();
        scanRange.setMetadataType(TMetadataType.PARQUET);
        scanRange.setParquetParams(parquetParams);
        // Fan out: one file per split so MetadataScanNode can distribute across BEs.
        if (paths != null && !paths.isEmpty()) {
            scanRange.setSerializedSplits(paths);
        }
        return scanRange;
    }

    @Override
    public String getTableName() {
        return "ParquetMetadataTableValuedFunction<" + mode + ">";
    }

    @Override
    public List<Column> getTableColumns() {
        if (MODE_SCHEMA.equals(mode)) {
            return PARQUET_SCHEMA_COLUMNS;
        }
        if (MODE_FILE_METADATA.equals(mode)) {
            return PARQUET_FILE_METADATA_COLUMNS;
        }
        if (MODE_KV_METADATA.equals(mode)) {
            return PARQUET_KV_METADATA_COLUMNS;
        }
        if (MODE_BLOOM_PROBE.equals(mode)) {
            return PARQUET_BLOOM_PROBE_COLUMNS;
        }
        return PARQUET_METADATA_COLUMNS;
    }
}
