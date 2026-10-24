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
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGlobResponse;
import org.apache.doris.proto.InternalService.PGlobResponse.PFileInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The implement of table valued function
 * local("file_path" = "path/to/file.txt", "backend_id" = "be_id").
 */
public class LocalTableValuedFunction extends ExternalFileTableValuedFunction {
    private static final Logger LOG = LogManager.getLogger(LocalTableValuedFunction.class);
    public static final String NAME = "local";
    public static final String PROP_BACKEND_ID = "backend_id";
    public static final String PROP_SHARED_STORAGE = "shared_storage";

    // This backend is user specified backend for listing files, fetching file schema and executing query.
    private long backendId;
    // This backend if for listing files and fetching file schema.
    // If "backendId" is set, "backendIdForRequest" will be set to "backendId",
    // otherwise, "backendIdForRequest" will be set to one of the available backends.
    private long backendIdForRequest = -1;
    private boolean sharedStorage = false;

    public LocalTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        checkEnabled();

        // Check privileges before listing files or discovering their schema.
        checkAuth(ConnectContext.get());

        // 1. analyze common properties
        Map<String, String> props = super.parseCommonProperties(properties);

        try {
            this.storageProperties = StorageProperties.createPrimary(props);
            backendConnectProperties.putAll(storageProperties.getBackendConfigProperties());
            String uri = storageProperties.validateAndGetUri(props);
            filePath = storageProperties.validateAndNormalizeUri(uri);
        } catch (UserException e) {
            throw new AnalysisException("Failed check storage props, " + e.getMessage(), e);
        }


        backendId = Long.parseLong(storageProperties.getBackendConfigProperties()
                .getOrDefault(PROP_BACKEND_ID, "-1"));
        sharedStorage = Boolean.parseBoolean(storageProperties.getBackendConfigProperties()
                .getOrDefault(PROP_SHARED_STORAGE, "false"));

        // If not shared storage, backend_id is required
        // If is shared storage, backend_id can still be set, so that we can query data on single BE node.
        // Otherwise, if shared storage is true, we may use multi BE nodes.
        if (backendId == -1 && !sharedStorage) {
            throw new AnalysisException("'backend_id' is required when 'shared_storage' is false.");
        }

        initializeBackendForRequest();
        if (!isLanceFormat()) {
            // A Lance path identifies a dataset directory and is opened directly by Lance-C.
            // Globbing it would incorrectly turn its internal files into independent splits.
            getFileListFromBackend();
        }
    }

    /** Select the schema backend from the same eligibility policy used for query execution. */
    private void initializeBackendForRequest() throws AnalysisException {
        Preconditions.checkState(backendId != -1 || sharedStorage);
        FederationBackendPolicy backendPolicy = new FederationBackendPolicy();
        try {
            if (backendId == -1) {
                backendPolicy.init();
            } else {
                backendPolicy.initWithBackendId(backendId);
            }
        } catch (UserException e) {
            throw new AnalysisException("Failed to select local TVF backend: " + e.getMessage(), e);
        }
        backendIdForRequest = backendPolicy.getNextBe().getId();
    }

    private void getFileListFromBackend() throws AnalysisException {
        Backend be = Env.getCurrentSystemInfo().getBackend(backendIdForRequest);
        Preconditions.checkNotNull(be);
        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        TNetworkAddress address = be.getBrpcAddress();
        InternalService.PGlobRequest.Builder requestBuilder = InternalService.PGlobRequest.newBuilder();
        requestBuilder.setPattern(filePath);
        long timeoutS = ConnectContext.get() == null ? 60 : Math.min(ConnectContext.get().getQueryTimeoutS(), 60);
        try {
            Future<PGlobResponse> response = proxy.glob(address, requestBuilder.build());
            PGlobResponse globResponse = response.get(timeoutS, TimeUnit.SECONDS);
            if (globResponse.getStatus().getStatusCode() != 0) {
                throw new AnalysisException(
                        "error code: " + globResponse.getStatus().getStatusCode()
                                + ", error msg: " + globResponse.getStatus().getErrorMsgsList());
            }
            for (PFileInfo file : globResponse.getFilesList()) {
                fileStatuses.add(new TBrokerFileStatus(file.getFile().trim(), false, file.getSize(), true));
                LOG.info("get file from backend success. file: {}, size: {}", file.getFile(), file.getSize());
            }
        } catch (Exception e) {
            throw new AnalysisException("get file list from backend failed. " + e.getMessage());
        }
    }

    /** Apply LOCAL admission to the effective sink before planning or filesystem side effects. */
    public static void checkSinkEnabled(String tvfName, Map<String, String> properties) {
        if ("local".equalsIgnoreCase(tvfName) || isLocalWriter(properties.getOrDefault("writer_class", ""))) {
            checkEnabled();
        }
        if ("hdfs".equalsIgnoreCase(tvfName)) {
            if (!properties.containsKey("writer_class")) {
                checkHdfsSinkEnabled(properties);
            }
            if (Boolean.parseBoolean(properties.getOrDefault("delete_existing_files", "false"))) {
                checkHadoopFileSystemEnabled(
                        BrokerUtil.extractParentDirectory(properties.getOrDefault("file_path", "")), properties);
            }
        }
    }

    private static boolean isLocalWriter(String writer) {
        // ScannerLoader accepts slashes, and JNI passes the class name as a C string.
        return "org.apache.doris.writer.LocalFileJniWriter".equals(nativeStringPrefix(writer).replace('/', '.'));
    }

    /** Check the NameNode and provider used by the native FILE_HDFS writer. */
    public static void checkHdfsSinkEnabled(Map<String, String> properties) {
        checkHdfsSinkEnabled(properties.getOrDefault("file_path", ""), properties);
    }

    public static void checkHdfsSinkEnabled(String outputPrefix, Map<String, String> properties) {
        String nameNode = nativeStringPrefix(properties.getOrDefault("fs.defaultFS", "")).trim();
        if ("default".equals(nameNode)) {
            // libhdfs delegates this selector to the BE's defaults, which may select local storage.
            checkEnabled();
        }
        // libhdfs prefixes bare NameNodes with hdfs:// rather than using the output path's scheme.
        checkHadoopSchemeEnabled(nameNode.contains("://") ? uriScheme(nameNode) : "hdfs", properties);
        if (!Config.enable_local_tvf) {
            try {
                URI filesystem = new URI(nameNode.contains("://") ? nameNode : "hdfs://" + nameNode);
                // BE appends a generated filename directly to this prefix, then convert_path
                // removes the NameNode. Resolve its parent, not an exact mount-point filename.
                String generated = outputPrefix + "doris-tvf-file";
                String rawNameNode = properties.getOrDefault("fs.defaultFS", "");
                if (!rawNameNode.isEmpty() && generated.contains(rawNameNode)) {
                    generated = generated.substring(rawNameNode.length());
                }
                if (!generated.startsWith("/")) {
                    generated = "/" + generated;
                }
                String path = hadoopPathUri(nativeStringPrefix(generated)).getPath();
                path = path.substring(0, path.lastIndexOf('/') + 1);
                if (path.length() > 1) {
                    path = path.substring(0, path.length() - 1);
                }
                checkViewFsEnabled(filesystem, path, properties, new HashSet<>());
            } catch (URISyntaxException e) {
                // Invalid NameNodes remain subject to the existing storage validation.
            }
        }
    }

    /** Check Hadoop's path/default-filesystem selection before FE-side cleanup. */
    public static void checkHadoopFileSystemEnabled(String path, Map<String, String> properties) {
        String scheme = uriScheme(path);
        if (!scheme.isEmpty()) {
            checkHadoopSchemeEnabled(scheme, properties);
            checkViewFsPathEnabled(path, path, properties);
            return;
        }
        String defaultFs = hadoopSelectorProperty("fs.defaultFS", properties);
        String legacyDefaultFs = hadoopSelectorProperty("fs.default.name", properties);
        // Configuration.set synchronizes aliases; either value can win when the map is applied.
        // Do not assume that the canonical key overrides a local legacy selector.
        if (defaultFs != null) {
            checkHadoopDefaultEnabled(defaultFs, path, properties);
        }
        if (legacyDefaultFs != null) {
            checkHadoopDefaultEnabled(legacyDefaultFs, path, properties);
        }
        if (defaultFs == null && legacyDefaultFs == null) {
            checkHadoopSchemeEnabled("file", properties);
        }
    }

    private static void checkHadoopDefaultEnabled(String value, String path, Map<String, String> properties) {
        value = hadoopSelectorValue(value).trim();
        // Hadoop retains the old "local" default-FS spelling.
        String scheme = "local".equals(value) ? "file" : uriScheme(value);
        checkHadoopSchemeEnabled(scheme.isEmpty() ? "hdfs" : scheme, properties);
        checkViewFsPathEnabled(scheme.isEmpty() ? "hdfs://" + value : value, path, properties);
    }

    private static void checkViewFsPathEnabled(String location, String path, Map<String, String> properties) {
        if (!Config.enable_local_tvf) {
            try {
                URI filesystem = uriScheme(path).isEmpty() ? new URI(location) : hadoopPathUri(path);
                checkViewFsEnabled(filesystem, hadoopPathUri(path).getPath(), properties, new HashSet<>());
            } catch (URISyntaxException e) {
                checkEnabled();
            }
        }
    }

    private static void checkHadoopSchemeEnabled(String scheme, Map<String, String> properties) {
        if ("file".equalsIgnoreCase(scheme)) {
            checkEnabled();
        }
        String implementationKey = "fs." + scheme + ".impl";
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // The native Hadoop configuration boundary consumes C strings for keys and values.
            if (implementationKey.equals(nativeStringPrefix(entry.getKey()).trim())) {
                String implementation = hadoopSelectorValue(nativeStringPrefix(entry.getValue())).trim();
                if ("org.apache.hadoop.fs.LocalFileSystem".equals(implementation)
                        || "org.apache.hadoop.fs.RawLocalFileSystem".equals(implementation)) {
                    checkEnabled();
                }
            }
        }
    }

    private static void checkViewFsEnabled(URI filesystem, String path, Map<String, String> properties,
            Set<String> activeTables) {
        if (Config.enable_local_tvf) {
            return;
        }
        String scheme = filesystem.getScheme();
        checkHadoopSchemeEnabled(scheme, properties);
        String implementation = hadoopSelectorProperty("fs." + scheme + ".impl", properties);
        if (implementation != null
                && "org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme".equals(implementation.trim())) {
            // OverloadScheme can reload runtime mount tables and overrides provider selection.
            checkEnabled();
        }
        boolean viewFs = isViewFs(scheme, implementation);
        if (!viewFs) {
            return;
        }
        String table = filesystem.getAuthority();
        String ignorePort = hadoopSelectorProperty("fs.viewfs.ignore.port.in.mount.table.name", properties);
        if (ignorePort != null && Boolean.parseBoolean(hadoopSelectorValue(ignorePort).trim())
                && table != null && filesystem.getPort() != -1) {
            table = filesystem.getHost();
            if (table == null) {
                checkEnabled();
            }
        }
        if (table == null || table.isEmpty()) {
            table = hadoopSelectorProperty("fs.viewfs.mounttable.default.name.key", properties);
            table = table == null ? "default" : hadoopSelectorValue(table);
        }
        // Nested ViewFS cycles or arbitrarily deep delegation cannot establish remote storage.
        if (activeTables.size() >= 32 || !activeTables.add(table)) {
            checkEnabled();
        }
        try {
            String prefix = "fs.viewfs.mounttable." + table + ".";
            String source = "";
            List<String> selectedKeys = new ArrayList<>();
            List<String> mountSources = new ArrayList<>();
            String fallback = null;
            String rootMerge = null;
            for (String rawKey : properties.keySet()) {
                String key = nativeStringPrefix(rawKey).trim();
                if (!key.startsWith(prefix)) {
                    continue;
                }
                String entry = key.substring(prefix.length());
                if (entry.equals("linkFallback")) {
                    fallback = key;
                    continue;
                }
                if (entry.equals("linkMergeSlash")) {
                    rootMerge = key;
                    continue;
                }
                if (entry.startsWith("linkRegex")) {
                    // Regex interception rewrites runtime paths before ordinary mount resolution.
                    checkEnabled();
                }
                int slash = entry.indexOf('/');
                if (slash < 0 || !(entry.startsWith("link.") || entry.startsWith("linkMerge.")
                        || entry.startsWith("linkNfly."))) {
                    continue;
                }
                String candidate = entry.substring(slash);
                // InodeTree splits source components and discards trailing empty components.
                while (candidate.length() > 1 && candidate.endsWith("/")) {
                    candidate = candidate.substring(0, candidate.length() - 1);
                }
                mountSources.add(candidate);
                if (!(path.equals(candidate) || path.startsWith(candidate + "/"))) {
                    continue;
                }
                if (candidate.length() > source.length()) {
                    source = candidate;
                    selectedKeys.clear();
                }
                if (candidate.equals(source)) {
                    selectedKeys.add(key);
                }
            }
            if (rootMerge != null) {
                selectedKeys.add(rootMerge);
                source = "";
            } else if (selectedKeys.isEmpty() && fallback != null) {
                selectedKeys.add(fallback);
            }
            if (selectedKeys.isEmpty()) {
                // No explicit target can prove what a BE-inherited mount table will select.
                checkEnabled();
            }
            for (String key : selectedKeys) {
                String targets = hadoopSelectorValue(hadoopSelectorProperty(key, properties));
                for (String target : targets.split(",")) {
                    if (target.isEmpty()) {
                        continue;
                    }
                    URI uri = new URI(target);
                    if (uri.getScheme() == null) {
                        checkEnabled();
                    }
                    String targetImplementation = hadoopSelectorProperty("fs." + uri.getScheme() + ".impl",
                            properties);
                    if (isViewFs(uri.getScheme(), targetImplementation)) {
                        for (String nested : mountSources) {
                            if (!nested.startsWith(source + "/")) {
                                continue;
                            }
                            int branchEnd = nested.indexOf('/', source.length() + 1);
                            String branch = branchEnd < 0 ? nested : nested.substring(0, branchEnd);
                            if (path.equals(branch) || path.startsWith(branch + "/")) {
                                // A missing child below an internal nested-mount directory uses
                                // a different suffix in InodeTree. Do not guess its delegation.
                                checkEnabled();
                            }
                        }
                    }
                    String base = uri.getPath() == null ? "/" : uri.getPath();
                    String remaining = path.substring(source.length());
                    String nextPath = new URI(null, null,
                            (base.endsWith("/") ? base : base + "/")
                                    + (remaining.startsWith("/") ? remaining.substring(1) : remaining), null)
                            .normalize().getPath();
                    checkViewFsEnabled(uri, nextPath, properties, activeTables);
                }
            }
        } catch (URISyntaxException e) {
            checkEnabled();
        } finally {
            activeTables.remove(table);
        }
    }

    private static boolean isViewFs(String scheme, String implementation) {
        return implementation == null ? "viewfs".equals(scheme)
                : "org.apache.hadoop.fs.viewfs.ViewFileSystem".equals(implementation.trim());
    }

    private static URI hadoopPathUri(String path) throws URISyntaxException {
        String scheme = uriScheme(path);
        String authority = null;
        String relative = scheme.isEmpty() ? path : path.substring(scheme.length() + 1);
        if (relative.startsWith("//")) {
            int slash = relative.indexOf('/', 2);
            authority = slash < 0 ? relative.substring(2) : relative.substring(2, slash);
            relative = slash < 0 ? "/" : relative.substring(slash);
        }
        if (!relative.startsWith("/")) {
            relative = "/" + relative;
        }
        // Hadoop Path(String) quotes path bytes before URI normalization; percent text is literal.
        return new URI(scheme.isEmpty() ? null : scheme, authority, relative, null, null).normalize();
    }

    private static String hadoopSelectorProperty(String key, Map<String, String> properties) {
        String value = null;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // Configuration.set trims keys; native configuration strings also stop at NUL.
            if (key.equals(nativeStringPrefix(entry.getKey()).trim())) {
                String candidate = nativeStringPrefix(entry.getValue());
                if (value != null && !value.equals(candidate)) {
                    // FE and BE map iteration can choose different conflicting spellings.
                    checkEnabled();
                }
                value = candidate;
            }
        }
        return value;
    }

    private static String hadoopSelectorValue(String value) {
        if (value.contains("${")) {
            // Hadoop aliases and BE runtime properties can change the referenced value.
            // An indirect filesystem selector needs opt-in; the FE cannot prove it is remote.
            checkEnabled();
        }
        return value;
    }

    private static String uriScheme(String path) {
        int colon = path.indexOf(':');
        int slash = path.indexOf('/');
        return colon > 0 && (slash < 0 || colon < slash) ? path.substring(0, colon) : "";
    }

    private static String nativeStringPrefix(String value) {
        int nul = value.indexOf('\0');
        return nul < 0 ? value : value.substring(0, nul);
    }

    /** Check read and write admission before properties or backend access. */
    public static void checkEnabled() {
        if (!Config.enable_local_tvf) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "LOCAL TVF is disabled. Set enable_local_tvf=true in fe.conf and restart every FE to enable it.");
        }
    }

    @Override
    public void checkAuth(ConnectContext ctx) {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)
                && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.OPERATOR)) {
            String message = ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR.formatErrorMsg("ADMIN/OPERATOR");
            throw new org.apache.doris.nereids.exceptions.AnalysisException(message);
        }
        super.checkAuth(ctx);
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_LOCAL;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("LocalTvfBroker", StorageType.LOCAL, processedParams);
    }

    @Override
    public String getTableName() {
        return "LocalTableValuedFunction";
    }

    public Long getBackendId() {
        return backendId;
    }

    @Override
    public long getBackendIdForExecution() {
        return isLanceFormat() ? backendIdForRequest : backendId;
    }

    @Override
    protected Backend getBackend() {
        return Env.getCurrentSystemInfo().getBackend(backendIdForRequest);
    }
}
