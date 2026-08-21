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

package org.apache.doris.common.plugin;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.storage.ObjectInfo;
import org.apache.doris.cloud.storage.ObjectInfoAdapter;
import org.apache.doris.common.Config;
import org.apache.doris.common.EnvUtils;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.service.FrontendOptions;

import com.google.common.base.Strings;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * Simple cloud plugin downloader for UDF and JDBC drivers.
 */
public class CloudPluginDownloader {

    private static final Pattern SAFE_PLUGIN_NAME =
            Pattern.compile("^[A-Za-z0-9._@-]+(?:/[A-Za-z0-9._@-]+)*\\.jar$");
    private static final ConcurrentHashMap<Path, TargetLock> DOWNLOAD_LOCKS = new ConcurrentHashMap<>();

    public enum PluginType {
        JDBC_DRIVERS("jdbc_drivers"),
        JAVA_UDF("java_udf"),
        CONNECTORS("connectors"),     // Reserved, not supported yet
        HADOOP_CONF("hadoop_conf");   // Reserved, not supported yet

        private final String directory;

        PluginType(String directory) {
            this.directory = directory;
        }

        String directory() {
            return directory;
        }
    }

    /**
     * Download plugin from cloud storage to local path
     */
    public static String downloadFromCloud(PluginType type, String name, String localPath) {
        validateInput(type, name);
        Path targetPath = validateLocalPath(type, name, localPath);
        try {
            Cloud.ObjectStoreInfoPB objInfo = getCloudStorageInfo();
            String remotePath = buildS3Path(objInfo, type, name);
            return doDownload(objInfo, remotePath, targetPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to download plugin: " + e.getMessage(), e);
        }
    }

    /**
     * Validate input parameters
     */
    static void validateInput(PluginType type, String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Plugin name cannot be empty");
        }

        if (type != PluginType.JDBC_DRIVERS && type != PluginType.JAVA_UDF) {
            throw new UnsupportedOperationException("Plugin type " + type + " is not supported yet");
        }

        if (!SAFE_PLUGIN_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Plugin name must be a safe relative jar path: " + name);
        }

        Path normalizedName = Paths.get(name).normalize();
        if (normalizedName.isAbsolute() || normalizedName.startsWith("..")
                || !normalizedName.toString().equals(name)) {
            throw new IllegalArgumentException("Plugin name must stay inside its plugin directory: " + name);
        }
    }

    /** Returns whether the FE is running with the legacy instance object store. */
    public static boolean isLegacySaaSMode() {
        return Config.isCloudMode() && !((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault();
    }

    /**
     * Get cloud storage info from MetaService
     * Package-private for testing
     */
    static Cloud.ObjectStoreInfoPB getCloudStorageInfo() throws Exception {
        Cloud.GetObjStoreInfoResponse response = MetaServiceProxy.getInstance()
                .getObjStoreInfo(Cloud.GetObjStoreInfoRequest.newBuilder()
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                        .build());

        return selectCloudStorageInfo(response);
    }

    static Cloud.ObjectStoreInfoPB selectCloudStorageInfo(Cloud.GetObjStoreInfoResponse response) {
        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new RuntimeException("Failed to get storage info: " + response.getStatus().getMsg());
        }

        if (response.getEnableStorageVault()) {
            throw new RuntimeException("Cloud plugin auto-download is only supported in legacy SaaS mode");
        }

        if (response.getObjInfoList().isEmpty()) {
            throw new RuntimeException("Only SaaS cloud storage is supported currently");
        }

        return response.getObjInfo(response.getObjInfoCount() - 1);
    }

    /**
     * Build complete S3 path from objInfo
     * Package-private for testing
     */
    static String buildS3Path(Cloud.ObjectStoreInfoPB objInfo, PluginType type, String name) {
        validateInput(type, name);
        String bucket = objInfo.getBucket();
        String prefix = objInfo.hasPrefix() ? objInfo.getPrefix() : "";
        String relativePath = String.format("plugins/%s/%s", type.directory(), name);

        String fullPath;
        if (Strings.isNullOrEmpty(prefix)) {
            fullPath = bucket + "/" + relativePath;
        } else {
            fullPath = bucket + "/" + prefix + "/" + relativePath;
        }

        return "s3://" + fullPath;
    }

    /**
     * Execute download using SPI FileSystem
     */
    private static String doDownload(Cloud.ObjectStoreInfoPB objInfo, String remotePath, Path localPath)
            throws Exception {
        // Bind the provider reported by MetaService explicitly. Raw property auto-detection is
        // order-dependent when more than one storage provider recognizes the same map.
        StorageAdapter storageAdapter = createStorageAdapter(objInfo);
        try (FileSystem fileSystem = FileSystemFactory.getFileSystem(storageAdapter)) {
            return downloadToLocal(localPath,
                    () -> fileSystem.newInputFile(Location.of(remotePath)).newStream());
        }
    }

    static String downloadToLocal(Path localPath, RemoteStreamSupplier streamSupplier) throws Exception {
        Path normalizedPath = localPath.toAbsolutePath().normalize();
        TargetLock targetLock = acquireTargetLock(normalizedPath);
        try {
            return publishDownloadedFile(normalizedPath, streamSupplier);
        } finally {
            releaseTargetLock(normalizedPath, targetLock);
        }
    }

    private static String publishDownloadedFile(Path localPath, RemoteStreamSupplier streamSupplier)
            throws Exception {
        Path parentDir = localPath.getParent();
        Files.createDirectories(parentDir);
        Path tempFile = Files.createTempFile(parentDir, "." + localPath.getFileName() + ".", ".tmp");
        try {
            try (InputStream in = streamSupplier.open()) {
                Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }
            Files.move(tempFile, localPath, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            return localPath.toString();
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private static Path validateLocalPath(PluginType type, String name, String localPath) {
        Path pluginDirectory = Paths.get(EnvUtils.getDorisHome(), "plugins", type.directory())
                .toAbsolutePath().normalize();
        Path expectedPath = pluginDirectory.resolve(name).normalize();
        Path actualPath = Paths.get(localPath).toAbsolutePath().normalize();
        if (!expectedPath.startsWith(pluginDirectory) || !actualPath.equals(expectedPath)) {
            throw new IllegalArgumentException("Plugin target must stay inside " + pluginDirectory);
        }
        return actualPath;
    }

    private static TargetLock acquireTargetLock(Path localPath) {
        TargetLock targetLock = DOWNLOAD_LOCKS.compute(localPath, (path, current) -> {
            TargetLock result = current == null ? new TargetLock() : current;
            result.users++;
            return result;
        });
        targetLock.lock.lock();
        return targetLock;
    }

    private static void releaseTargetLock(Path localPath, TargetLock targetLock) {
        targetLock.lock.unlock();
        DOWNLOAD_LOCKS.compute(localPath, (path, current) -> {
            current.users--;
            return current.users == 0 ? null : current;
        });
    }

    /**
     * Bind cloud object-store information to the provider selected by MetaService.
     * Package-private for testing.
     */
    static StorageAdapter createStorageAdapter(Cloud.ObjectStoreInfoPB objInfo) {
        return ObjectInfoAdapter.toStorageAdapter(new ObjectInfo(objInfo));
    }

    @FunctionalInterface
    interface RemoteStreamSupplier {
        InputStream open() throws Exception;
    }

    private static class TargetLock {
        private final ReentrantLock lock = new ReentrantLock();
        private int users;
    }
}
