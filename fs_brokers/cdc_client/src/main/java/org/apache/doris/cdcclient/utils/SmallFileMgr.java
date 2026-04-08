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

package org.apache.doris.cdcclient.utils;

import org.apache.doris.cdcclient.common.Env;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages small files (e.g. SSL certificates) referenced by FILE:{file_id}:{md5}.
 *
 * <p>Files are fetched from FE via HTTP on first access, then cached on disk as {file_id}.{md5} and
 * in memory to avoid repeated I/O on subsequent calls.
 */
public class SmallFileMgr {
    private static final Logger LOG = LoggerFactory.getLogger(SmallFileMgr.class);

    private static final String FILE_PREFIX = "FILE:";

    /** In-memory cache: "file_id:md5" -> absolute local file path */
    private static final Map<String, String> MEM_CACHE = new ConcurrentHashMap<>();

    /**
     * Per-key locks to serialize concurrent downloads of the same file, preventing tmp file
     * corruption when multiple threads race on the same file_id:md5 key.
     */
    private static final Map<String, Object> DOWNLOAD_LOCKS = new ConcurrentHashMap<>();

    private SmallFileMgr() {}

    /**
     * Resolve a FILE: reference to an absolute local file path, downloading from FE if needed. FE
     * address and cluster token are read from {@link Env}.
     *
     * @param filePath FILE reference, format: FILE:{file_id}:{md5}
     * @return absolute local file path
     */
    public static String getFilePath(String filePath) {
        return getFilePath(
                Env.getCurrentEnv().getFeMasterAddress(),
                filePath,
                Env.getCurrentEnv().getClusterToken(),
                getLocalDir());
    }

    /**
     * Get the directory of the currently running JAR file
     *
     * @return
     */
    static String getLocalDir() {
        try {
            URL url = SmallFileMgr.class.getProtectionDomain().getCodeSource().getLocation();
            LOG.info("Get code source URL: {}", url);
            // Spring Boot fat jar: jar:file:/path/to/app.jar!/BOOT-INF/classes!/
            if ("jar".equals(url.getProtocol())) {
                String path = url.getPath(); // file:/path/to/app.jar!/BOOT-INF/classes!/
                int separator = path.indexOf("!");
                if (separator > 0) {
                    path = path.substring(0, separator); // file:/path/to/app.jar
                }
                url = new URL(path);
            }
            File file = new File(url.toURI());
            // When running a JAR file, `file` refers to the JAR file itself, taking its parent
            // directory.
            // When running an IDE file, `file` refers to the classes directory, returning directly.
            return file.isFile() ? file.getParentFile().getAbsolutePath() : file.getAbsolutePath();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /** Package-private overload that accepts a custom local directory, used for testing. */
    static String getFilePath(
            String feMasterAddress, String filePath, String clusterToken, String localDir) {
        if (!filePath.startsWith(FILE_PREFIX)) {
            throw new IllegalArgumentException("filePath must start with FILE:, got: " + filePath);
        }
        if (feMasterAddress == null || feMasterAddress.isEmpty()) {
            throw new IllegalArgumentException(
                    "feMasterAddress is required when filePath is a FILE: reference");
        }
        String[] parts = filePath.substring(FILE_PREFIX.length()).split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid filePath format, expected FILE:file_id:md5, got: " + filePath);
        }
        String fileId = parts[0];
        String md5 = parts[1];
        String cacheKey = fileId + ":" + md5;

        // 1. Fast path: in-memory cache hit — zero I/O, no lock needed
        String memCached = MEM_CACHE.get(cacheKey);
        if (memCached != null) {
            LOG.debug("SmallFile memory cache hit: {}", memCached);
            return memCached;
        }

        // 2. Serialize concurrent downloads of the same file to prevent tmp file corruption
        Object lock = DOWNLOAD_LOCKS.computeIfAbsent(cacheKey, k -> new Object());
        synchronized (lock) {
            // Double-check memory cache inside the lock
            String doubleChecked = MEM_CACHE.get(cacheKey);
            if (doubleChecked != null) {
                LOG.debug("SmallFile memory cache hit (after lock): {}", doubleChecked);
                return doubleChecked;
            }

            String finalFilePath = localDir + File.separator + fileId + "." + md5;
            File finalFile = new File(finalFilePath);

            // 3. Disk cache hit — avoid downloading again after process restart
            if (finalFile.exists()) {
                try (FileInputStream fis = new FileInputStream(finalFile)) {
                    String diskMd5 = DigestUtils.md5Hex(fis);
                    if (diskMd5.equalsIgnoreCase(md5)) {
                        LOG.info("SmallFile disk cache hit: {}", finalFilePath);
                        MEM_CACHE.put(cacheKey, finalFilePath);
                        return finalFilePath;
                    }
                    LOG.warn(
                            "SmallFile disk cache MD5 mismatch, re-downloading: {}", finalFilePath);
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to read disk cached file, re-downloading: {}",
                            finalFilePath,
                            e);
                }
                finalFile.delete();
            }

            // 4. Download from FE: GET /api/get_small_file?file_id=xxx&token=yyy
            String url =
                    "http://"
                            + feMasterAddress
                            + "/api/get_small_file?file_id="
                            + fileId
                            + "&token="
                            + clusterToken;
            LOG.info("Downloading small file from FE: {}", url);

            File tmpFile = new File(localDir + File.separator + fileId + ".tmp");
            try (CloseableHttpClient client = HttpUtil.getHttpClient();
                    CloseableHttpResponse response = client.execute(new HttpGet(url))) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new RuntimeException(
                            "Failed to download small file, status=" + statusCode + ", url=" + url);
                }
                try (InputStream in = response.getEntity().getContent();
                        FileOutputStream fos = new FileOutputStream(tmpFile)) {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = in.read(buf)) != -1) {
                        fos.write(buf, 0, n);
                    }
                }
            } catch (IOException e) {
                tmpFile.delete();
                throw new RuntimeException("Failed to download small file from FE: " + url, e);
            }

            // 5. Verify MD5 of downloaded content
            try (FileInputStream fis = new FileInputStream(tmpFile)) {
                String downloadedMd5 = DigestUtils.md5Hex(fis);
                if (!downloadedMd5.equalsIgnoreCase(md5)) {
                    tmpFile.delete();
                    throw new RuntimeException(
                            "Small file MD5 mismatch, expected=" + md5 + ", got=" + downloadedMd5);
                }
            } catch (IOException e) {
                tmpFile.delete();
                throw new RuntimeException("Failed to verify downloaded file MD5", e);
            }

            // 6. Atomically promote tmp file to final path
            if (!tmpFile.renameTo(finalFile)) {
                tmpFile.delete();
                throw new RuntimeException("Failed to rename tmp file to: " + finalFilePath);
            }

            LOG.info("Small file ready at: {}", finalFilePath);
            MEM_CACHE.put(cacheKey, finalFilePath);
            return finalFilePath;
        }
    }

    /** Clears the in-memory cache. Exposed for testing. */
    static void clearCache() {
        MEM_CACHE.clear();
        DOWNLOAD_LOCKS.clear();
    }
}
