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

package org.apache.doris.master;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.IOUtils;
import org.apache.doris.common.util.HttpURLUtil;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.persist.gson.GsonUtils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Map;

public class MetaHelper {
    public static final Logger LOG = LogManager.getLogger(MetaHelper.class);
    private static final String PART_SUFFIX = ".part";
    public static final String X_IMAGE_SIZE = "X-Image-Size";
    public static final String X_IMAGE_MD5 = "X-Image-Md5";
    private static final int BUFFER_BYTES = 8 * 1024;
    private static final int CHECKPOINT_LIMIT_BYTES = 30 * 1024 * 1024;
    private static final String VALID_FILENAME_REGEX = "^(?!\\.)[a-zA-Z0-9_\\-.]+$";


    public static File getMasterImageDir() {
        String metaDir = Env.getCurrentEnv().getImageDir();
        return new File(metaDir);
    }

    public static int getLimit() {
        return CHECKPOINT_LIMIT_BYTES;
    }

    private static void completeCheck(File dir, File file, File newFile) throws IOException {
        if (!Config.meta_helper_security_mode) {
            return;
        }
        String dirPath = dir.getCanonicalPath(); // Get the canonical path of the directory
        String filePath = file.getCanonicalPath(); // Get the canonical path of the original file
        String newFilePath = newFile.getCanonicalPath(); // Get the canonical path of the new file

        // Ensure both file paths are within the specified directory to prevent path traversal attacks
        if (!filePath.startsWith(dirPath) || !newFilePath.startsWith(dirPath)) {
            throw new SecurityException("File path traversal attempt detected.");
        }

        // Ensure the original file exists and is a valid file to avoid renaming a non-existing file
        if (!file.exists() || !file.isFile()) {
            throw new IOException("Source file does not exist or is not a valid file.");
        }

    }

    // rename the .PART_SUFFIX file to filename
    public static File complete(String filename, File dir) throws IOException {
        // Validate that the filename does not contain illegal path elements
        checkIsValidFileName(filename);

        File file = new File(dir, filename + MetaHelper.PART_SUFFIX); // Original file with a specific suffix
        File newFile = new File(dir, filename); // Target file without the suffix

        completeCheck(dir, file, newFile);
        // Attempt to rename the file. If it fails, throw an exception
        if (!file.renameTo(newFile)) {
            throw new IOException("Complete file " + filename + " failed");
        }

        return newFile; // Return the newly renamed file
    }

    public static File getFile(String filename, File dir) throws IOException {
        checkIsValidFileName(filename);
        File file = new File(dir, filename + MetaHelper.PART_SUFFIX);
        checkFile(dir, file);
        return file;
    }

    private static void checkFile(File dir, File file) throws IOException {
        if (!Config.meta_helper_security_mode) {
            return;
        }
        String dirPath = dir.getCanonicalPath();
        String filePath = file.getCanonicalPath();

        if (!filePath.startsWith(dirPath)) {
            throw new SecurityException("File path traversal attempt detected.");
        }
    }

    protected static void checkIsValidFileName(String filename) {
        if (!Config.meta_helper_security_mode) {
            return;
        }
        if (StringUtils.isBlank(filename)) {
            return;
        }
        if (!filename.matches(VALID_FILENAME_REGEX)) {
            throw new IllegalArgumentException("Invalid filename : " + filename);
        }
    }

    private static void checkFile(File file) throws IOException {
        if (!Config.meta_helper_security_mode) {
            return;
        }
        if (!file.getAbsolutePath().startsWith(file.getCanonicalFile().getParent())) {
            throw new IllegalArgumentException("Invalid file path");
        }

        File parentDir = file.getParentFile();
        if (!parentDir.canWrite()) {
            throw new IOException("No write permission in directory: " + parentDir);
        }

        if (file.exists() && !file.delete()) {
            throw new IOException("Failed to delete existing file: " + file);
        }
        checkIsValidFileName(file.getName());
    }

    public static <T> ResponseBody doGet(String url, int timeout, Class<T> clazz) throws IOException {
        Map<String, String> headers = HttpURLUtil.getNodeIdentHeaders();
        LOG.info("meta helper, url: {}, timeout{}, headers: {}", url, timeout, headers);
        String response = HttpUtils.doGet(url, headers, timeout);
        return parseResponse(response, clazz);
    }

    // download file from remote node
    public static void getRemoteFile(String urlStr, int timeout, File file)
            throws IOException {
        HttpURLConnection conn = null;
        checkFile(file);
        OutputStream out = new FileOutputStream(file);
        try {
            conn = HttpURLUtil.getConnectionWithNodeIdent(urlStr);
            conn.setConnectTimeout(timeout);
            conn.setReadTimeout(timeout);

            // Get image size
            long imageSize = -1;
            String imageSizeStr = conn.getHeaderField(X_IMAGE_SIZE);
            if (imageSizeStr != null) {
                imageSize = Long.parseLong(imageSizeStr);
            }
            if (imageSize < 0) {
                throw new IOException(getResponse(conn));
            }
            String remoteMd5 = conn.getHeaderField(X_IMAGE_MD5);
            BufferedInputStream bin = new BufferedInputStream(conn.getInputStream());

            // Do not limit speed in client side.
            long bytes = IOUtils.copyBytes(bin, out, BUFFER_BYTES, CHECKPOINT_LIMIT_BYTES, true);

            if ((imageSize > 0) && (bytes != imageSize)) {
                throw new IOException("Unexpected image size, expected: " + imageSize + ", actual: " + bytes);
            }

            // if remoteMd5 not null ,we need check md5
            if (remoteMd5 != null) {
                String localMd5 = DigestUtils.md5Hex(new FileInputStream(file));
                if (!remoteMd5.equals(localMd5)) {
                    throw new IOException("Unexpected image md5, expected: " + remoteMd5 + ", actual: " + localMd5);
                }
            }
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
            if (out != null) {
                out.close();
            }
        }
    }

    public static String getResponse(HttpURLConnection conn) throws IOException {
        String response;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line);
            }
            response = sb.toString();
        }
        return response;
    }

    public static <T> ResponseBody parseResponse(String response, Class<T> clazz) {
        return GsonUtils.GSON.fromJson(response,
                com.google.gson.internal.$Gson$Types.newParameterizedTypeWithOwner(null, ResponseBody.class, clazz));
    }

}
