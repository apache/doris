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
import org.apache.doris.common.io.IOUtils;
import org.apache.doris.common.util.HttpURLUtil;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.persist.gson.GsonUtils;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;

public class MetaHelper {
    private static final String PART_SUFFIX = ".part";
    public static final String X_IMAGE_SIZE = "X-Image-Size";
    public static final String X_IMAGE_MD5 = "X-Image-Md5";
    private static final int BUFFER_BYTES = 8 * 1024;
    private static final int CHECKPOINT_LIMIT_BYTES = 30 * 1024 * 1024;

    public static File getMasterImageDir() {
        String metaDir = Env.getCurrentEnv().getImageDir();
        return new File(metaDir);
    }

    public static int getLimit() {
        return CHECKPOINT_LIMIT_BYTES;
    }

    // rename the .PART_SUFFIX file to filename
    public static File complete(String filename, File dir) throws IOException {
        File file = new File(dir, filename + MetaHelper.PART_SUFFIX);
        File newFile = new File(dir, filename);
        if (!file.renameTo(newFile)) {
            throw new IOException("Complete file" + filename + " failed");
        }
        return newFile;
    }

    public static OutputStream getOutputStream(String filename, File dir)
            throws FileNotFoundException {
        File file = new File(dir, filename + MetaHelper.PART_SUFFIX);
        return new FileOutputStream(file);
    }

    public static File getFile(String filename, File dir) {
        return new File(dir, filename + MetaHelper.PART_SUFFIX);
    }

    public static <T> ResponseBody doGet(String url, int timeout, Class<T> clazz) throws IOException {
        String response = HttpUtils.doGet(url, HttpURLUtil.getNodeIdentHeaders(), timeout);
        return parseResponse(response, clazz);
    }

    // download file from remote node
    public static void getRemoteFile(String urlStr, int timeout, File file)
            throws IOException {
        HttpURLConnection conn = null;
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
