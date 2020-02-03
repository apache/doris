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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class MetaHelper {
    private static final String PART_SUFFIX = ".part";
    public static final String X_IMAGE_SIZE = "X-Image-Size";
    private static final int BUFFER_BYTES = 8 * 1024;
    private static final int CHECKPOINT_LIMIT_BYTES = 30 * 1024 * 1024;

    public static File getMasterImageDir() {
        String metaDir = Catalog.getCurrentCatalog().getImageDir();
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

    // download file from remote node
    public static void getRemoteFile(String urlStr, int timeout, OutputStream out)
            throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = null;

        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(timeout);
            conn.setReadTimeout(timeout);

            // Get image size
            long imageSize = -1;
            String imageSizeStr = conn.getHeaderField(X_IMAGE_SIZE);
            if (imageSizeStr != null) {
                imageSize = Long.parseLong(imageSizeStr);
            }

            BufferedInputStream bin = new BufferedInputStream(conn.getInputStream());

            // Do not limit speed in client side.
            long bytes = IOUtils.copyBytes(bin, out, BUFFER_BYTES, CHECKPOINT_LIMIT_BYTES, true);

            if ((imageSize > 0) && (bytes != imageSize)) {
                throw new IOException("Unexpected image size, expected: " + imageSize + ", actual: " + bytes);
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

}
