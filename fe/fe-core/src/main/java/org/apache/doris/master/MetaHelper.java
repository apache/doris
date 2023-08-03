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
import org.apache.doris.httpv2.util.Md5Util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetaHelper {
    private static final String PART_SUFFIX = ".part";
    public static final String X_IMAGE_SIZE = "X-Image-Size";
    public static final String X_MD5 = "X-MD5";
    private static final int BUFFER_BYTES = 8 * 1024;
    private static final int CHECKPOINT_LIMIT_BYTES = 30 * 1024 * 1024;
    private static final Logger LOG = LogManager.getLogger(MetaHelper.class);

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

    // download file from remote node
    public static void getRemoteFile(String urlStr, int timeout, OutputStream out)
            throws IOException {
        getRemoteFileAndReturnMd5(urlStr, timeout, out);
    }

    // download file from remote node and get md5 from header
    public static String getRemoteFileAndReturnMd5(String urlStr, int timeout, OutputStream out) throws IOException {
        HttpURLConnection conn = null;
        String md5Sum = null;
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

            BufferedInputStream bin = new BufferedInputStream(conn.getInputStream());

            // Do not limit speed in client side.
            long bytes = IOUtils.copyBytes(bin, out, BUFFER_BYTES, CHECKPOINT_LIMIT_BYTES, true);

            if ((imageSize > 0) && (bytes != imageSize)) {
                throw new IOException("Unexpected image size, expected: " + imageSize + ", actual: " + bytes);
            }

            if (Config.enable_image_md5_check) {
                md5Sum = conn.getHeaderField(X_MD5);
            }
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
            if (out != null) {
                out.close();
            }
        }
        return md5Sum;
    }

    /**
     * Check image file md5sum between master and non-master fe when do checkpoint
     * @param masterImageMd5 master fe md5 get in http header
     * @param fileName non-master fe image filename
     * @param dir non-master fe image dir
     * @throws IOException
     */
    public static void checkMd5WithMaster(String masterImageMd5, String fileName, File dir) throws IOException {
        if (StringUtils.isEmpty(masterImageMd5)) {
            return;
        }
        File newFile = new File(dir, fileName);
        String newFileMd5 = Md5Util.getMd5String(newFile);
        if (!masterImageMd5.equals(newFileMd5)) {
            LOG.error("The Master image md5 is different with copied image md5, fileName is: {}, "
                    + "master md5 is: {}, copied md5 is: {}", fileName, masterImageMd5, newFileMd5);
            throw new IOException("The Master image md5 is different with copied image md5, fileName is:"
                + fileName + ", master md5 is: " + masterImageMd5 + ", copied md5 is :" + newFileMd5);
        }
        LOG.info("The Master image md5 is same as copied image md5, fileName is :{} , md5 is: {}",
                fileName, newFileMd5);
    }
}
