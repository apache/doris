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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.HttpURLUtil;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.persist.StorageInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class MetaHelperTest {

    @Test
    public void testDownloadAndComplete(@TempDir File dir) throws Exception {
        byte[] image = "synthetic image content".getBytes(StandardCharsets.UTF_8);
        File downloaded = downloadAndComplete(dir, image, DigestUtils.md5Hex(image));

        Assertions.assertArrayEquals(image, Files.readAllBytes(downloaded.toPath()));
        Assertions.assertFalse(new File(dir, "image.1.part").exists());
    }

    @Test
    public void testDownloadRejectsMd5Mismatch(@TempDir File dir) {
        byte[] image = "synthetic image content".getBytes(StandardCharsets.UTF_8);
        IOException e = Assertions.assertThrows(IOException.class,
                () -> downloadAndComplete(dir, image, DigestUtils.md5Hex("other content")));
        Assertions.assertTrue(e.getMessage().contains("Unexpected image md5"), e.getMessage());
        Assertions.assertFalse(new File(dir, "image.1").exists());
    }

    @Test
    public void testSyncDirectory(@TempDir File dir) throws IOException {
        MetaHelper.syncDirectory(dir);
        Assertions.assertThrows(IOException.class, () -> MetaHelper.syncDirectory(new File(dir, "missing")));
    }

    private File downloadAndComplete(File dir, byte[] image, String md5) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/image", exchange -> {
            exchange.getResponseHeaders().add(MetaHelper.X_IMAGE_SIZE, String.valueOf(image.length));
            exchange.getResponseHeaders().add(MetaHelper.X_IMAGE_MD5, md5);
            exchange.sendResponseHeaders(200, image.length);
            try (OutputStream body = exchange.getResponseBody()) {
                body.write(image);
            }
        });
        server.start();
        String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/image?version=1";
        try (MockedStatic<HttpURLUtil> httpUtil = Mockito.mockStatic(HttpURLUtil.class)) {
            httpUtil.when(() -> HttpURLUtil.getConnectionWithNodeIdent(url))
                    .thenAnswer(invocation -> (HttpURLConnection) new URL(url).openConnection());
            MetaHelper.getRemoteFile(url, 10_000, MetaHelper.getFile("image.1", dir));
            return MetaHelper.complete("image.1", dir);
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void test() throws JsonProcessingException {
        ResponseBody<StorageInfo> bodyBefore = buildResponseBody();
        ObjectMapper mapper = new ObjectMapper();
        String bodyStr = mapper.writeValueAsString(bodyBefore);
        ResponseBody<StorageInfo> bodyAfter = MetaHelper.parseResponse(bodyStr, StorageInfo.class);
        Assertions.assertEquals(bodyAfter, bodyBefore);
    }

    private ResponseBody<StorageInfo> buildResponseBody() {
        StorageInfo infoBefore = new StorageInfo();
        infoBefore.setClusterID(1);
        infoBefore.setEditsSeq(2L);
        infoBefore.setImageSeq(3L);
        ResponseBody<StorageInfo> bodyBefore = new ResponseBody<>();
        bodyBefore.setCode(RestApiStatusCode.UNAUTHORIZED.code);
        bodyBefore.setCount(5);
        bodyBefore.setData(infoBefore);
        bodyBefore.setMsg("msg");
        return bodyBefore;
    }

    File tempDir = new File(System.getProperty("java.io.tmpdir"), "tempDir");

    @BeforeEach
    void setUp() {

        if (tempDir.exists()) {
            tempDir.delete();
        }
        tempDir.mkdir();
    }

    @Test
    public void testFile() throws IOException {

        String errorFilename = "..testfile.";
        File errorFileWithSuffix = new File(tempDir, errorFilename);
        String rightFilename = "image.1";
        File rightFileWithSuffix = new File(tempDir, rightFilename);

        Config.meta_helper_security_mode = true;

        if (errorFileWithSuffix.exists()) {
            errorFileWithSuffix.delete();
        }
        Assertions.assertThrows(Exception.class, () -> MetaHelper.complete(errorFilename, tempDir));
        Assertions.assertThrows(Exception.class, () -> MetaHelper.getFile(errorFilename, tempDir));
        if (rightFileWithSuffix.exists()) {
            rightFileWithSuffix.delete();
        }
        Assertions.assertEquals(rightFileWithSuffix.getName() + ".part", MetaHelper.getFile(rightFilename, tempDir).getName());

    }

    @Test
    public void testFileNameCheck() {
        Config.meta_helper_security_mode = true;
        MetaHelper.checkIsValidFileName("VERSION");
        MetaHelper.checkIsValidFileName("image.1");
        MetaHelper.checkIsValidFileName("image.1.part");
        MetaHelper.checkIsValidFileName("image.1.part.1");
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetaHelper.checkIsValidFileName("../testfile."));


    }

    @AfterEach
    public void tearDown() {
        if (tempDir.exists()) {
            tempDir.delete();
        }
    }

}
