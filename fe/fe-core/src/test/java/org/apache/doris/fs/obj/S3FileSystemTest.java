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

package org.apache.doris.fs.obj;

import org.apache.doris.backup.Repository;
import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.S3FileSystem;

import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class S3FileSystemTest {
    private static String basePath;
    private final String bucket = "s3://doris-test/";
    private Map<String, String> properties;
    private S3FileSystem fileSystem;
    private MockedS3Client mockedClient;
    private String testFile;
    private String content;
    // we use mocked s3 client to test s3 file system by default.
    private boolean injectMockedClient = true;

    @BeforeAll
    public static void init() {
        basePath = "s3/" + UUID.randomUUID();
    }

    @BeforeEach
    public void setUp() throws Exception {
        properties = new HashMap<>();
        properties.put("AWS_ACCESS_KEY", System.getenv().getOrDefault("AWS_AK", ""));
        properties.put("AWS_SECRET_KEY", System.getenv().getOrDefault("AWS_SK", ""));
        properties.put("AWS_ENDPOINT", "http://s3.bj.bcebos.com");
        properties.put(PropertyConverter.USE_PATH_STYLE, "false");
        properties.put("AWS_REGION", "bj");
        content =
                "O wild West Wind, thou breath of Autumn's being\n"
                        + "Thou, from whose unseen presence the leaves dead\n"
                        + "Are driven, like ghosts from an enchanter fleeing,\n"
                        + "Yellow, and black, and pale, and hectic red,\n"
                        + "Pestilence-stricken multitudes:O thou\n"
                        + "Who chariotest to their dark wintry bed\n"
                        + "The winged seeds, where they lie cold and low,\n"
                        + "Each like a corpse within its grave, until\n"
                        + "Thine azure sister of the Spring shall blow\n"
                        + "Her clarion o'er the dreaming earth, and fill\n"
                        + "(Driving sweet buds like flocks to feed in air)\n"
                        + "With living hues and odors plain and hill:\n"
                        + "Wild Spirit, which art moving everywhere;\n"
                        + "Destroyer and preserver; hear, oh, hear!";

        if (injectMockedClient) {
            properties.put("AWS_ACCESS_KEY", "ak");
            properties.put("AWS_SECRET_KEY", "sk");
            // create storage
            mockedClient = new MockedS3Client();
            mockedClient.setCanMakeData(true);
            mockedClient.setMockedData(content.getBytes());
            new MockUp<S3ObjStorage>(S3ObjStorage.class) {
                @Mock
                public S3Client getClient() throws UserException {
                    return mockedClient;
                }
            };
            S3ObjStorage mockedStorage = new S3ObjStorage(properties);
            Assertions.assertTrue(mockedStorage.getClient() instanceof MockedS3Client);
            // inject storage to file system.
            fileSystem = new S3FileSystem(mockedStorage);
            new MockUp<S3FileSystem>(S3FileSystem.class) {
                @Mock
                public Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
                    try {
                        S3URI uri = S3URI.create(remotePath, false);
                        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(uri.getBucket());
                        ListObjectsV2Response response = mockedClient.listObjectsV2(requestBuilder.build());
                        for (S3Object c : response.contents()) {
                            result.add(new RemoteFile(c.key(), true, c.size(), 0));
                        }
                    } catch (UserException e) {
                        throw new RuntimeException(e);
                    }
                    return Status.OK;
                }
            };
        } else {
            // can also real file system to test.
            fileSystem = (S3FileSystem) FileSystemFactory.getS3FileSystem(properties);
        }
        testFile = bucket + basePath + "/Ode_to_the_West_Wind";
        Assertions.assertEquals(Status.OK, fileSystem.directUpload(content, testFile));
    }

    @Test
    public void downloadWithFileSize() throws IOException {
        File localFile = File.createTempFile("s3unittest", ".dat");
        localFile.deleteOnExit();
        Status status = fileSystem.downloadWithFileSize(testFile, localFile.getAbsolutePath(), content.getBytes().length);
        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(DigestUtils.md5Hex(content.getBytes()),
                    DigestUtils.md5Hex(Files.newInputStream(localFile.toPath())));
        status = fileSystem.downloadWithFileSize(bucket + basePath + "/Ode_to_the_West_Wind", localFile.getAbsolutePath(), content.getBytes().length + 1);
        Assertions.assertNotEquals(Status.OK, status);
    }

    @Test
    public void upload() throws IOException {
        File localFile = File.createTempFile("s3unittest", ".dat");
        localFile.deleteOnExit();
        OutputStream os = Files.newOutputStream(localFile.toPath());
        byte[] buf = new byte[1024 * 1024];
        Random r = new Random();
        r.nextBytes(buf);
        os.write(buf);
        os.close();
        if (injectMockedClient) {
            mockedClient.setMockedData(buf);
        }
        String remote = bucket + basePath + "/" + localFile.getName();
        Status status = fileSystem.upload(localFile.getAbsolutePath(), remote);
        Assertions.assertEquals(Status.OK, status);
        File localFile2 = File.createTempFile("s3unittest", ".dat");
        localFile2.deleteOnExit();
        status = fileSystem.downloadWithFileSize(remote, localFile2.getAbsolutePath(), 1024 * 1024);
        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(DigestUtils.md5Hex(Files.newInputStream(localFile.toPath())),
                DigestUtils.md5Hex(Files.newInputStream(localFile2.toPath())));
    }

    @Test
    public void testRepositoryUpload() throws IOException {
        Repository repo = new Repository(10000, "repo", false, bucket + basePath, fileSystem);
        File localFile = File.createTempFile("s3unittest", ".dat");
        localFile.deleteOnExit();
        String remote = bucket + basePath + "/" + localFile.getName();
        Status status = repo.upload(localFile.getAbsolutePath(), remote);
        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    public void copy() {
        Assertions.assertEquals(Status.OK, fileSystem.copy(testFile, testFile + ".bak"));
        Assertions.assertEquals(Status.OK, fileSystem.exists(testFile + ".bak"));
        if (!injectMockedClient) {
            Assertions.assertNotEquals(Status.OK, fileSystem.copy(testFile + ".bakxxx", testFile + ".bak"));
        }
    }

    @Test
    public void rename() {
        Assertions.assertEquals(Status.OK, fileSystem.directUpload(content, testFile + ".bak"));
        fileSystem.rename(testFile + ".bak", testFile + ".bak1");
        if (!injectMockedClient) {
            Assertions.assertEquals(Status.ErrCode.NOT_FOUND, fileSystem.exists(testFile + ".bak").getErrCode());
        }
        Assertions.assertEquals(Status.OK, fileSystem.exists(testFile + ".bak1"));
    }

    @Test
    public void checkPathExist() {
        Status status = fileSystem.exists(testFile);
        Assertions.assertEquals(Status.OK, status);
        status = fileSystem.exists(testFile + ".NOT_EXIST");
        if (!injectMockedClient) {
            Assertions.assertEquals(Status.ErrCode.NOT_FOUND, status.getErrCode());
        }
    }

    @Test
    public void makeDir() {
        String path = bucket + basePath + "/test_path";
        Assertions.assertEquals(Status.OK, fileSystem.makeDir(path));
        if (!injectMockedClient) {
            Assertions.assertNotEquals(Status.OK, fileSystem.exists(path));
        }
        String path1 = bucket + basePath + "/test_path1/";
        Assertions.assertEquals(Status.OK, fileSystem.makeDir(path1));
        Assertions.assertEquals(Status.OK, fileSystem.exists(path1));
    }

    @Test
    public void list() {
        List<RemoteFile> result = new ArrayList<>();
        String listPath = bucket + basePath + "_list" + "/Ode_to_the_West_Wind";
        Assertions.assertEquals(Status.OK, fileSystem.delete(testFile));
        Assertions.assertEquals(Status.OK, fileSystem.directUpload(content, listPath + ".1"));
        Assertions.assertEquals(Status.OK, fileSystem.directUpload(content, listPath + ".2"));
        Assertions.assertEquals(Status.OK, fileSystem.directUpload(content, listPath + ".3"));
        Assertions.assertEquals(Status.OK, fileSystem.globList(bucket + basePath + "_list/*", result));
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void delete() {
        String deleteFile = testFile + ".to_be_delete";
        Assertions.assertEquals(Status.OK, fileSystem.directUpload(content, deleteFile));
        Assertions.assertEquals(Status.OK, fileSystem.delete(deleteFile));
        if (!injectMockedClient) {
            Assertions.assertEquals(Status.ErrCode.NOT_FOUND, fileSystem.exists(deleteFile).getErrCode());
        }
        Assertions.assertEquals(Status.OK, fileSystem.delete(deleteFile + "xxxx"));
    }
}
