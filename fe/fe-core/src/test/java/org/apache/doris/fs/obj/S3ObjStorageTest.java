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

import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3ObjStorageTest {
    private S3ObjStorage storage;

    private MockedS3Client mockedClient;

    @BeforeAll
    public void beforeAll() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", "s3.e.c");
        properties.put("s3.access_key", "abc");
        properties.put("s3.secret_key", "123");
        storage = new S3ObjStorage(properties);
        Field client = storage.getClass().getDeclaredField("client");
        client.setAccessible(true);
        mockedClient = new MockedS3Client();
        client.set(storage, mockedClient);
        Assertions.assertTrue(storage.getClient("mocked") instanceof MockedS3Client);
    }

    @Test
    public void testBaseOp() throws UserException {
        S3URI vUri = S3URI.create("s3://bucket/key", true);
        S3URI uri = S3URI.create("s3://bucket/key", false);
        Assertions.assertEquals(vUri.getVirtualBucket(), "bucket");
        Assertions.assertEquals(vUri.getBucket(), "key");
        Assertions.assertEquals(uri.getVirtualBucket(), "");
        Assertions.assertEquals(uri.getBucket(), "bucket");

        Status st = storage.headObject("s3://bucket/key");
        Assertions.assertEquals(Status.OK, st);

        mockedClient.setMockedData(new byte[0]);
        st = storage.getObject("s3://bucket/key", new File("/mocked/file"));
        Assertions.assertEquals(Status.OK, st);

        for (int i = 0; i < 5; i++) {
            st = storage.putObject("s3://bucket/keys/key" + i,  RequestBody.fromString("mocked"));
            Assertions.assertEquals(Status.OK, st);
        }
        st = storage.copyObject("s3://bucket/key", "s3://bucket/key1");
        Assertions.assertEquals(Status.OK, st);

        st = storage.deleteObject("s3://bucket/key");
        Assertions.assertEquals(Status.OK, st);

        RemoteObjects remoteObjects = storage.listObjects("s3://bucket/keys", null);
        Assertions.assertEquals(5, remoteObjects.getObjectList().size());
        Assertions.assertTrue(remoteObjects.isTruncated());
        Assertions.assertEquals("next-token", remoteObjects.getContinuationToken());

        List<RemoteObject> objectList = remoteObjects.getObjectList();
        for (int i = 0; i < objectList.size(); i++) {
            RemoteObject remoteObject = objectList.get(i);
            Assertions.assertEquals("key" + i, remoteObject.getRelativePath());
        }

        storage.properties.put("use_path_style", "false");
        storage.properties.put("s3.endpoint", "oss.a.c");
        storage.setProperties(storage.properties);
        RemoteObjects remoteObjectsVBucket = storage.listObjects("oss://bucket/keys", null);
        List<RemoteObject> list = remoteObjectsVBucket.getObjectList();
        for (int i = 0; i < list.size(); i++) {
            RemoteObject remoteObject = list.get(i);
            Assertions.assertTrue(remoteObject.getRelativePath().startsWith("keys/key" + i));
        }
    }
}
