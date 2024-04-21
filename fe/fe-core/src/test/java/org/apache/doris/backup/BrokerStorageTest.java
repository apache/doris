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

package org.apache.doris.backup;

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.fs.remote.BrokerFileSystem;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Ignore
public class BrokerStorageTest {
    private static String basePath;
    private final String bucket = "bos://yang-repo/";
    private final String brokerHost = "your_host";
    private Map<String, String> properties;

    @Tested
    private BrokerFileSystem fileSystem;
    private String testFile;
    private String content;
    private Pair<TPaloBrokerService.Client, TNetworkAddress> pair;
    @Mocked
    GenericPool pool;

    @BeforeClass
    public static void init() {
        basePath = "broker/" + UUID.randomUUID().toString();
    }

    @Before
    public void setUp() throws Exception {
        pair = Pair.of(null, null);
        TTransport transport = new TSocket(brokerHost, 8111);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        pair.first = new TPaloBrokerService.Client(protocol);
        pair.second = new TNetworkAddress(brokerHost, 8111);
        properties = new HashMap<>();
        properties.put("bos_accesskey",  System.getenv().getOrDefault("AWS_AK", ""));
        properties.put("bos_secret_accesskey",  System.getenv().getOrDefault("AWS_SK", ""));
        properties.put("bos_endpoint", "http://bj.bcebos.com");
        fileSystem = new BrokerFileSystem("bos_broker", properties);
        testFile = bucket + basePath + "/Ode_to_the_West_Wind";
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
        new MockUp<BrokerFileSystem>() {
            @Mock
            public Pair<TPaloBrokerService.Client, TNetworkAddress> getBroker() {
                return pair;
            }
        };
        new Expectations() {
            {
                pool.returnObject(withInstanceOf(TNetworkAddress.class), withInstanceOf(TServiceClient.class));
                minTimes = 0;
            }
        };
        Deencapsulation.setField(ClientPool.class, "brokerPool", pool);
        Assert.assertEquals(Status.OK, fileSystem.directUpload(content, testFile));
    }

    @Test
    public void downloadWithFileSize() throws IOException {
        File localFile = File.createTempFile("brokerunittest", ".dat");
        localFile.deleteOnExit();
        Status status = fileSystem.downloadWithFileSize(testFile, localFile.getAbsolutePath(), content.getBytes().length);
        Assert.assertEquals(Status.OK, status);
        Assert.assertEquals(DigestUtils.md5Hex(content.getBytes()), DigestUtils.md5Hex(new FileInputStream(localFile)));
        status = fileSystem.downloadWithFileSize(bucket + basePath + "/Ode_to_the_West_Wind", localFile.getAbsolutePath(), content.getBytes().length + 1);
        Assert.assertNotEquals(Status.OK, status);
    }

    @Test
    public void upload() throws IOException {
        File localFile = File.createTempFile("brokerunittest", ".dat");
        localFile.deleteOnExit();
        OutputStream os = new FileOutputStream(localFile);
        byte[] buf = new byte[1024 * 1024];
        Random r = new Random();
        r.nextBytes(buf);
        os.write(buf);
        os.close();
        String remote = bucket + basePath + "/" + localFile.getName();
        Status status = fileSystem.upload(localFile.getAbsolutePath(), remote);
        Assert.assertEquals(Status.OK, status);
        File localFile2 = File.createTempFile("brokerunittest", ".dat");
        localFile2.deleteOnExit();
        status = fileSystem.downloadWithFileSize(remote, localFile2.getAbsolutePath(), 1024 * 1024);
        Assert.assertEquals(Status.OK, status);
        Assert.assertEquals(DigestUtils.md5Hex(new FileInputStream(localFile)),
                DigestUtils.md5Hex(new FileInputStream(localFile2)));
    }

    @Test
    public void rename() {
        Assert.assertEquals(Status.OK, fileSystem.directUpload(content, testFile + ".bak"));
        fileSystem.rename(testFile + ".bak", testFile + ".bak1");
        Assert.assertEquals(Status.OK, fileSystem.exists(testFile + ".bak1"));
    }

    @Test
    public void delete() {
        String deleteFile = testFile + ".to_be_delete";
        Assert.assertEquals(Status.OK, fileSystem.delete(deleteFile + "xxxx"));
        Assert.assertEquals(Status.OK, fileSystem.directUpload(content, deleteFile));
        Assert.assertEquals(Status.OK, fileSystem.delete(deleteFile));
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, fileSystem.exists(deleteFile).getErrCode());
        Assert.assertEquals(Status.OK, fileSystem.delete(deleteFile + "xxxx"));
    }

    @Test
    public void list() {
        List<RemoteFile> result = new ArrayList<>();
        String listPath =  bucket + basePath + "_list" + "/Ode_to_the_West_Wind";
        Assert.assertEquals(Status.OK, fileSystem.directUpload(content, listPath + ".1"));
        Assert.assertEquals(Status.OK, fileSystem.directUpload(content, listPath + ".2"));
        Assert.assertEquals(Status.OK, fileSystem.directUpload(content, listPath + ".3"));
        Assert.assertEquals(Status.OK, fileSystem.globList(bucket + basePath  + "_list/*", result));
        Assert.assertEquals(3, result.size());
    }

    @Test
    public void exists() {
        Status status = fileSystem.exists(testFile);
        Assert.assertEquals(Status.OK, status);
        status = fileSystem.exists(testFile + ".NOT_EXIST");
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, status.getErrCode());
    }
}
