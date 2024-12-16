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

package org.apache.doris.cloud.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.stage.StageUtil;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CopyJobTest {
    private static final Logger LOG = LogManager.getLogger(CopyJobTest.class);
    private static final String fileName = "./CopyJobTest";

    private static FakeEditLog fakeEditLog;
    private static FakeEnv fakeEnv;
    private static Env masterEnv;

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        masterEnv = CatalogTestUtil.createTestCatalog();
        fakeEnv.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
    }

    @Test
    public void testParseLoadFiles() {
        Config.cloud_delete_loaded_internal_stage_files = true;

        String bucket = "";
        List<String> stagePrefixes = Lists.newArrayList("instance1/stage/root/root",
                "org1/instance1/stage/bob/8f3b7371-c096-48ef-b81c-0eb951dd0f52", "stage/root/root");
        for (String stagePrefix : stagePrefixes) {
            Pair<List<String>, List<String>> files = generateLoadFiles(bucket, stagePrefix);
            Assert.assertEquals(files.second, StageUtil.parseLoadFiles(files.first, bucket, stagePrefix));
        }

        Assert.assertNull(StageUtil.parseLoadFiles(null, bucket, stagePrefixes.get(0)));

        Assert.assertNull(StageUtil.parseLoadFiles(new ArrayList<>(), bucket, "instance1/data/dbId"));

        Config.cloud_delete_loaded_internal_stage_files = false;
        Pair<List<String>, List<String>> files = generateLoadFiles(bucket, stagePrefixes.get(0));
        Assert.assertNull(StageUtil.parseLoadFiles(files.first, bucket, stagePrefixes.get(0)));
    }

    private Pair<List<String>, List<String>> generateLoadFiles(String bucket, String stagePrefix) {
        List<String> loadFiles = new ArrayList<>();
        List<String> parseFiles = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            loadFiles.add("s3://" + bucket + "/" + stagePrefix + "/load" + i);
            parseFiles.add(stagePrefix + "/load" + i);
        }
        for (int i = 0; i < 10; i++) {
            loadFiles.add("s3://" + bucket + "/" + stagePrefix + "/load1/load" + i);
            parseFiles.add(stagePrefix + "/load1/load" + i);
        }
        return Pair.of(loadFiles, parseFiles);
    }

    @Test
    public void testSerialization() throws IOException, MetaNotFoundException {
        File file = new File(fileName);
        file.createNewFile();

        long dbId = CatalogTestUtil.testDbId1;
        String label = "copy_test";
        String userName = "admin";
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        BrokerDesc brokerDesc = new BrokerDesc("test_broker", null);
        OriginStatement originStmt = new OriginStatement("test_sql", 0);
        UserIdentity userIdent = UserIdentity.ROOT;
        String stageId = "test_stage";
        String stagePrefix = "test_prefix";
        String stagePattern = "test_pattern";
        ObjectInfo objectInfo = new ObjectInfo(Provider.OSS, "test_ak", "test_sk", "test_bucket", "test_endpoint", "test_region",
                            "test_prefix");

        LoadJob copyJob1 = new CopyJob(dbId, label, queryId,
                        brokerDesc, originStmt, userIdent, stageId,
                        StageType.INTERNAL, stagePrefix, 112131231, stagePattern,
                        objectInfo, true, userName);

        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        copyJob1.write(out);
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        LoadJob copyJob2 = LoadJob.read(in);
        Assert.assertEquals(copyJob1.getDbId(), copyJob2.getDbId());
        in.close();
        file.delete();
        Assert.assertEquals(copyJob1.getDbId(), copyJob2.getDbId());
    }
}
