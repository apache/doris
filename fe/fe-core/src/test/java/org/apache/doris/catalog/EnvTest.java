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

package org.apache.doris.catalog;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.alter.SchemaChangeJobV2;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.meta.MetaHeader;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class EnvTest {

    @Before
    public void setUp() {
        MetaContext metaContext = new MetaContext();
        new Expectations(metaContext) {
            {
                MetaContext.get();
                minTimes = 0;
                result = metaContext;
            }
        };
    }

    public void mkdir(String dirString) {
        File dir = new File(dirString);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void addFiles(int image, int edit, String metaDir) {
        File imageFile = new File(metaDir + "image." + image);
        try {
            imageFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 1; i <= edit; i++) {
            File editFile = new File(metaDir + "edits." + i);
            try {
                editFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File current = new File(metaDir + "edits");
        try {
            current.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File version = new File(metaDir + "VERSION");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2015\n";
            String line2 = "clusterId=966271669";
            FileWriter fw = new FileWriter(version);
            fw.write(line1);
            fw.write(line2);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteDir(String metaDir) {
        File dir = new File(metaDir);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    @Test
    public void testSaveLoadHeader() throws Exception {
        String dir = "testLoadHeader";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        CountingDataOutputStream dos = new CountingDataOutputStream(new FileOutputStream(file));
        Env env = Env.getCurrentEnv();
        MetaContext.get().setMetaVersion(FeConstants.meta_version);

        long checksum1 = env.saveHeader(dos, new Random().nextLong(), 0);
        env.clear();
        env = null;
        dos.close();

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        env = Env.getCurrentEnv();
        long checksum2 = env.loadHeader(dis, MetaHeader.EMPTY_HEADER, 0);
        Assert.assertEquals(checksum1, checksum2);
        dis.close();

        deleteDir(dir);
    }

    @Test
    public void testOnEraseOlapTableCleansAlterJobs(@Mocked InternalCatalog internalCatalog,
                                                    @Mocked TabletInvertedIndex invertedIndex,
                                                    @Mocked ColocateTableIndex colocateTableIndex,
                                                    @Mocked OlapTable olapTable) {
        Env env = new Env(true);
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        long erasedTableId = 10001L;
        AlterJobV2 mvTargetJob = new SchemaChangeJobV2("sql", 1L, 1L, erasedTableId, "t1", 1000L);
        AlterJobV2 mvOtherJob = new SchemaChangeJobV2("sql", 2L, 1L, 20002L, "t2", 1000L);
        AlterJobV2 scTargetJob = new SchemaChangeJobV2("sql", 3L, 1L, erasedTableId, "t1", 1000L);
        AlterJobV2 scOtherJob = new SchemaChangeJobV2("sql", 4L, 1L, 30003L, "t3", 1000L);

        Map<Long, AlterJobV2> mvJobs = materializedViewHandler.getAlterJobsV2();
        Map<Long, AlterJobV2> scJobs = schemaChangeHandler.getAlterJobsV2();
        mvJobs.put(mvTargetJob.getJobId(), mvTargetJob);
        mvJobs.put(mvOtherJob.getJobId(), mvOtherJob);
        scJobs.put(scTargetJob.getJobId(), scTargetJob);
        scJobs.put(scOtherJob.getJobId(), scOtherJob);

        new MockUp<Env>() {
            @Mock
            public static TabletInvertedIndex getCurrentInvertedIndex() {
                return invertedIndex;
            }

            @Mock
            public static ColocateTableIndex getCurrentColocateIndex() {
                return colocateTableIndex;
            }
        };

        new Expectations(env, olapTable) {
            {
                invertedIndex.getTabletMetaMap();
                result = new ConcurrentHashMap<>();
                minTimes = 0;

                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;

                env.getMaterializedViewHandler();
                result = materializedViewHandler;
                minTimes = 0;

                env.getSchemaChangeHandler();
                result = schemaChangeHandler;
                minTimes = 0;

                olapTable.getId();
                result = erasedTableId;
                minTimes = 0;

                olapTable.getName();
                result = "test_tbl";
                minTimes = 0;

                olapTable.getAllPartitions();
                result = Collections.emptyList();
                minTimes = 0;
            }
        };

        env.onEraseOlapTable(1L, olapTable, false);

        Assert.assertEquals(1, mvJobs.size());
        Assert.assertTrue(mvJobs.containsValue(mvOtherJob));
        Assert.assertFalse(mvJobs.containsValue(mvTargetJob));

        Assert.assertEquals(1, scJobs.size());
        Assert.assertTrue(scJobs.containsValue(scOtherJob));
        Assert.assertFalse(scJobs.containsValue(scTargetJob));

        new Verifications() {
            {
                internalCatalog.eraseTableDropBackendReplicas(1L, olapTable, false);
                times = 1;
                colocateTableIndex.removeTable(erasedTableId);
                times = 1;
            }
        };
    }
}
