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

import mockit.Expectations;
import org.apache.doris.alter.AlterJob;
import org.apache.doris.alter.AlterJob.JobType;
import org.apache.doris.alter.SchemaChangeJob;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.FeConstants;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadJob;
import org.apache.doris.meta.MetaContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class CatalogTest {

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
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        Catalog catalog = Catalog.getCurrentCatalog();
        MetaContext.get().setMetaVersion(FeConstants.meta_version);
        Field field = catalog.getClass().getDeclaredField("load");
        field.setAccessible(true);
        field.set(catalog, new Load());

        long checksum1 = catalog.saveHeader(dos, new Random().nextLong(), 0);
        catalog.clear();
        catalog = null;
        dos.close();
        
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        catalog = Catalog.getCurrentCatalog();
        long checksum2 = catalog.loadHeader(dis, 0);
        Assert.assertEquals(checksum1, checksum2);
        dis.close();
        
        deleteDir(dir);
    }
    
    @Test
    public void testSaveLoadJob() throws Exception {
        String dir = "testLoadLoadJob";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        Catalog catalog = Catalog.getCurrentCatalog();
        MetaContext.get().setMetaVersion(FeConstants.meta_version);
        Field field = catalog.getClass().getDeclaredField("load");
        field.setAccessible(true);
        field.set(catalog, new Load());

        LoadJob job1 = new LoadJob("label1", 20, 0);
        catalog.getLoadInstance().unprotectAddLoadJob(job1, true);
        long checksum1 = catalog.saveLoadJob(dos, 0);
        catalog.clear();
        catalog = null;
        dos.close();
        
        catalog = Catalog.getCurrentCatalog();

        Field field2 = catalog.getClass().getDeclaredField("load");
        field2.setAccessible(true);
        field2.set(catalog, new Load());

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        long checksum2 = catalog.loadLoadJob(dis, 0);
        Assert.assertEquals(checksum1, checksum2);
        LoadJob job2 = catalog.getLoadInstance().getLoadJob(-1);
        Assert.assertTrue(job1.equals(job2));
        dis.close();
        
        deleteDir(dir);
    }
    @Test
    public void testSaveLoadSchemaChangeJob() throws Exception {
        String dir = "testLoadSchemaChangeJob";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        Catalog catalog = Catalog.getCurrentCatalog();
        MetaContext.get().setMetaVersion(FeConstants.meta_version);
        Field field = catalog.getClass().getDeclaredField("load");
        field.setAccessible(true);
        field.set(catalog, new Load());

        Database db1 = new Database(10000L, "testCluster.db1");
        db1.setClusterName("testCluster");
        final Cluster cluster = new Cluster("testCluster", 10001L);
        MaterializedIndex baseIndex = new MaterializedIndex(20000L, IndexState.NORMAL);
        Partition partition = new Partition(2000L, "single", baseIndex, new RandomDistributionInfo(10));
        List<Column> baseSchema = new LinkedList<Column>();
        OlapTable table = new OlapTable(2L, "base", baseSchema, KeysType.AGG_KEYS,
                                        new SinglePartitionInfo(), new RandomDistributionInfo(10));
        table.addPartition(partition);
        db1.createTable(table);

        catalog.addCluster(cluster);
        catalog.unprotectCreateDb(db1);
        SchemaChangeJob job1 = new SchemaChangeJob(db1.getId(), table.getId(), null, table.getName(), -1);
        
        catalog.getSchemaChangeHandler().replayInitJob(job1, catalog);
        long checksum1 = catalog.saveAlterJob(dos, 0, JobType.SCHEMA_CHANGE);
        catalog.clear();
        catalog = null;
        dos.close();
        
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        catalog = Catalog.getCurrentCatalog();
        long checksum2 = catalog.loadAlterJob(dis, 0, JobType.SCHEMA_CHANGE);
        Assert.assertEquals(checksum1, checksum2);
        Map<Long, AlterJob> map = catalog.getSchemaChangeHandler().unprotectedGetAlterJobs();
        Assert.assertEquals(1, map.size());
        SchemaChangeJob job2 = (SchemaChangeJob) map.get(table.getId());
        Assert.assertTrue(job1.equals(job2));
        dis.close();
        
        deleteDir(dir);
    }
}
