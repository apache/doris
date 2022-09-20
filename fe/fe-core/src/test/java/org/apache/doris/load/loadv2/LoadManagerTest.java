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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.meta.MetaContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class LoadManagerTest {
    private LoadManager loadManager;
    private final String fieldName = "idToLoadJob";

    @Test
    public void testCreateHadoopJob(@Injectable LoadStmt stmt, @Injectable LabelName labelName, @Mocked Env env,
            @Mocked InternalCatalog catalog, @Injectable Database database, @Injectable BrokerLoadJob brokerLoadJob) {
        Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Maps.newHashMap();
        Map<String, List<LoadJob>> labelToLoadJobs = Maps.newHashMap();
        String label1 = "label1";
        List<LoadJob> loadJobs = Lists.newArrayList();
        loadJobs.add(brokerLoadJob);
        labelToLoadJobs.put(label1, loadJobs);
        dbIdToLabelToLoadJobs.put(1L, labelToLoadJobs);
        LoadJobScheduler loadJobScheduler = new LoadJobScheduler();
        loadManager = new LoadManager(loadJobScheduler);
        Deencapsulation.setField(loadManager, "dbIdToLabelToLoadJobs", dbIdToLabelToLoadJobs);
        new Expectations() {
            {
                stmt.getLabel();
                minTimes = 0;
                result = labelName;
                labelName.getLabelName();
                minTimes = 0;
                result = "label1";
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyString);
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = 1L;
            }
        };

        try {
            loadManager.createLoadJobV1FromStmt(stmt, EtlJobType.HADOOP, System.currentTimeMillis());
            Assert.fail("duplicated label is not be allowed");
        } catch (LabelAlreadyUsedException e) {
            // successful
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testSerializationNormal(@Mocked Env env, @Mocked InternalCatalog catalog, @Injectable Database database,
            @Injectable Table table) throws Exception {
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = "tablename";
                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_CURRENT;
            }
        };

        final Path path = Files.createTempFile("LoadManagerTest", "tmp");

        loadManager = new LoadManager(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, 1L, System.currentTimeMillis(), "", "");
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);

        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));
        loadManager.write(dos);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        LoadManager newLoadManager = new LoadManager(new LoadJobScheduler());
        newLoadManager.readFields(dis);

        Map<Long, LoadJob> loadJobs = Deencapsulation.getField(loadManager, fieldName);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);
        Assert.assertEquals(loadJobs, newLoadJobs);
        dis.close();
        Files.deleteIfExists(path);
    }

    @Test
    public void testSerializationWithJobRemoved(@Mocked MetaContext metaContext, @Mocked Env env,
            @Mocked InternalCatalog catalog, @Injectable Database database, @Injectable Table table) throws Exception {
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = database;
                database.getTableNullable(anyLong);
                minTimes = 0;
                result = table;
                table.getName();
                minTimes = 0;
                result = "tablename";
                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeMetaVersion.VERSION_CURRENT;
            }
        };

        loadManager = new LoadManager(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, 1L, System.currentTimeMillis(), "", "");
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);

        final Path path = Files.createTempFile("LoadManagerTest", "tmp");

        //make job1 don't serialize
        Config.streaming_label_keep_max_second = 1;
        Thread.sleep(2000);
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));
        loadManager.write(dos);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        LoadManager newLoadManager = new LoadManager(new LoadJobScheduler());
        newLoadManager.readFields(dis);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.getField(newLoadManager, fieldName);

        Assert.assertEquals(0, newLoadJobs.size());
        dis.close();
        Files.deleteIfExists(path);
    }
}
