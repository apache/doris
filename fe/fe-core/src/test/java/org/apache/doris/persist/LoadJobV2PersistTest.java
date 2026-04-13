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

package org.apache.doris.persist;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;

public class LoadJobV2PersistTest {
    private BrokerLoadJob createJob() throws Exception {
        String loadStmt = "";
        BrokerDesc brokerDesc = new BrokerDesc("bos", Maps.newHashMap());
        OriginStatement originStatement = new OriginStatement(loadStmt, 0);
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob(1L, "label", brokerDesc, originStatement,
                UserIdentity.ADMIN);
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(LoadCommand.LOAD_PARALLELISM, "5");
        brokerLoadJob.setJobProperties(jobProperties);
        return brokerLoadJob;
    }

    @Test
    public void testBrokerLoadJob() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            envStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeMetaVersion.VERSION_CURRENT);

            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(database);
            Mockito.when(catalog.getDbOrMetaException(Mockito.anyLong())).thenReturn(database);
            Mockito.when(database.getTableNullable(Mockito.anyLong())).thenReturn(table);
            Mockito.when(database.getFullName()).thenReturn("testDb");
            Mockito.when(table.getName()).thenReturn("tablename");

            // 1. Write objects to file
            File file = new File("./testBrokerLoadJob");
            file.createNewFile();
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

            BrokerLoadJob job = createJob();
            Assert.assertEquals(5, job.getLoadParallelism());
            job.write(dos);

            dos.flush();
            dos.close();

            // 2. Read objects from file
            DataInputStream dis = new DataInputStream(new FileInputStream(file));

            BrokerLoadJob rJob = (BrokerLoadJob) BrokerLoadJob.read(dis);
            Assert.assertEquals(5, rJob.getLoadParallelism());
            Assert.assertEquals(EtlJobType.BROKER, rJob.getJobType());

            // 3. delete files
            dis.close();
            file.delete();
        }
    }
}
