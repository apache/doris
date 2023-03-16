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
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

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
        jobProperties.put(LoadStmt.LOAD_PARALLELISM, "5");
        brokerLoadJob.setJobProperties(jobProperties);
        return brokerLoadJob;
    }

    @Test
    public void testBrokerLoadJob(@Mocked Env env, @Mocked InternalCatalog catalog, @Injectable Database database,
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

        BrokerLoadJob rJob = (BrokerLoadJob)  BrokerLoadJob.read(dis);
        Assert.assertEquals(5, rJob.getLoadParallelism());
        Assert.assertEquals(EtlJobType.BROKER, rJob.getJobType());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
