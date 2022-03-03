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

package org.apache.doris.load;

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LoadException;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;

import mockit.Expectations;
import mockit.Mocked;

public class DppConfigTest {
    private FakeCatalog fakeCatalog;

    @Test
    public void testNormal(@Mocked Catalog catalog) throws LoadException {
        // mock catalog
        int clusterId = 10;
        fakeCatalog = new FakeCatalog();
        FakeCatalog.setCatalog(catalog);
        new Expectations() {
            {
                catalog.getClusterId();
                minTimes = 0;
                result = clusterId;
            }
        };

        Map<String, String> configMap = Maps.newHashMap();
        configMap.put("hadoop_palo_path", "/user/palo2");
        configMap.put("hadoop_http_port", "1234");
        configMap.put("hadoop_configs", "mapred.job.tracker=127.0.0.1:111;fs.default.name=hdfs://127.0.0.1:112;"
                        + "hadoop.job.ugi=user,password;mapred.job.priority=NORMAL");
        DppConfig dppConfig = DppConfig.create(configMap);

        Assert.assertEquals(String.format("/user/palo2/%d/applications/%s", clusterId, FeConstants.dpp_version),
                dppConfig.getApplicationsPath());
        Assert.assertEquals(String.format("/user/palo2/%d/output", clusterId), dppConfig.getOutputPath());
        Assert.assertEquals("hdfs://127.0.0.1:112", dppConfig.getFsDefaultName());
        Assert.assertEquals("user,password", dppConfig.getHadoopJobUgiStr());
        Assert.assertEquals("127.0.0.1", dppConfig.getNameNodeHost());
        Assert.assertEquals(1234, dppConfig.getHttpPort());

        Map<String, String> hadoopConfigs = dppConfig.getHadoopConfigs();
        Assert.assertEquals(4, hadoopConfigs.size());
        Assert.assertEquals("NORMAL", hadoopConfigs.get("mapred.job.priority"));

        // update
        Map<String, String> newConfigMap = Maps.newHashMap();
        newConfigMap.put("hadoop_configs", "mapred.job.priority=VERY_HIGH");
        DppConfig newDppConfig = DppConfig.create(newConfigMap);
        dppConfig.update(newDppConfig);
        Assert.assertEquals(4, hadoopConfigs.size());
        Assert.assertEquals("VERY_HIGH", hadoopConfigs.get("mapred.job.priority"));

        newConfigMap.clear();
        newConfigMap.put(LoadStmt.BOS_ENDPOINT, "http://127.0.0.2:1234");
        newConfigMap.put(LoadStmt.BOS_ACCESSKEY, "123");
        newConfigMap.put(LoadStmt.BOS_SECRET_ACCESSKEY, "456");
        dppConfig.updateHadoopConfigs(newConfigMap);
        Assert.assertEquals(7, hadoopConfigs.size());
        Assert.assertEquals("http://127.0.0.2:1234", hadoopConfigs.get("fs.bos.endpoint"));
        Assert.assertEquals("123", hadoopConfigs.get("fs.bos.access.key"));
        Assert.assertEquals("456", hadoopConfigs.get("fs.bos.secret.access.key"));

        // clear
        dppConfig.clear();
        Assert.assertEquals(3, hadoopConfigs.size());
        Assert.assertFalse(hadoopConfigs.containsKey("mapred.job.priority"));
        Assert.assertTrue(hadoopConfigs.containsKey("fs.default.name"));
    }

    @Test(expected = LoadException.class)
    public void testHadoopConfigsNotEnough() throws LoadException {
        Map<String, String> configMap = Maps.newHashMap();
        configMap.put("hadoop_palo_path", "/user/palo2");
        configMap.put("hadoop_http_port", "1234");
        configMap.put("hadoop_configs", "mapred.job.tracker=127.0.0.1:111;fs.default.name=hdfs://127.0.0.1:112;");
        DppConfig dppConfig = DppConfig.create(configMap);

        dppConfig.check();
    }

    @Test(expected = LoadException.class)
    public void testBosParamsNotEnough() throws LoadException {
        Map<String, String> configMap = Maps.newHashMap();
        configMap.put(LoadStmt.BOS_ENDPOINT, "http://127.0.0.2:1234");
        configMap.put(LoadStmt.BOS_ACCESSKEY, "123");

        DppConfig dppConfig = new DppConfig();
        dppConfig.updateHadoopConfigs(configMap);
    }

    @Test
    public void testSerialization() throws Exception {
        // mock catalog
        fakeCatalog = new FakeCatalog();
        FakeCatalog.setMetaVersion(FeConstants.meta_version);

        Map<String, String> configMap = Maps.newHashMap();
        configMap.put("hadoop_palo_path", "/user/palo2");
        configMap.put("hadoop_http_port", "1234");
        configMap.put("hadoop_configs", "mapred.job.tracker=127.0.0.1:111;fs.default.name=hdfs://127.0.0.1:112;"
                        + "hadoop.job.ugi=user,password;mapred.job.priority=NORMAL");
        DppConfig dppConfig = DppConfig.create(configMap);

        File file = new File("./dppConfigTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        dppConfig.write(dos);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        DppConfig newDppConfig = new DppConfig();
        newDppConfig.readFields(dis);
        dis.close();
        file.delete();

        Assert.assertEquals("/user/palo2", newDppConfig.getPaloPath());
        Assert.assertEquals(1234, newDppConfig.getHttpPort());
        Assert.assertEquals(4, newDppConfig.getHadoopConfigs().size());
    }
}
