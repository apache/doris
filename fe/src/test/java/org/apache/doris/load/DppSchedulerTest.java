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

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.CommandResult;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.thrift.TEtlState;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DppSchedulerTest {
    private DppScheduler dppScheduler;

    @Mocked
    private Util util;
    
    @Before
    public void setUp() {
        // mock palo home env
//        PowerMock.mockStatic(System.class);
//        EasyMock.expect(System.getenv("DORIS_HOME")).andReturn(".").anyTimes();
//        PowerMock.replay(System.class);

        UnitTestUtil.initDppConfig();
        dppScheduler = new DppScheduler(Load.dppDefaultConfig);
    }
    
    @Ignore
    @Test
    public void testCalcReduceNumByInputSize() throws Exception {
        // mock hadoop count
        String fileInfos = "           0            1           1000000000 /label2/export/label2.10007.10007.10005\n"
                         + "           0            1           1000000001 /label2/export/label2.10007.10007.10006";
        CommandResult commandResult = new CommandResult();
        commandResult.setReturnCode(0);
        commandResult.setStdout(fileInfos);
        new Expectations(util) {
            {
                Util.executeCommand(anyString, (String[]) any);
                times = 3;
                result = commandResult;
            }
        };
 
        // get method
        Method calcReduceNumByInputSize = UnitTestUtil.getPrivateMethod(
                DppScheduler.class, "calcReduceNumByInputSize", new Class[] {Set.class});
        
        // normal test
        Set<String> inputPaths = new HashSet<String>();
        Config.load_input_size_limit_gb = 0;
        Config.dpp_bytes_per_reduce = 2000000000;
        Assert.assertEquals(2, calcReduceNumByInputSize.invoke(dppScheduler, new Object[] {inputPaths}));
        Config.dpp_bytes_per_reduce = 2000000002;
        Assert.assertEquals(1, calcReduceNumByInputSize.invoke(dppScheduler, new Object[] {inputPaths}));

        // input file size exceeds limit
        Config.load_input_size_limit_gb = 1;
        try {
            calcReduceNumByInputSize.invoke(dppScheduler, new Object[]{inputPaths});
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        Config.load_input_size_limit_gb = 0;
    }
    
    @Test
    public void testCalcReduceNumByTablet() throws Exception {
        Map<String, Object> jobConf = new HashMap<String, Object>();
        Map<String, Map> tables = new HashMap<String, Map>();
        jobConf.put("tables", tables);
        Map<String, Map> table = new HashMap<String, Map>();
        tables.put("0", table);
        Map<String, Map> views = new HashMap<String, Map>();
        table.put("views", views);
        Map<String, Object> view1 = new HashMap<String, Object>();
        view1.put("hash_mod", 10);
        views.put("view1", view1);
        Map<String, Object> view2 = new HashMap<String, Object>();
        List<Object> rangeList = new ArrayList<Object>();
        for (int i = 0; i < 5; i++) {
            rangeList.add(i);
        }
        view2.put("key_ranges", rangeList);
        views.put("view2", view2);
        
        Method calcReduceNumByTablet = UnitTestUtil.getPrivateMethod(
                DppScheduler.class, "calcReduceNumByTablet", new Class[] {Map.class});
        Assert.assertEquals(15, calcReduceNumByTablet.invoke(dppScheduler, new Object[] {jobConf}));
    }
    
    @Test
    public void testGetEtlJobStatus() {
        String jobStatus = "Job: job_201501261830_12231\n" 
                         + "file: hdfs://host:54310/system/mapred/job_201501261830_12231/job.xml\n" 
                         + "tracking URL: http://host:8030/jobdetails.jsp?jobid=job_201501261830_12231\n"
                         + "job state: 1\n"
                         + "map() completion: 0.9036233\n"
                         + "reduce() completion: 0.0\n"
                         + "Counters: 14\n"
                         + "        File Systems\n"
                         + "                HDFS bytes read=398065481\n"
                         + "        DPP\n"
                         + "                dpp.abnorm.ALL=0\n"
                         + "                dpp.norm.ALL=0\n"
                         + "        Map-Reduce Framework\n"
                         + "                Map input records=4085933\n"
                         + "                Map output bytes=503053858";
        CommandResult commandResult = new CommandResult();
        commandResult.setReturnCode(0);
        commandResult.setStdout(jobStatus);
        new Expectations(util) {
            {
                Util.executeCommand(anyString, (String[]) any);
                times = 1;
                result = commandResult;
            }
        };
 
        EtlStatus status = dppScheduler.getEtlJobStatus("etlJobId");
        Assert.assertEquals(TEtlState.RUNNING, status.getState());
        Assert.assertEquals("0", status.getCounters().get("dpp.abnorm.ALL"));
        Assert.assertEquals("0.9036233", status.getStats().get("map() completion"));
    }
    
    @Test
    public void testGetEtlFileList() {
        String outputPath = "/label_0";
        CommandResult successLsResult = new CommandResult();
        successLsResult.setReturnCode(0);
        CommandResult failLsResult = new CommandResult();
        failLsResult.setReturnCode(-1);
        CommandResult successTestDirResult = new CommandResult();
        successTestDirResult.setReturnCode(0);
        CommandResult failTestDirResult = new CommandResult();
        failTestDirResult.setReturnCode(-1);

        String files = "-rw-r--r--   3 palo palo   29896160 2015-02-03 13:10 /label_0/export/label_0.32241.32241.0\n"
                     + "-rw-r--r--   3 palo palo   29896161 2015-02-03 13:10 /label_0/export/label_0.32241.32241.1";
        successLsResult.setStdout(files);
        new Expectations(util) {
            {
                Util.executeCommand(anyString, (String[]) any);
                times = 6;
                returns(
                        // success
                        successLsResult,
                        // ls fail
                        failLsResult,
                        // outputPath not exist
                        failTestDirResult,
                        // ls fail
                        failLsResult,
                        // success
                        successTestDirResult,
                        // fileDir not exist
                        failTestDirResult
                );
            }
        };
        Map<String, Long> fileMap = dppScheduler.getEtlFiles(outputPath);
        Assert.assertEquals(2, fileMap.size());
        Assert.assertNull(dppScheduler.getEtlFiles(outputPath));

        fileMap = dppScheduler.getEtlFiles(outputPath);
        Assert.assertNotNull(fileMap);
        Assert.assertTrue(fileMap.isEmpty());
    }
    
    @Test
    public void testKillEtlJob() {
        CommandResult commandResult = new CommandResult();
        new Expectations(util) {
            {
                Util.executeCommand(anyString, (String[]) any);
                times = 1;
                result = commandResult;
            }
        };
 
        dppScheduler.killEtlJob("etlJobId");
    }
    
    @Test
    public void testGetEtlOutputPath() {
        DppConfig dppConfig = Load.dppDefaultConfig.getCopiedDppConfig();
        long dbId = 0;
        String loadLabel = "test_label";
        String etlOutputDir = "10000";

        String actualPath = DppScheduler.getEtlOutputPath(dppConfig.getFsDefaultName(), dppConfig.getOutputPath(),
                dbId, loadLabel, etlOutputDir);
        String expectedPath = dppConfig.getFsDefaultName() + dppConfig.getOutputPath() + "/" + String.valueOf(dbId)
                + "/" + loadLabel + "/" + etlOutputDir;
        Assert.assertEquals(expectedPath, actualPath);
    }
    
}
