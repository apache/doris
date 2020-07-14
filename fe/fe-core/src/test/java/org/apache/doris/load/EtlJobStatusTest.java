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

import org.apache.doris.thrift.TEtlState;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.TreeMap;

public class EtlJobStatusTest {

    @Test
    public void testSerialization() throws Exception {
        File file = new File("./EtlJobStatusTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        EtlStatus etlJobStatus = new EtlStatus();

        TEtlState state = TEtlState.FINISHED;
        String trackingUrl = "http://localhost:9999/xxx?job_id=zzz";
        Map<String, String> stats = new TreeMap<String, String>();
        Map<String, String> counters = new TreeMap<String, String>();
        for (int count = 0; count < 5; ++count) {
            String statsKey = "statsKey" + count;
            String statsValue = "statsValue" + count;
            String countersKey = "countersKey" + count;
            String countersValue = "countersValue" + count;
            stats.put(statsKey, statsValue);
            counters.put(countersKey, countersValue);
        }

        etlJobStatus.setState(state);
        etlJobStatus.setTrackingUrl(trackingUrl);
        etlJobStatus.setStats(stats);
        etlJobStatus.setCounters(counters);
        etlJobStatus.write(dos);
        // stats.clear();
        // counters.clear();

        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        EtlStatus etlJobStatus1 = new EtlStatus();
        etlJobStatus1.readFields(dis);
        stats = etlJobStatus1.getStats();
        counters = etlJobStatus1.getCounters();
                
        Assert.assertEquals(etlJobStatus1.getState().name(), "FINISHED");
        for (int count = 0; count < 5; ++count) {
            String statsKey = "statsKey" + count;
            String statsValue = "statsValue" + count;
            String countersKey = "countersKey" + count;
            String countersValue = "countersValue" + count;
            Assert.assertEquals(stats.get(statsKey), statsValue);
            Assert.assertEquals(counters.get(countersKey), countersValue);
        }
        
        Assert.assertTrue(etlJobStatus.equals(etlJobStatus1));
        Assert.assertEquals(trackingUrl, etlJobStatus1.getTrackingUrl());
 
        dis.close();
        file.delete();
    }

}
