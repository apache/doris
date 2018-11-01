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

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class ColumnStatsTest {
    
    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./columnStats");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        
        ColumnStats stats1 = new ColumnStats();
        stats1.write(dos);
        
        ColumnStats stats2 = new ColumnStats();
        stats2.setAvgSerializedSize(1.1f);
        stats2.setNumDistinctValues(100L);
        stats2.setMaxSize(1000L);
        stats2.setNumNulls(10000L);
        stats2.write(dos);
        
        ColumnStats stats3 = new ColumnStats();
        stats3.setAvgSerializedSize(3.3f);
        stats3.setNumDistinctValues(200L);
        stats3.setMaxSize(2000L);
        stats3.setNumNulls(20000L);
        stats3.write(dos);
        
        ColumnStats stats4 = new ColumnStats(stats3);
        stats4.write(dos);

        dos.flush();
        dos.close();
        
        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        ColumnStats rStats1 = new ColumnStats();
        rStats1.readFields(dis);
        Assert.assertTrue(rStats1.equals(stats1));
        
        ColumnStats rStats2 = new ColumnStats();
        rStats2.readFields(dis);
        Assert.assertTrue(rStats2.equals(stats2));
        
        ColumnStats rStats3 = ColumnStats.read(dis);
        Assert.assertTrue(rStats3.equals(stats3));
        
        ColumnStats rStats4 = ColumnStats.read(dis);
        Assert.assertTrue(rStats4.equals(stats4));
        Assert.assertTrue(rStats4.equals(stats3));
        
        Assert.assertTrue(rStats3.equals(rStats3));
        Assert.assertFalse(rStats3.equals(this));
        Assert.assertFalse(rStats2.equals(rStats3));

        // 3. delete files
        dis.close();
        file.delete();
    }
    
}
