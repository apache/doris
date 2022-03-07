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

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class PartitionLoadInfoTest {

    public Source makeSource(int startId, int size) {
        List<String> files = new ArrayList<String>(size);
        List<String> columns = new ArrayList<String>(size);
        for (int count = startId; count < startId + size; ++count) {
            String filename = "hdfs://host:port/dir/load-" + count;
            String column = "column-" + count;
            files.add(filename);
            columns.add(column);
        }
        Source source = new Source(files, columns, "\t", "\n", false);
        return source;
    }
 
    @Test
    public void testSerialization() throws Exception {
        File file = new File("./partitionLoadInfoTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        Source source1 = makeSource(0, 10);
        Source source2 = makeSource(100, 30);
        List<Source> sources = new ArrayList<Source>();
        sources.add(source1);
        sources.add(source2);
        PartitionLoadInfo partitionLoadInfo = new PartitionLoadInfo(sources);
        partitionLoadInfo.setVersion(100000);
        partitionLoadInfo.write(dos);
        
        PartitionLoadInfo partitionLoadInfo0 = new PartitionLoadInfo();
        partitionLoadInfo0.write(dos);
        
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        PartitionLoadInfo partitionLoadInfo1 = new PartitionLoadInfo();
        partitionLoadInfo1.readFields(dis);
        
        PartitionLoadInfo rPartitionLoadInfo0 = new PartitionLoadInfo();
        rPartitionLoadInfo0.readFields(dis);
        
        List<Source> sources1 = partitionLoadInfo1.getSources();

        Assert.assertEquals(partitionLoadInfo1.getVersion(), 100000);
        Assert.assertEquals(sources1.size(), 2);
        Assert.assertEquals(sources1.get(0).getFileUrls().size(), 10);       
        Assert.assertEquals(sources1.get(0).getColumnNames().size(), 10);
        Assert.assertEquals(sources1.get(1).getFileUrls().size(), 30);       
        Assert.assertEquals(sources1.get(1).getColumnNames().size(), 30);
        
        Assert.assertTrue(partitionLoadInfo1.equals(partitionLoadInfo));
        Assert.assertTrue(rPartitionLoadInfo0.equals(partitionLoadInfo0));
        Assert.assertFalse(partitionLoadInfo0.equals(partitionLoadInfo1));
       
        dis.close();
        file.delete();
    }

}
