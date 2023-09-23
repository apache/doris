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

public class SourceTest {

    @Test
    public void testSerialization() throws Exception {
        File file = new File("./sourceTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        Source source0 = new Source();
        source0.write(dos);

        List<String> files = new ArrayList<String>(100);
        List<String> columns = new ArrayList<String>(100);
        for (int count = 0; count < 100; ++count) {
            String filename = "hdfs://host:port/dir/load-" + count;
            String column = "column-" + count;
            files.add(filename);
            columns.add(column);
        }
        Source source1 = new Source(files, columns, "\t", "\n", false);
        source1.write(dos);

        Source source2 = new Source();
        source2.setFileUrls(null);
        source2.setColumnNames(null);
        source2.write(dos);

        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Source rSource0 = new Source();
        rSource0.readFields(dis);

        Source rSource1 = new Source();
        rSource1.readFields(dis);

        Source rSource2 = new Source();
        rSource2.readFields(dis);

        Assert.assertEquals(rSource0, source0);
        Assert.assertEquals(source0, source0);
        Assert.assertNotEquals(rSource0, this);
        Assert.assertEquals(rSource1, source1);
        Assert.assertNotEquals(rSource2, source2);
        Assert.assertNotEquals(rSource0, source1);

        rSource2.setFileUrls(null);
        Assert.assertNotEquals(rSource2, source2);
        rSource2.setColumnNames(null);
        rSource2.setFileUrls(new ArrayList<String>());
        rSource2.setColumnNames(null);
        rSource2.setFileUrls(null);
        Assert.assertEquals(rSource2, source2);

        dis.close();
        file.delete();
    }

}
