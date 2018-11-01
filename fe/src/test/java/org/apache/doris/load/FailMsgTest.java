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

public class FailMsgTest {

    @Test
    public void testSerialization() throws Exception {
        File file = new File("./failMsgTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        FailMsg failMsg = new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED, "Job failed");
        failMsg.write(dos);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        FailMsg failMsg1 = new FailMsg();
        failMsg1.readFields(dis);
           
        Assert.assertEquals(failMsg1.getMsg(), "Job failed");
        Assert.assertEquals(failMsg1.getCancelType(), FailMsg.CancelType.ETL_QUALITY_UNSATISFIED);
        
        Assert.assertTrue(failMsg1.equals(failMsg));
 
        dis.close();
        file.delete();
    }

}
