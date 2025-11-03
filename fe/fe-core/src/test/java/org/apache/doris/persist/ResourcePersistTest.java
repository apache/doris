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

import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.S3Resource;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class ResourcePersistTest {
    @Test
    public void test() throws IOException {
        Resource resource = new S3Resource("s3_resource");
        File file = new File("./ResourcePersistTest");
        try {
            // 1. Write objects to file
            file.createNewFile();
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
            resource.write(dos);
            dos.flush();
            dos.close();

            // 2. Read objects from file
            DataInputStream dis = new DataInputStream(new FileInputStream(file));
            S3Resource resource1 = (S3Resource) Resource.read(dis);
            dis.close();
            Assert.assertEquals(resource1.toString(), resource.toString());
            resource1.readLock();
            resource1.readUnlock();
        } finally {
            file.delete();
        }
    }
}
