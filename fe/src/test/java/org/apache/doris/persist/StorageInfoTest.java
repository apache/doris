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

import org.junit.Assert;
import org.junit.Test;

public class StorageInfoTest {
    @Test
    public void test() {
        StorageInfo info = new StorageInfo();
        Assert.assertEquals(-1, info.getClusterID());
        Assert.assertEquals(0, info.getImageSeq());
        Assert.assertEquals(0, info.getEditsSeq());
        
        info = new StorageInfo(10, 20, 30);
        Assert.assertEquals(10, info.getClusterID());
        Assert.assertEquals(20, info.getImageSeq());
        Assert.assertEquals(30, info.getEditsSeq());
        
        info.setClusterID(100);
        info.setImageSeq(200);
        info.setEditsSeq(300);
        
        Assert.assertEquals(100, info.getClusterID());
        Assert.assertEquals(200, info.getImageSeq());
        Assert.assertEquals(300, info.getEditsSeq());
    }
}
