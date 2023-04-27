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

public class LoadJobRowResultTest {

    @Test
    public void testResult() {
        LoadJobRowResult result = new LoadJobRowResult();
        Assert.assertEquals("Records: 0  Deleted: 0  Skipped: 0  Warnings: 0", result.toString());
        result.setRecords(199);
        Assert.assertEquals("Records: 199  Deleted: 0  Skipped: 0  Warnings: 0", result.toString());
        result.incRecords(1);
        result.setSkipped(20);
        Assert.assertEquals("Records: 200  Deleted: 0  Skipped: 20  Warnings: 0", result.toString());
        result.incSkipped(20);
        Assert.assertEquals("Records: 200  Deleted: 0  Skipped: 40  Warnings: 0", result.toString());
        Assert.assertEquals(200, result.getRecords());
        Assert.assertEquals(40, result.getSkipped());
        Assert.assertEquals(0, result.getWarnings());
    }
}
