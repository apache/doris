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

package org.apache.doris.mtmv.ivm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IvmInfoTest {

    @Test
    public void testCopyConstructor() {
        IvmInfo info = new IvmInfo();
        info.setEnableIvm(true);
        info.setBinlogBroken(true);
        info.setPlanSignature("abc123");

        IvmInfo copy = new IvmInfo(info);

        Assertions.assertTrue(copy.isEnableIvm());
        Assertions.assertTrue(copy.isBinlogBroken());
        Assertions.assertEquals("abc123", copy.getPlanSignature());
    }

    @Test
    public void testCopyConstructorDeepCopiesIndependent() {
        IvmInfo info = new IvmInfo();
        info.setPlanSignature("original");

        IvmInfo copy = new IvmInfo(info);
        copy.setPlanSignature("modified");

        // Modifications to copy should not affect original
        Assertions.assertEquals("original", info.getPlanSignature());
        Assertions.assertEquals("modified", copy.getPlanSignature());
    }

    @Test
    public void testDefaultValues() {
        IvmInfo info = new IvmInfo();
        Assertions.assertFalse(info.isEnableIvm());
        Assertions.assertFalse(info.isBinlogBroken());
        Assertions.assertNull(info.getPlanSignature());
    }
}
