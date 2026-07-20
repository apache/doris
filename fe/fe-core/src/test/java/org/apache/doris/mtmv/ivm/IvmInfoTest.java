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

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IvmInfoTest {
    @Test
    void testRefreshVersionAdvancesAfterCommittedRefresh() {
        IvmInfo info = new IvmInfo();

        Assertions.assertEquals(0, info.getRefreshVersion());
        info.advanceRefreshVersion();
        Assertions.assertEquals(1, info.getRefreshVersion());
    }

    @Test
    void testRefreshVersionPersistsThroughGson() {
        IvmInfo info = new IvmInfo();
        info.advanceRefreshVersion();

        IvmInfo recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(info), IvmInfo.class);
        Assertions.assertEquals(1, recovered.getRefreshVersion());
    }

    @Test
    void testCopyConstructor() {
        IvmInfo info = new IvmInfo();
        info.setEnableIvm(true);
        info.setBinlogBroken(true);
        info.setPlanSignature("abc123");
        info.advanceRefreshVersion();
        info.increaseBinlogBrokenGeneration();

        IvmInfo copy = new IvmInfo(info);

        Assertions.assertTrue(copy.isEnableIvm());
        Assertions.assertTrue(copy.isBinlogBroken());
        Assertions.assertEquals("abc123", copy.getPlanSignature());
        Assertions.assertEquals(1, copy.getRefreshVersion());
        Assertions.assertEquals(1, copy.getBinlogBrokenGeneration());
    }
}
