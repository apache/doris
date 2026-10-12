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
    void testSequencePrefixAdvancesAfterCommittedRefresh() {
        IvmInfo info = new IvmInfo();

        Assertions.assertEquals(0, info.getSequencePrefix());
        info.advanceSequencePrefix();
        Assertions.assertEquals(1, info.getSequencePrefix());
    }

    @Test
    void testSequencePrefixPersistsThroughGson() {
        IvmInfo info = new IvmInfo();
        info.advanceSequencePrefix();

        IvmInfo recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(info), IvmInfo.class);
        Assertions.assertEquals(1, recovered.getSequencePrefix());
    }

    @Test
    void testSequencePrefixIsPersistedAsSp() {
        IvmInfo info = new IvmInfo();
        info.advanceSequencePrefix();

        String json = GsonUtils.GSON.toJson(info);
        Assertions.assertTrue(json.contains("\"sp\":1"), json);
        Assertions.assertFalse(json.contains("\"rv\""), json);
    }

    @Test
    void testUseFullKeysPersistsThroughGson() {
        IvmInfo info = new IvmInfo();
        info.setUseFullKeys(true);

        IvmInfo recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(info), IvmInfo.class);
        Assertions.assertTrue(recovered.isUseFullKeys());
    }

    @Test
    void testCopyConstructor() {
        IvmInfo info = new IvmInfo();
        info.setEnableIvm(true);
        info.setUseFullKeys(true);
        info.setPlanSignature("abc123");
        info.advanceSequencePrefix();

        IvmInfo copy = new IvmInfo(info);

        Assertions.assertTrue(copy.isEnableIvm());
        Assertions.assertTrue(copy.isUseFullKeys());
        Assertions.assertEquals("abc123", copy.getPlanSignature());
        Assertions.assertEquals(1, copy.getSequencePrefix());
    }

}
