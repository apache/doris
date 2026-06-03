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

import org.apache.doris.mtmv.BaseTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class IvmInfoTest {

    @Test
    public void testCopyConstructorDeepCopiesStreams() {
        IvmInfo info = new IvmInfo();
        info.setEnableIvm(true);
        info.setBinlogBroken(true);
        info.setRunningIvmRefresh(true);
        BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
        IvmStreamRef streamRef = new IvmStreamRef(42L);
        streamRef.setLatestTso(100L);
        Map<BaseTableInfo, IvmStreamRef> streams = new HashMap<>();
        streams.put(baseTableInfo, streamRef);
        info.setBaseTableStreams(streams);

        IvmInfo copy = new IvmInfo(info);

        Assertions.assertTrue(copy.isEnableIvm());
        Assertions.assertTrue(copy.isBinlogBroken());
        Assertions.assertTrue(copy.isRunningIvmRefresh());
        Assertions.assertEquals(1, copy.getBaseTableStreams().size());
        IvmStreamRef copiedRef = copy.getBaseTableStreams().get(baseTableInfo);
        Assertions.assertNotSame(streamRef, copiedRef);
        Assertions.assertEquals(42L, copiedRef.getConsumedTso());
        Assertions.assertEquals(100L, copiedRef.getLatestTso());

        copiedRef.setConsumedTso(200L);
        Assertions.assertEquals(42L, streamRef.getConsumedTso());
    }

    @Test
    public void testCopyConstructorHandlesNullStreams() {
        IvmInfo info = new IvmInfo();
        info.setBaseTableStreams(null);

        IvmInfo copy = new IvmInfo(info);

        Assertions.assertNotNull(copy.getBaseTableStreams());
        Assertions.assertTrue(copy.getBaseTableStreams().isEmpty());
    }
}
