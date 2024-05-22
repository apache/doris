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

package org.apache.doris.common.util;

import org.apache.doris.thrift.TUniqueId;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class UtilTest {
    @Test
    public void testParseTUniqueIdFromString() {
        // test normal
        for (int i = 0; i < 10; i++) {
            UUID uuid = UUID.randomUUID();
            TUniqueId tUID = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            String strUID = DebugUtil.printId(tUID);
            TUniqueId parsedTUniqueId = Util.parseTUniqueIdFromString(strUID);
            Assert.assertEquals(tUID, parsedTUniqueId);
        }

        // test null
        Assert.assertNull(Util.parseTUniqueIdFromString(null));
        Assert.assertNull(Util.parseTUniqueIdFromString(""));
        Assert.assertEquals(new TUniqueId(), Util.parseTUniqueIdFromString("0-0"));
        Assert.assertNull(Util.parseTUniqueIdFromString("INVALID-STRING"));
        Assert.assertNull(Util.parseTUniqueIdFromString("INVALID"));
    }
}
