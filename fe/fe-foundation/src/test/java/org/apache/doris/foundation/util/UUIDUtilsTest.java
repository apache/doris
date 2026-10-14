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

package org.apache.doris.foundation.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class UUIDUtilsTest {
    @Test
    void generateRfcUuidVersions() {
        UUID uuidV4 = UUIDUtils.fastUUID();
        Assertions.assertEquals(4, uuidV4.version());
        Assertions.assertEquals(2, uuidV4.variant());

        UUID previous = UUIDUtils.uuidV7();
        Assertions.assertEquals(7, previous.version());
        Assertions.assertEquals(2, previous.variant());
        for (int i = 0; i < 100; i++) {
            UUID current = UUIDUtils.uuidV7();
            Assertions.assertEquals(7, current.version());
            Assertions.assertEquals(2, current.variant());
            Assertions.assertTrue(Long.compareUnsigned(
                    previous.getMostSignificantBits(), current.getMostSignificantBits()) < 0);
            previous = current;
        }
    }
}
