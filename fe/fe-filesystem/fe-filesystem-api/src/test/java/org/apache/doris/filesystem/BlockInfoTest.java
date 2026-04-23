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

package org.apache.doris.filesystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BlockInfoTest {

    @Test
    void gettersReturnCorrectValues() {
        BlockInfo block = new BlockInfo(128L, 256L, new String[]{"host1", "host2"});
        Assertions.assertEquals(128L, block.offset());
        Assertions.assertEquals(256L, block.length());
        Assertions.assertArrayEquals(new String[]{"host1", "host2"}, block.hosts());
    }

    @Test
    void hostsDefensiveCopyOnConstruction() {
        String[] original = {"host1", "host2"};
        BlockInfo block = new BlockInfo(0, 100, original);

        // Mutating the original array should NOT affect the block
        original[0] = "mutated";
        Assertions.assertEquals("host1", block.hosts()[0]);
    }

    @Test
    void hostsDefensiveCopyOnAccess() {
        BlockInfo block = new BlockInfo(0, 100, new String[]{"host1", "host2"});
        String[] first = block.hosts();
        String[] second = block.hosts();

        // Each call should return a fresh copy
        Assertions.assertNotSame(first, second);
        Assertions.assertArrayEquals(first, second);

        // Mutating the returned array should NOT affect future calls
        first[0] = "mutated";
        Assertions.assertEquals("host1", block.hosts()[0]);
    }

    @Test
    void nullHostsTreatedAsEmpty() {
        BlockInfo block = new BlockInfo(0, 100, null);
        Assertions.assertEquals(0, block.hosts().length);
    }
}
