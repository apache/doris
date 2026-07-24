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

package org.apache.doris.nereids.load;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NereidsBrokerLoadTaskTest {

    /**
     * Broker load does not participate in fill_missing_columns (that is JSON-only in routine load).
     * NereidsBrokerLoadTask therefore relies on the NereidsLoadTaskInfo default, which must be false.
     * Pinning this here guards against an accidental default flip on the interface.
     */
    @Test
    public void testIsFillMissingColumnsDefaultFalse() {
        NereidsBrokerLoadTask task = new NereidsBrokerLoadTask(
                1L, 0, 1, false, false, false, null);
        Assertions.assertFalse(task.isFillMissingColumns());
    }
}
