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

package org.apache.doris.qe;

import org.apache.doris.common.Config;
import org.apache.doris.load.routineload.RoutineLoadJob;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NumericOptionBoundaryTest {
    @Test
    void operatorMemoryUsesNonnegativeWireValues() throws Exception {
        SessionVariable session = new SessionVariable();
        // 2 GiB is the first KiB value whose byte count overflows a signed 32-bit int.
        session.checkMinimumOperatorMemoryRequiredKB("2097152");
        session.minimumOperatorMemoryRequiredKB = 2097152;
        Assertions.assertEquals(2097152, session.toThrift().getMinimumOperatorMemoryRequiredKb());
        Assertions.assertThrows(Exception.class, () -> session.checkMinimumOperatorMemoryRequiredKB("-1"));
        session.checkMinimumOperatorMemoryRequiredKB("0");
    }

    @Test
    void routineLoadTimeoutFitsExecutionPlan() {
        int saved = Config.routine_load_task_timeout_multiplier;
        try {
            Config.routine_load_task_timeout_multiplier = 10;
            // The first interval below overflows an int after multiplication by 10.
            Assertions.assertEquals(Integer.MAX_VALUE, RoutineLoadJob.taskTimeoutSeconds(214748365, 60));
            Assertions.assertEquals(Integer.MAX_VALUE, RoutineLoadJob.taskTimeoutSeconds(Long.MAX_VALUE, 60));
            Assertions.assertEquals(60, RoutineLoadJob.taskTimeoutSeconds(1, 60));
            Assertions.assertEquals(2147480, RoutineLoadJob.taskTimeoutSeconds(214748, 60));
        } finally {
            Config.routine_load_task_timeout_multiplier = saved;
        }
    }
}
