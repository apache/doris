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

package org.apache.doris.task;

import org.apache.doris.common.Config;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

public class SyncTaskPool {
    private static final int NUM_OF_SLOTS = Config.max_sync_task_threads_num;
    private static final SerialExecutorService EXECUTOR = new SerialExecutorService(NUM_OF_SLOTS);
    private static final AtomicInteger nextIndex = new AtomicInteger();

    public static void submit(Runnable task) {
        if (task == null) {
            return;
        }
        EXECUTOR.submit(task);
    }

    /**
     * Gets the next index loop from 0 to @NUM_OF_SLOTS - 1
     */
    public static int getNextIndex() {
        return nextIndex.updateAndGet(new IntUnaryOperator() {
            @Override
            public int applyAsInt(int operand) {
                if (++operand >= NUM_OF_SLOTS) {
                    operand = 0;
                }
                return operand;
            }
        });
    }
}
