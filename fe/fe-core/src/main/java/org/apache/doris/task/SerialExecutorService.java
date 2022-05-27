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

import org.apache.doris.common.ThreadPoolManager;

import java.util.concurrent.ExecutorService;

/**
 * This executor service ensures that all tasks submitted to
 * the same slot are executed in the order of submission.
 */
public class SerialExecutorService {

    public interface SerialRunnable extends Runnable {
        int getIndex();
    }

    private final int numOfSlots;
    private final ExecutorService taskPool;
    private final SerialExecutor[] slots;

    private SerialExecutorService(int numOfSlots, ExecutorService taskPool) {
        this.numOfSlots = numOfSlots;
        this.slots = new SerialExecutor[numOfSlots];
        this.taskPool = taskPool;
        for (int i = 0; i < numOfSlots; i++) {
            slots[i] = new SerialExecutor(taskPool);
        }
    }

    public SerialExecutorService(int numOfSlots) {
        this(numOfSlots, ThreadPoolManager.newDaemonFixedThreadPool(
                numOfSlots, 256, "sync-task-pool", true));
    }

    public void submit(Runnable command) {
        int index = getIndex(command);
        if (isSlotIndex(index)) {
            SerialExecutor serialEx = slots[index];
            serialEx.execute(command);
        } else {
            taskPool.execute(command);
        }
    }

    private int getIndex(Runnable command) {
        int index = -1;
        if (command instanceof SerialRunnable) {
            index = (((SerialRunnable) command).getIndex());
        }
        return index;
    }

    private boolean isSlotIndex(int index) {
        return index >= 0 && index < numOfSlots;
    }

    public void close() {
        for (int i = 0; i < numOfSlots; i++) {
            final SerialExecutor serialEx = slots[i];
            serialEx.shutdown();
        }
    }
}
